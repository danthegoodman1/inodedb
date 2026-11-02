//! Scripted scenarios for a writer that reads the tree into its cache when
//! it starts.

use inodedb_core::cache::Budget;
use inodedb_core::preload::{Cursor, PAGE};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::Error;
use inodedb_core::writer::{Action, Config, Writer};
use inodedb_sim::harness::*;

fn preloading() -> Config {
    Config {
        preload: true,
        ..Config::default()
    }
}

fn found(result: &QueryResult) -> bool {
    match result {
        QueryResult::Stat { .. } => true,
        QueryResult::Failed(Error::NotFound) => false,
        other => panic!("expected a stat, got {other:?}"),
    }
}

/// Stats `path`, returning whether it exists and how many storage reads
/// the stat took.
fn stat(h: &mut Harness, path: &str) -> (bool, usize) {
    let reads = h.reads;
    let result = h.stat(path);
    (found(&result), h.reads - reads)
}

#[test]
fn a_preloaded_writer_answers_stats_without_reads() {
    let mut h = Harness::new();
    h.ops([
        mkdir_p("a/b"),
        put("a/b/f", "1"),
        put("a/g", "2"),
        mkdir("c"),
    ]);
    let mut h = Harness::with(h.storage.clone(), preloading());
    for path in ["a", "a/b", "a/b/f", "a/g", "c"] {
        assert_eq!(stat(&mut h, path), (true, 0), "{path}");
    }
    for path in ["a/b/missing", "c/missing", "missing", "a/missing/deeper"] {
        assert_eq!(stat(&mut h, path), (false, 0), "{path}");
    }
    // A stat by inode checks its key's reach through back-pointers.
    let QueryResult::Stat { ino, .. } = h.stat("a/b/f") else {
        panic!("a/b/f exists");
    };
    let reads = h.reads;
    let (_, result) = h.query(Query::Stat(Target::Ino(ino)));
    assert!(found(&result));
    assert_eq!(h.reads, reads, "back-pointers came with the preload");
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_writer_without_preload_reads_as_it_misses() {
    let mut h = Harness::new();
    h.ops([mkdir("a"), put("a/f", "1")]);
    let mut h = Harness::with(h.storage.clone(), Config::default());
    assert!(stat(&mut h, "a/f").1 > 0);
}

#[test]
fn a_directory_larger_than_a_page_arrives_as_names() {
    let mut h = Harness::new();
    h.ops([mkdir("big")]);
    let names: Vec<String> = (0..PAGE + 10)
        .map(|index| format!("big/f{index:05}"))
        .collect();
    for chunk in names.chunks(500) {
        h.ops(chunk.iter().map(|name| put(name, "")));
    }
    h.ops([mkdir("small"), put("small/f", "")]);
    let mut h = Harness::with(h.storage.clone(), preloading());
    assert_eq!(stat(&mut h, "big/f00000"), (true, 0));
    assert_eq!(stat(&mut h, &format!("big/f{:05}", PAGE + 9)), (true, 0));
    let (found, reads) = stat(&mut h, "big/missing");
    assert!(
        !found && reads > 0,
        "a name missing from a partial directory needs a read"
    );
    assert_eq!(stat(&mut h, "small/missing"), (false, 0));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_preload_stops_at_the_cache_budget() {
    let mut h = Harness::new();
    h.ops([mkdir("d")]);
    let names: Vec<String> = (0..PAGE + 10)
        .map(|index| format!("d/f{index:05}"))
        .collect();
    for chunk in names.chunks(500) {
        h.ops(chunk.iter().map(|name| put(name, "")));
    }
    let budget = Budget {
        inodes: 10,
        ..Budget::default()
    };
    let config = Config {
        cache: budget,
        ..Config::default()
    };
    let plain = Harness::with(h.storage.clone(), config.clone());
    let preloaded = Harness::with(
        h.storage.clone(),
        Config {
            preload: true,
            ..config
        },
    );
    assert_eq!(
        preloaded.reads - plain.reads,
        1,
        "the first page of inodes fills the budget, and the preload stops"
    );
    assert!(preloaded.writer.cache().inodes() <= budget.inodes);
}

#[test]
fn a_preload_takes_the_commits_made_while_it_reads() {
    let mut h = Harness::new();
    h.ops([mkdir("d"), put("d/old", "1"), mkdir("e")]);
    let storage = h.storage.clone();
    let mut writer = Writer::new(preloading(), Harness::writer_id(), 1_000, [7; 32]);
    // The first page of entries reads the store as it stands, and returns
    // only after more commits.
    let first = Cursor::Entries(inodedb_core::keys::entry_range().start).read();
    let mut held = None;
    loop {
        let actions = writer.drain();
        if actions.is_empty() {
            break;
        }
        for action in actions {
            match action {
                Action::Read { id, reads } if reads.get(1) == Some(&first) => {
                    held = Some((id, storage.read_now(&reads)));
                }
                Action::Read { id, reads } => writer.read_done(id, storage.read_now(&reads)),
                Action::Commit { id, expect, writes } => {
                    let outcome = storage.commit_now(expect.as_deref(), &writes);
                    writer.commit_done(id, outcome);
                }
                Action::Resolve { id } => writer.resolve_done(id, storage.head_now()),
                Action::Answer { .. } => {}
            }
        }
    }
    let mut h = Harness::with_writer(storage, writer);
    h.ops([put("d/new", "2"), unlink("d/old"), put("e/x", "3")]);
    let (id, results) = held.expect("the first page went out");
    h.writer.read_done(id, results);
    h.run();
    assert!(!h.writer.is_stopped());
    assert_eq!(stat(&mut h, "d/new"), (true, 0));
    assert_eq!(stat(&mut h, "d/old"), (false, 0));
    assert_eq!(stat(&mut h, "e/x"), (true, 0));
    assert_eq!(stat(&mut h, "e/y"), (false, 0));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}
