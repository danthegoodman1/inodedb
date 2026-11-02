//! Scripted scenarios for a writer that reads the tree into its cache when
//! it starts.

use inodedb_core::cache::{Budget, DIR_BYTES, INODE_BYTES, NAME_BYTES};
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
    // An empty directory whose entries would lie pages past the root's.
    h.ops([mkdir("small"), put("small/f", ""), mkdir("empty")]);
    let mut h = Harness::with(h.storage.clone(), preloading());
    assert_eq!(stat(&mut h, "big/f00000"), (true, 0));
    assert_eq!(stat(&mut h, &format!("big/f{:05}", PAGE + 9)), (true, 0));
    let (found, reads) = stat(&mut h, "big/missing");
    assert!(
        !found && reads > 0,
        "a name missing from a partial directory needs a read"
    );
    assert_eq!(stat(&mut h, "small/missing"), (false, 0));
    assert_eq!(stat(&mut h, "empty/missing"), (false, 0));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn an_empty_store_preloads_as_an_empty_root() {
    let h = Harness::new();
    let mut h = Harness::with(h.storage.clone(), preloading());
    assert_eq!(stat(&mut h, "missing"), (false, 0));
}

#[test]
fn a_preload_stops_at_the_cache_budget_with_the_entries_first() {
    let mut h = Harness::new();
    h.ops([mkdir("d")]);
    let names: Vec<String> = (0..50).map(|index| format!("d/f{index:05}")).collect();
    h.ops(names.iter().map(|name| put(name, "")));
    // Room for both listings and about ten of the 52 inode records.
    let budget = Budget {
        tree_bytes: 2 * DIR_BYTES + (NAME_BYTES + 1) + 50 * (NAME_BYTES + 6) + 10 * INODE_BYTES,
        ..Budget::default()
    };
    let config = Config {
        cache: budget,
        ..Config::default()
    };
    let plain = Harness::with(h.storage.clone(), config.clone());
    let mut preloaded = Harness::with(
        h.storage.clone(),
        Config {
            preload: true,
            ..config
        },
    );
    assert_eq!(
        preloaded.reads - plain.reads,
        2,
        "a page of entries, then a page of inodes that fills the budget"
    );
    let cache = preloaded.writer.cache();
    assert!(cache.tree_bytes() <= budget.tree_bytes);
    assert!((1..52).contains(&cache.inodes()), "{}", cache.inodes());
    assert_eq!(stat(&mut preloaded, "d/missing"), (false, 0));
    assert_eq!(
        stat(&mut preloaded, "d/f00049"),
        (true, 1),
        "the path resolves from the cache, and only the record needs a read"
    );
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

#[test]
fn a_preloaded_writer_answers_reads_of_values_without_reads() {
    let mut h = Harness::new();
    h.ops([mkdir("a"), put("a/f", "one"), put("a/g", "two")]);
    let mut h = Harness::with(h.storage.clone(), preloading());
    let reads = h.reads;
    let (_, result) = h.query(Query::Read {
        target: Target::Path(p("a/g")),
        version: None,
    });
    match result {
        QueryResult::Read { value, .. } => assert_eq!(&value[..], b"two"),
        other => panic!("expected a value, got {other:?}"),
    }
    assert_eq!(h.reads, reads, "the value came with the preload");
}

#[test]
fn a_preload_stops_at_the_value_budget() {
    let mut h = Harness::new();
    h.ops([put("f", "one"), put("g", "two"), put("h", "three")]);
    let room = inodedb_core::cache::VALUE_BYTES + 3;
    let config = Config {
        preload: true,
        cache: Budget {
            value_bytes: room,
            ..Budget::default()
        },
        ..Config::default()
    };
    let h = Harness::with(h.storage.clone(), config);
    assert!(h.writer.cache().value_bytes() <= room, "within its budget");
    assert!(h.writer.cache().value_bytes() > 0, "up to it");
    assert!(stat_holds_every_file(&h));
}

fn stat_holds_every_file(h: &Harness) -> bool {
    h.writer.cache().inodes() >= 4
}

#[test]
fn a_standbys_preload_stops_at_its_budget() {
    use inodedb_core::standby::Follower;
    let mut h = Harness::new();
    h.ops([
        mkdir("a"),
        put("a/f", "1"),
        put("a/g", "2"),
        put("a/h", "3"),
    ]);
    let room = DIR_BYTES + 4 * (NAME_BYTES + 1) + INODE_BYTES;
    let config = Config {
        preload: true,
        cache: Budget {
            tree_bytes: room,
            ..Budget::default()
        },
        ..Config::default()
    };
    let mut follower = Follower::new(config, 1_000);
    loop {
        let actions = follower.drain();
        if actions.is_empty() {
            break;
        }
        for action in actions {
            let Action::Read { id, reads } = action else {
                panic!("a follower only reads: {action:?}");
            };
            follower
                .read_done(id, h.storage.read_now(&reads))
                .expect("reads what it wrote");
        }
    }
    let cache = follower.hand_over().cache;
    assert!(
        cache.tree_bytes() <= room,
        "{} of {room}",
        cache.tree_bytes()
    );
    assert!(cache.dirs() > 0, "the entries came first");
}

/// A standby starts as warm as a writer does: once it has read the store, a
/// read at a version it has reached needs no storage.
#[test]
fn a_standby_preloads_the_tree() {
    use inodedb_core::auth::Caller;
    use inodedb_core::standby::Follower;
    use inodedb_core::types::ROOT;
    use inodedb_core::writer::Answer;
    let mut h = Harness::new();
    h.ops([mkdir("a"), put("a/f", "one")]);
    let version = h.writer.version();
    let mut follower = Follower::new(preloading(), 1_000);
    loop {
        let actions = follower.drain();
        if actions.is_empty() {
            break;
        }
        for action in actions {
            let Action::Read { id, reads } = action else {
                panic!("a follower only reads: {action:?}");
            };
            follower
                .read_done(id, h.storage.read_now(&reads))
                .expect("reads what it wrote");
        }
    }
    let caller = Caller::Trusted(ROOT);
    let stat = Query::Stat(Target::Path(p("a/f")));
    let read = Query::Read {
        target: Target::Path(p("a/f")),
        version: None,
    };
    for query in [stat, read] {
        assert!(
            matches!(
                follower.answer_cached(&caller, &query, version, 1_000),
                Some(Answer::Query { .. })
            ),
            "{query:?}"
        );
    }
    let mut cold = Follower::new(Config::default(), 1_000);
    for action in cold.drain() {
        let Action::Read { id, reads } = action else {
            panic!("a follower only reads");
        };
        cold.read_done(id, h.storage.read_now(&reads))
            .expect("reads what it wrote");
    }
    assert!(
        cold.answer_cached(
            &caller,
            &Query::Stat(Target::Path(p("a/f"))),
            version,
            1_000
        )
        .is_none(),
        "without the preload its cache starts empty"
    );
}
