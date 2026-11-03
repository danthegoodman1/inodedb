//! Scripted scenarios for snapshots of a subscriber fed from a writer's
//! log.

use inodedb_core::cache::{Budget, Cache};
use inodedb_core::feed::{Change, Subscriber, route};
use inodedb_core::log::LogEntry;
use inodedb_core::query::{Query, QueryResult, Target, run};
use inodedb_core::snapshot::Miss;
use inodedb_core::types::{Error, Ino, ROOT, Version};
use inodedb_core::view::{Fact, Read};
use inodedb_sim::harness::*;

fn ino(h: &mut Harness, path: &str) -> Ino {
    match h.stat(path) {
        QueryResult::Stat { ino, .. } => ino,
        other => panic!("{path}: {other:?}"),
    }
}

/// Applies every committed entry after the subscriber's version.
fn follow(h: &Harness, subscriber: &mut Subscriber) {
    for (version, bytes) in h.storage.log_now(subscriber.version() + 1, usize::MAX) {
        let entry = LogEntry::decode(&bytes).expect("decodes");
        let change = route(&entry, subscriber.root(), ROOT).unwrap_or(Change {
            version,
            ..Change::default()
        });
        subscriber.apply(&change);
    }
}

/// What storage holds now for `read`.
fn fetch(h: &Harness, read: &Read) -> Fact {
    let result = h.storage.read_now(&[read.kv()]).remove(0);
    read.fact(result).expect("decodes")
}

/// Answers `query` through the snapshot at `at`, fetching what it misses
/// from storage as a client would; the subscriber has followed the log to
/// its end, so every fetched fact counts.
fn at(
    h: &Harness,
    subscriber: &Subscriber,
    at: Version,
    query: Query,
) -> Result<QueryResult, Miss> {
    let mut fetched = Cache::new();
    for _ in 0..32 {
        match subscriber.run_at(at, subscriber.root(), &query, &fetched) {
            Err(Miss::Reads(reads)) => {
                for read in reads {
                    fetched.fill(fetch(h, &read));
                }
            }
            answered => return answered,
        }
    }
    panic!("the snapshot kept missing")
}

fn names(result: Result<QueryResult, Miss>) -> Vec<String> {
    match result {
        Ok(QueryResult::List { entries, .. }) => entries
            .iter()
            .map(|listed| String::from_utf8_lossy(&listed.name).into_owned())
            .collect(),
        other => panic!("expected a listing, got {other:?}"),
    }
}

fn list(path: &str) -> Query {
    Query::List {
        target: Target::Path(p(path)),
        after: None,
        prefix: None,
        limit: usize::MAX,
        budget: usize::MAX,
        attrs: true,
        values: false,
    }
}

fn read(path: &str) -> Query {
    Query::Read {
        target: Target::Path(p(path)),
        version: None,
    }
}

fn value(result: Result<QueryResult, Miss>) -> String {
    match result {
        Ok(QueryResult::Read { value, .. }) => String::from_utf8_lossy(&value).into_owned(),
        other => panic!("expected a value, got {other:?}"),
    }
}

/// Fills the subscriber with a directory's listing and its entries'
/// inodes, as a client's listing with attributes would.
fn cache_listing(h: &mut Harness, subscriber: &mut Subscriber, path: &str) {
    let dir = ino(h, path);
    let version = subscriber.version();
    let listing = fetch(h, &Read::Entries { dir, limit: None });
    if let Fact::Listing { entries, .. } = &listing {
        for (_, entry) in entries {
            let child = entry.expect("present").child;
            subscriber.fill(fetch(h, &Read::Inode(child)), version);
        }
    }
    subscriber.fill(listing, version);
}

#[test]
fn a_snapshot_reads_as_of_its_version() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), put("r/a", "1"), put("r/b", "2")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    let snapshot = subscriber.open_snapshot();
    h.ops([put("r/a", "3"), mv("r/b", "r/c"), mkdir("r/d")]);
    h.ops([unlink("r/c")]);
    follow(&h, &mut subscriber);
    assert_eq!(names(at(&h, &subscriber, snapshot, list(""))), ["a", "b"]);
    assert_eq!(value(at(&h, &subscriber, snapshot, read("a"))), "1");
    assert_eq!(value(at(&h, &subscriber, snapshot, read("b"))), "2");
    assert_eq!(
        at(&h, &subscriber, snapshot, Query::Stat(Target::Path(p("d")))),
        Ok(QueryResult::Failed(Error::NotFound))
    );
    // The cache itself moves on.
    let now = subscriber.open_snapshot();
    assert_eq!(names(at(&h, &subscriber, now, list(""))), ["a", "d"]);
    assert_eq!(value(at(&h, &subscriber, now, read("a"))), "3");
}

/// A file moved into a directory that leaves the subtree in the same commit
/// goes with it, though a name in the commit gained it. Its later changes reach
/// no feed, so a snapshot from before finds it lost, never its current state.
#[test]
fn a_file_leaving_inside_a_leaving_directory_is_lost_to_older_snapshots() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), mkdir("r/sub"), put("r/f", "old"), mkdir("out")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    let snapshot = subscriber.open_snapshot();
    h.ops([mv("r/f", "r/sub/f"), mv("r/sub", "out/sub")]);
    follow(&h, &mut subscriber);
    h.ops([put("out/sub/f", "new")]);
    follow(&h, &mut subscriber);
    assert_eq!(
        at(&h, &subscriber, snapshot, read("f")),
        Err(Miss::TooOld),
        "the snapshot read the file's state from outside its feed"
    );
}

/// A file gained beneath a directory whose parent leaves goes too, though the
/// directory that gained it lost no name of its own.
#[test]
fn a_file_leaving_beneath_a_leaving_directory_is_lost_to_older_snapshots() {
    let mut h = Harness::new();
    h.ops([mkdir_p("r/sub/deep"), put("r/f", "old"), mkdir("out")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    let snapshot = subscriber.open_snapshot();
    h.ops([mv("r/f", "r/sub/deep/f"), mv("r/sub", "out/sub")]);
    follow(&h, &mut subscriber);
    h.ops([put("out/sub/deep/f", "new")]);
    follow(&h, &mut subscriber);
    assert_eq!(at(&h, &subscriber, snapshot, read("f")), Err(Miss::TooOld));
}

/// What the cache held of such a file still answers.
#[test]
fn a_cached_file_leaving_inside_a_leaving_directory_reads_as_it_was() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), mkdir("r/sub"), put("r/f", "old"), mkdir("out")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    cache_listing(&mut h, &mut subscriber, "r");
    let snapshot = subscriber.open_snapshot();
    h.ops([mv("r/f", "r/sub/f"), mv("r/sub", "out/sub")]);
    follow(&h, &mut subscriber);
    h.ops([put("out/sub/f", "new")]);
    follow(&h, &mut subscriber);
    assert_eq!(value(at(&h, &subscriber, snapshot, read("f"))), "old");
}

/// A rename inside the subtree in a commit where nothing leaves keeps the
/// file in sight, so snapshots read it as before.
#[test]
fn a_rename_that_stays_inside_keeps_its_file_in_sight() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), mkdir("r/sub"), put("r/f", "old")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    let snapshot = subscriber.open_snapshot();
    h.ops([mv("r/f", "r/sub/f")]);
    follow(&h, &mut subscriber);
    h.ops([put("r/sub/f", "new")]);
    follow(&h, &mut subscriber);
    assert_eq!(value(at(&h, &subscriber, snapshot, read("f"))), "old");
}

#[test]
fn a_removed_directory_reads_as_the_cache_held_it() {
    let mut h = Harness::new();
    h.ops([
        mkdir_p("r/t"),
        mkdir("r/u"),
        put("r/t/f", "v"),
        put("r/u/g", "w"),
    ]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    cache_listing(&mut h, &mut subscriber, "r/t");
    let snapshot = subscriber.open_snapshot();
    h.ops([remove_all("r/t"), remove_all("r/u")]);
    follow(&h, &mut subscriber);
    assert_eq!(h.writer.detached(), 0, "the subtrees are freed");
    assert_eq!(names(at(&h, &subscriber, snapshot, list("t"))), ["f"]);
    assert_eq!(value(at(&h, &subscriber, snapshot, read("t/f"))), "v");
    // Nothing kept u's entries, and storage no longer holds them.
    assert_eq!(at(&h, &subscriber, snapshot, list("u")), Err(Miss::TooOld));
}

#[test]
fn a_directory_that_left_reads_as_it_left() {
    let mut h = Harness::new();
    h.ops([mkdir_p("r/t"), mkdir("x"), put("r/t/f", "v")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    cache_listing(&mut h, &mut subscriber, "r/t");
    let snapshot = subscriber.open_snapshot();
    // Outside the subtree, t changes where the feed can't see.
    h.ops([mv("r/t", "x/t")]);
    h.ops([put("x/t/g", "w"), unlink("x/t/f")]);
    h.ops([mv("x/t", "r/t2")]);
    follow(&h, &mut subscriber);
    cache_listing(&mut h, &mut subscriber, "r/t2");
    assert_eq!(names(at(&h, &subscriber, snapshot, list(""))), ["t"]);
    assert_eq!(names(at(&h, &subscriber, snapshot, list("t"))), ["f"]);
    let now = subscriber.open_snapshot();
    assert_eq!(names(at(&h, &subscriber, now, list("t2"))), ["g"]);
}

#[test]
fn undo_past_the_budget_ends_the_oldest_snapshot() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), put("r/a", "0")]);
    let root = ino(&mut h, "r");
    let budget = Budget {
        undo_bytes: 1_000,
        ..Budget::default()
    };
    let mut subscriber = Subscriber::with_budget(root, h.writer.version(), budget);
    let old = subscriber.open_snapshot();
    for round in 0..5 {
        h.ops([put("r/a", &round.to_string())]);
        follow(&h, &mut subscriber);
    }
    let newer = subscriber.open_snapshot();
    for round in 5..20 {
        h.ops([put("r/a", &round.to_string())]);
        follow(&h, &mut subscriber);
    }
    assert_eq!(at(&h, &subscriber, old, read("a")), Err(Miss::TooOld));
    assert!(subscriber.undo().bytes() <= 1_000);
    assert_eq!(
        subscriber.oldest(),
        old,
        "an ended snapshot holds its lease until closed"
    );
    subscriber.close_snapshot(old);
    subscriber.close_snapshot(newer);
    assert_eq!(subscriber.oldest(), subscriber.version());
    let recent = subscriber.open_snapshot();
    h.ops([put("r/a", "last")]);
    follow(&h, &mut subscriber);
    assert_eq!(value(at(&h, &subscriber, recent, read("a"))), "19");
}

#[test]
fn closing_the_last_snapshot_drops_its_records() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), put("r/a", "0")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    let first = subscriber.open_snapshot();
    h.ops([put("r/a", "1"), put("r/b", "1")]);
    follow(&h, &mut subscriber);
    let second = subscriber.open_snapshot();
    h.ops([put("r/a", "2")]);
    follow(&h, &mut subscriber);
    assert_eq!(subscriber.oldest(), first);
    subscriber.close_snapshot(first);
    assert_eq!(subscriber.oldest(), second);
    assert_eq!(value(at(&h, &subscriber, second, read("a"))), "1");
    subscriber.close_snapshot(second);
    assert_eq!(subscriber.undo().bytes(), 0);
    assert_eq!(subscriber.oldest(), subscriber.version());
    assert!(
        run(subscriber.cache(), root, &read("a")).is_err(),
        "nothing cached"
    );
}
