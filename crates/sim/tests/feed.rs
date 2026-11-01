//! Scripted scenarios for subscribers fed from a writer's log.

use inodedb_core::feed::{Subscriber, route};
use inodedb_core::log::LogEntry;
use inodedb_core::query::{self, Query, QueryResult, Target};
use inodedb_core::types::{Entry, Error, Ino, Kind, Life, Name, ROOT, Version};
use inodedb_core::view::Fact;
use inodedb_sim::harness::*;
use std::collections::BTreeSet;

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
        assert_eq!(entry.version, version);
        match route(&entry, subscriber.root(), ROOT) {
            Some(change) => subscriber.apply(&change),
            None => subscriber.apply(&inodedb_core::feed::Change {
                version,
                ..Default::default()
            }),
        }
    }
}

/// A directory that leaves and comes back changed brings nothing stale
/// with it, though the cache lacked a directory between the root and what
/// it held.
#[test]
fn a_directory_that_returns_brings_nothing_stale() {
    let mut h = Harness::new();
    h.ops([mkdir_p("r/b/c"), mkdir("x")]);
    let (root, b, c) = (ino(&mut h, "r"), ino(&mut h, "r/b"), ino(&mut h, "r/b/c"));
    let mut subscriber = Subscriber::new(root, h.writer.version());
    // The feed caches c's new entry and f's inode, but not b's entries.
    h.ops([put("r/b/c/f", "v")]);
    follow(&h, &mut subscriber);
    h.ops([mv("r/b", "x/b")]);
    h.ops([unlink("x/b/c/f")]);
    h.ops([mv("x/b", "r/b2")]);
    follow(&h, &mut subscriber);
    let at: Version = subscriber.version();
    // A read of b's entries fills, as a client's miss would.
    subscriber.fill(
        Fact::Listing {
            dir: b,
            entries: vec![(
                Name::from(&b"c"[..]),
                Some(Entry {
                    child: c,
                    kind: Kind::Dir,
                    version: 2,
                }),
            )],
            complete: true,
        },
        at,
    );
    let stat = query::run(
        subscriber.cache(),
        root,
        &Query::Stat(Target::Path(p("b2/c/f"))),
    );
    match stat {
        Ok(QueryResult::Failed(Error::NotFound)) | Err(_) => {}
        other => panic!("a file the feed never showed gone answers {other:?}"),
    }
}

/// Removing directories forgets only them: what else the cache holds
/// stays.
#[test]
fn removing_a_directory_keeps_the_rest_cached() {
    let mut h = Harness::new();
    h.ops([mkdir_p("r/a"), mkdir("r/gone")]);
    let root = ino(&mut h, "r");
    let mut subscriber = Subscriber::new(root, h.writer.version());
    h.ops([put("r/a/f", "v")]);
    h.ops([rmdir("r/gone")]);
    follow(&h, &mut subscriber);
    let a = ino(&mut h, "r/a");
    assert!(subscriber.cache().holds(a), "a's entries stay");
}

fn inode_at(h: &mut Harness, path: &str) -> inodedb_core::types::Inode {
    match h.stat(path) {
        QueryResult::Stat { inode, .. } => inode,
        other => panic!("{path}: {other:?}"),
    }
}

fn size(subscriber: &Subscriber, root: Ino, path: &str) -> Option<u64> {
    match query::run(
        subscriber.cache(),
        root,
        &Query::Stat(Target::Path(p(path))),
    ) {
        Ok(QueryResult::Stat { inode, .. }) => Some(inode.size),
        _ => None,
    }
}

/// The root's listing, as a client's miss would fill it.
fn fill_root(h: &mut Harness, subscriber: &mut Subscriber, root: Ino) {
    let (version, listing) = h.query(Query::List {
        target: Target::Path(p("r")),
        after: None,
        prefix: None,
        limit: usize::MAX,
        budget: usize::MAX,
        attrs: false,
        values: false,
    });
    let QueryResult::List { entries, .. } = listing else {
        panic!("{listing:?}");
    };
    subscriber.fill(
        Fact::Listing {
            dir: root,
            entries: entries
                .into_iter()
                .map(|listed| (listed.name, Some(listed.entry)))
                .collect(),
            complete: true,
        },
        version,
    );
}

/// A file that leaves and comes back changed is read again: the feed never
/// showed what happened to it outside.
#[test]
fn a_file_that_returns_is_read_again() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), mkdir("x"), put("r/f", "a")]);
    let (root, f) = (ino(&mut h, "r"), ino(&mut h, "r/f"));
    let mut subscriber = Subscriber::new(root, h.writer.version());
    fill_root(&mut h, &mut subscriber, root);
    let inode = inode_at(&mut h, "r/f");
    subscriber.fill(Fact::Inode(f, Some(inode)), h.writer.version());
    assert_eq!(size(&subscriber, root, "f"), Some(1));
    h.ops([mv("r/f", "x/f")]);
    h.ops([put("x/f", "changed")]);
    h.ops([mv("x/f", "r/f")]);
    follow(&h, &mut subscriber);
    assert_ne!(
        size(&subscriber, root, "f"),
        Some(1),
        "the old record is gone"
    );
}

/// A read from before something left the subtree fills nothing: moving it
/// forward would need changes the feed never showed.
#[test]
fn a_read_from_before_a_departure_fills_nothing() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), mkdir("x"), put("r/f", "a")]);
    let (root, f) = (ino(&mut h, "r"), ino(&mut h, "r/f"));
    let mut subscriber = Subscriber::new(root, h.writer.version());
    let (read_at, old) = (h.writer.version(), inode_at(&mut h, "r/f"));
    h.ops([mv("r/f", "x/f")]);
    h.ops([put("x/f", "changed")]);
    h.ops([mv("x/f", "r/f")]);
    follow(&h, &mut subscriber);
    fill_root(&mut h, &mut subscriber, root);
    subscriber.fill(Fact::Inode(f, Some(old)), read_at);
    assert_ne!(
        size(&subscriber, root, "f"),
        Some(1),
        "the old read stays out"
    );
}

/// A read newer than the feed waits for the feed, so the cache never
/// answers with a later state than its version.
#[test]
fn a_read_newer_than_the_feed_waits_for_it() {
    let mut h = Harness::new();
    h.ops([mkdir("r"), put("r/f", "a")]);
    let (root, f) = (ino(&mut h, "r"), ino(&mut h, "r/f"));
    let mut subscriber = Subscriber::new(root, h.writer.version());
    fill_root(&mut h, &mut subscriber, root);
    h.ops([put("r/f", "bb")]);
    let newer = inode_at(&mut h, "r/f");
    subscriber.fill(Fact::Inode(f, Some(newer)), h.writer.version());
    assert_eq!(
        size(&subscriber, root, "f"),
        None,
        "not before the feed arrives"
    );
    follow(&h, &mut subscriber);
    assert_eq!(size(&subscriber, root, "f"), Some(2));
}

/// Freeing a detached subtree reaches the subscribers that saw it before it
/// detached, wherever its parent went since, and no others.
#[test]
fn freeing_a_subtree_reaches_those_that_saw_it() {
    let mut h = Harness::new();
    h.ops([mkdir_p("a/p/t/q"), put("a/p/t/q/f", "v"), mkdir("b")]);
    let (a, b) = (ino(&mut h, "a"), ino(&mut h, "b"));
    let subtree: BTreeSet<Ino> = ["a/p/t", "a/p/t/q", "a/p/t/q/f"]
        .into_iter()
        .map(|path| ino(&mut h, path))
        .collect();
    let removed = h.ops([remove_all("a/p/t"), mv("a/p", "b/p")]).version;
    assert_eq!(h.writer.detached(), 0);
    let freed = |root: Ino| -> BTreeSet<Ino> {
        let mut freed = BTreeSet::new();
        for (_, bytes) in h.storage.log_now(removed + 1, usize::MAX) {
            let entry = LogEntry::decode(&bytes).expect("decodes");
            for effect in route(&entry, root, ROOT).map_or(Vec::new(), |change| change.inodes) {
                if effect
                    .after
                    .is_some_and(|inode| matches!(inode.life, Life::Freed(_)))
                {
                    freed.insert(effect.ino);
                }
            }
        }
        freed
    };
    assert_eq!(freed(a), subtree);
    assert_eq!(freed(b), BTreeSet::new());
}
