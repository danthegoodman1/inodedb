//! Scripted scenarios for leases, release, claims and acknowledgements.

use inodedb_core::auth::{Caller, Grant};
use inodedb_core::delta::Freed;
use inodedb_core::keys::KvRead;
use inodedb_core::lease::{ClaimError, LeaseError};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Error, Ino, ROOT};
use inodedb_core::writer::{Action, Answer, CommitOutcome, Config, Writer};
use inodedb_sim::harness::*;
use inodedb_storage::Memory;

const ADMIN: Caller = Caller::Trusted(ROOT);

/// A writer started over `storage`, stopped at its first commit, and that
/// commit's id.
fn at_its_first_commit(storage: &Memory) -> (Writer, u64) {
    let mut writer = Writer::new(Config::default(), Harness::writer_id(), 1_000, [7; 32]);
    loop {
        for action in writer.drain() {
            match action {
                Action::Read { id, reads } => writer.read_done(id, storage.read_now(&reads)),
                Action::Commit { id, .. } => return (writer, id),
                other => panic!("a starting writer asked for {other:?}"),
            }
        }
    }
}

fn ino(h: &mut Harness, path: &str) -> Ino {
    match h.stat(path) {
        QueryResult::Stat { ino, .. } => ino,
        other => panic!("{path}: {other:?}"),
    }
}

fn by_handle(h: &mut Harness, ino: Ino) -> QueryResult {
    h.query(Query::Stat(Target::Ino(ino))).1
}

#[test]
fn freed_records_wait_for_every_lease() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), put("t/f", "doomed")]);
    let f = ino(&mut h, "t/f");
    let at = h.writer.version();
    let lease = h.open_lease(ADMIN, at).expect("opens").lease;
    let freed_at = h.ops([unlink("t/f")]).version;
    assert_eq!(
        h.claim(ADMIN, 10).1,
        [],
        "the lease holds the horizon below the free"
    );
    assert!(
        matches!(by_handle(&mut h, f), QueryResult::Stat { .. }),
        "still readable"
    );
    assert_eq!(
        h.beat(ADMIN, lease, at - 1),
        Err(LeaseError::Lowered { oldest: at })
    );
    h.beat(ADMIN, lease, freed_at).expect("beats");
    let (claim, entries) = h.claim(ADMIN, 10);
    assert_eq!(entries.len(), 1);
    assert_eq!(
        (entries[0].version, entries[0].freed),
        (freed_at, Freed::Inode(f))
    );
    assert_eq!(h.claim(ADMIN, 10).1, [], "claimed once");
    h.ack(ADMIN, claim, &entries).expect("acknowledges");
    assert_eq!(by_handle(&mut h, f), QueryResult::Failed(Error::NotFound));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    assert!(h.storage.dump().iter().all(|(key, _)| !matches!(
        inodedb_core::keys::parse(key),
        Some(inodedb_core::keys::Parsed::Inode(ino)) if ino == f
    )));
}

#[test]
fn a_lease_starts_at_the_horizon_and_runs_out() {
    let config = Config {
        lease_ttl: 1_000,
        ..Config::default()
    };
    let mut h = Harness::with(inodedb_storage::Memory::new(), config);
    h.ops([mkdir("t")]);
    let at = h.writer.version();
    assert_eq!(h.writer.horizon(), at, "no leases: everything is released");
    assert_eq!(
        h.open_lease(ADMIN, at - 1),
        Err(LeaseError::TooOld { horizon: at })
    );
    let lease = h.open_lease(ADMIN, at).expect("opens").lease;
    h.ops([put("t/f", "x"), unlink("t/f")]);
    assert_eq!(h.writer.horizon(), at);
    h.tick(1_000);
    assert_eq!(h.beat(ADMIN, lease, at), Err(LeaseError::Expired));
    assert_eq!(
        h.writer.horizon(),
        h.writer.version(),
        "the horizon moves on"
    );
}

#[test]
fn claims_stay_under_the_consumers_root() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), mkdir("u"), put("t/a", "1"), put("u/b", "2")]);
    h.ops([unlink("t/a"), unlink("u/b")]);
    let at_t = h.mint(ADMIN, "t", Grant::Write, 0);
    let reader = h.mint(ADMIN, "t", Grant::Read, 0);
    let (_, mine) = h.claim(at_t, 10);
    assert_eq!(mine.len(), 1, "{mine:?}");
    assert_eq!(mine[0].freed, Freed::Inode(ino_of_freed(&mine[0])));
    let token = h.token();
    h.writer.claim(token, reader, 10);
    h.run();
    assert!(matches!(
        h.answer(token),
        inodedb_core::writer::Answer::Denied(_)
    ));
    let (_, rest) = h.claim(ADMIN, 10);
    assert_eq!(rest.len(), 1, "the root's consumer gets what remains");
}

/// Freeing a detached subtree queues records naming the directories above
/// it as it detached, so consumers there claim them, wherever those
/// directories went since.
#[test]
fn a_subtrees_frees_go_to_consumers_above_it_as_it_detached() {
    let mut h = Harness::new();
    h.ops([mkdir_p("a/p/t/q"), put("a/p/t/q/f", "v"), mkdir("b")]);
    h.ops([remove_all("a/p/t"), mv("a/p", "b/p")]);
    let at_a = h.mint(ADMIN, "a", Grant::Write, 0);
    let at_b = h.mint(ADMIN, "b", Grant::Write, 0);
    assert_eq!(h.claim(at_b, 10).1, []);
    let (_, mine) = h.claim(at_a, 10);
    assert_eq!(
        mine.len(),
        3,
        "the top, its directory and its file: {mine:?}"
    );
}

fn ino_of_freed(entry: &inodedb_core::lease::FreeEntry) -> Ino {
    match entry.freed {
        Freed::Inode(ino) | Freed::Value(ino, _) => ino,
    }
}

#[test]
fn a_claim_that_runs_out_returns_its_entries() {
    let config = Config {
        claim_ttl: 1_000,
        ..Config::default()
    };
    let mut h = Harness::with(inodedb_storage::Memory::new(), config);
    h.ops([put("f", "x")]);
    h.ops([unlink("f")]);
    let (first, entries) = h.claim(ADMIN, 10);
    assert_eq!(entries.len(), 1);
    h.tick(999);
    assert_eq!(h.ack(ADMIN, first, &[]), Ok(h.writer.version()), "renews");
    h.tick(999);
    assert_eq!(h.claim(ADMIN, 10).1, [], "still claimed");
    h.tick(1);
    let (second, again) = h.claim(ADMIN, 10);
    assert_eq!(again, entries);
    assert_eq!(h.ack(ADMIN, first, &entries), Err(ClaimError::Unknown));
    h.ack(ADMIN, second, &again).expect("acknowledges");
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_replaced_value_goes_alone() {
    let mut h = Harness::new();
    h.ops([put("f", "old")]);
    let f = ino(&mut h, "f");
    let QueryResult::Stat { inode, .. } = h.stat("f") else {
        panic!()
    };
    h.ops([put("f", "new")]);
    let (claim, entries) = h.claim(ADMIN, 10);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].freed, Freed::Value(f, inode.value));
    h.ack(ADMIN, claim, &entries).expect("acknowledges");
    let old = h.query(Query::Read {
        target: Target::Ino(f),
        version: Some(inode.value),
    });
    assert_eq!(old.1, QueryResult::Failed(Error::NotFound));
    let current = h.query(Query::Read {
        target: Target::Path(p("f")),
        version: None,
    });
    assert!(matches!(current.1, QueryResult::Read { ref value, .. } if &value[..] == b"new"));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_lease_survives_its_writer() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let at = h.writer.version();
    let lease = h.open_lease(ADMIN, at).expect("opens").lease;
    h.ops([put("t/f", "x")]);
    h.ops([unlink("t/f")]);
    let mut restarted = Harness::with(h.storage.clone(), Config::default());
    assert_eq!(
        restarted.writer.horizon(),
        at,
        "the stored lease holds the horizon"
    );
    assert_eq!(restarted.claim(ADMIN, 10).1, []);
    restarted
        .beat(ADMIN, lease, restarted.writer.version())
        .expect("beats");
    assert_eq!(restarted.claim(ADMIN, 10).1.len(), 1);
}

/// A lease that runs out while a writer starts comes back when the writer
/// starts over, with nothing queued against it.
#[test]
fn a_writer_that_starts_over_keeps_the_leases_it_loads() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let at = h.writer.version();
    let lease = h.open_lease(ADMIN, at).expect("opens").lease;
    let (mut writer, commit) = at_its_first_commit(&h.storage);
    writer.set_now(1_000 + Config::default().lease_ttl);
    writer.commit_done(commit, CommitOutcome::Failed);
    let mut started = Harness::with_writer(h.storage.clone(), writer);
    let version = started.writer.version();
    started.beat(ADMIN, lease, version).expect("beats");
    started.ops([mkdir("after")]);
    started.ops([mkdir("later")]);
    let mut next = Harness::with(started.storage.clone(), Config::default());
    let version = next.writer.version();
    next.beat(ADMIN, lease, version)
        .expect("the renewed lease stays in storage");
}

/// A lease another writer ended while this one started is gone when this
/// one starts over.
#[test]
fn a_writer_that_starts_over_drops_a_lease_gone_from_storage() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let at = h.writer.version();
    let lease = h.open_lease(ADMIN, at).expect("opens").lease;
    let (mut writer, commit) = at_its_first_commit(&h.storage);
    let mut other = Harness::with(h.storage.clone(), Config::default());
    other.writer.release_lease(1_000, ADMIN, lease);
    other.run();
    other.ops([mkdir("after")]);
    writer.commit_done(commit, CommitOutcome::Failed);
    let mut started = Harness::with_writer(other.storage.clone(), writer);
    assert_eq!(started.writer.leases().len(), 0);
    let version = started.writer.version();
    assert_eq!(
        started.beat(ADMIN, lease, version),
        Err(LeaseError::Expired)
    );
}

#[test]
fn lease_ids_never_repeat_across_writers() {
    let config = Config {
        lease_ttl: 1_000,
        ..Config::default()
    };
    let mut h = Harness::with(inodedb_storage::Memory::new(), config.clone());
    h.ops([mkdir("t")]);
    let at = h.writer.version();
    let first = h.open_lease(ADMIN, at).expect("opens").lease;
    let second = h.open_lease(ADMIN, at).expect("opens").lease;
    // The second runs out and its record goes; the first stays stored.
    h.beat(ADMIN, first, at).expect("beats");
    h.tick(900);
    h.beat(ADMIN, first, at).expect("beats");
    h.tick(200);
    assert_eq!(h.beat(ADMIN, second, at), Err(LeaseError::Expired));
    h.ops([mkdir("u")]);
    let mut h = Harness::with(h.storage.clone(), config);
    let at = h.writer.version();
    let third = h.open_lease(ADMIN, at).expect("opens").lease;
    assert!(third > second, "{third} reuses an id up to {second}");
    assert_eq!(h.beat(ADMIN, second, at), Err(LeaseError::Expired));
}

#[test]
fn a_restarted_writer_releases_no_less_than_it_claimed() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), put("t/f", "v")]);
    let at = h.writer.version();
    let lease = h.open_lease(ADMIN, at).expect("opens").lease;
    let freed_at = h.ops([unlink("t/f")]).version;
    // A heartbeat raises the horizon past the free, with no batch to
    // carry it to storage.
    h.beat(ADMIN, lease, freed_at).expect("beats");
    let (_, entries) = h.claim(ADMIN, 10);
    assert_eq!(entries.len(), 1, "claimed once storage holds the horizon");
    let mut h = Harness::with(h.storage.clone(), Config::default());
    assert!(h.writer.horizon() >= freed_at);
    assert!(matches!(
        h.open_lease(ADMIN, freed_at - 1),
        Err(LeaseError::TooOld { .. })
    ));
}

fn listed(page: &FreePage) -> Vec<Freed> {
    page.entries.iter().map(|entry| entry.freed).collect()
}

/// A listing holds entries a lease holds and entries a consumer claimed,
/// and claims none itself.
#[test]
fn a_listing_holds_every_entry_and_claims_none() {
    let mut h = Harness::new();
    h.ops([put("a", "1"), put("b", "2")]);
    let (a, b) = (ino(&mut h, "a"), ino(&mut h, "b"));
    h.ops([unlink("a")]);
    let at = h.writer.version();
    h.open_lease(ADMIN, at).expect("opens");
    h.ops([unlink("b")]);
    let page = h.list_freed(ADMIN, None, 10);
    assert_eq!(listed(&page), [Freed::Inode(a), Freed::Inode(b)]);
    assert_eq!(page.next, None, "one page holds the queue");
    let (claim, claimed) = h.claim(ADMIN, 10);
    assert_eq!(
        claimed.iter().map(|entry| entry.freed).collect::<Vec<_>>(),
        [Freed::Inode(a)],
        "the listing claimed nothing, and the lease holds b"
    );
    assert_eq!(
        listed(&h.list_freed(ADMIN, None, 10)),
        [Freed::Inode(a), Freed::Inode(b)],
        "a claimed entry stays listed"
    );
    h.ack(ADMIN, claim, &claimed).expect("acknowledges");
    assert_eq!(listed(&h.list_freed(ADMIN, None, 10)), [Freed::Inode(b)]);
}

/// A listing under a directory holds what a listing of the whole queue
/// would: entries a lease holds, and entries a consumer claimed.
#[test]
fn a_listing_under_a_directory_holds_every_entry_and_claims_none() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), put("t/a", "1"), put("t/b", "2")]);
    let (a, b) = (ino(&mut h, "t/a"), ino(&mut h, "t/b"));
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    h.ops([unlink("t/a")]);
    let at = h.writer.version();
    h.open_lease(ADMIN, at).expect("opens");
    h.ops([unlink("t/b")]);
    assert_eq!(
        listed(&h.list_freed(at_t, None, 10)),
        [Freed::Inode(a), Freed::Inode(b)]
    );
    let (_, claimed) = h.claim(at_t, 10);
    assert_eq!(
        claimed.iter().map(|entry| entry.freed).collect::<Vec<_>>(),
        [Freed::Inode(a)],
        "the listing claimed nothing, and the lease holds b"
    );
    assert_eq!(
        listed(&h.list_freed(at_t, None, 10)),
        [Freed::Inode(a), Freed::Inode(b)],
        "a claimed entry stays listed"
    );
}

#[test]
fn a_listing_holds_a_replaced_value() {
    let mut h = Harness::new();
    h.ops([put("f", "old")]);
    let f = ino(&mut h, "f");
    let QueryResult::Stat { inode, .. } = h.stat("f") else {
        panic!()
    };
    h.ops([put("f", "new")]);
    assert_eq!(
        listed(&h.list_freed(ADMIN, None, 10)),
        [Freed::Value(f, inode.value)]
    );
}

#[test]
fn listings_stay_under_their_root_and_page_through_the_queue() {
    let mut h = Harness::new();
    h.ops([
        mkdir("t"),
        mkdir("u"),
        put("t/a", "1"),
        put("u/b", "2"),
        put("t/c", "3"),
    ]);
    let (a, c) = (ino(&mut h, "t/a"), ino(&mut h, "t/c"));
    h.ops([unlink("t/a"), unlink("u/b"), unlink("t/c")]);
    let at_t = h.mint(ADMIN, "t", Grant::Write, 0);
    let reader = h.mint(ADMIN, "t", Grant::Read, 0);
    let token = h.token();
    h.writer.list_freed(token, reader, None, 10);
    h.run();
    assert!(matches!(
        h.answer(token),
        inodedb_core::writer::Answer::Denied(_)
    ));
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = h.list_freed(at_t, after, 1);
        assert!(page.entries.len() <= 1, "{page:?}");
        seen.extend(listed(&page));
        assert!(seen.len() <= 2, "the listing goes round: {seen:?}");
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, [Freed::Inode(a), Freed::Inode(c)]);
    assert_eq!(h.list_freed(ADMIN, None, 10).entries.len(), 3);
}

/// A subtree's contents reach the queue as the writer frees it, after the
/// detach commits; until then a listing's `freed_through` stays below the
/// detach, for keys above the subtree.
#[test]
fn a_listing_reports_freeing_done_only_past_every_detached_subtree() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/d"), put("t/d/f", "v"), mkdir("u")]);
    let at_u = h.mint(ADMIN, "u", Grant::Write, 0);
    let token = h.token();
    h.writer.submit(
        token,
        ADMIN,
        inodedb_core::ops::Batch {
            ops: vec![remove_all("t/d")],
            ..Default::default()
        },
    );
    let held = h.run_until(token);
    let inodedb_core::writer::Answer::Commit(detach) = h.answer(token) else {
        panic!("the detach commits")
    };
    assert!(!held.is_empty(), "the freeing waits to commit");
    assert_eq!(h.writer.detached(), 1);
    let page = h.list_freed(ADMIN, None, 10);
    assert_eq!(page.freed_through, detach.version - 1);
    assert_eq!(page.entries, [], "nothing is freed yet");
    let page = h.list_freed(at_u, None, 10);
    assert_eq!(
        page.freed_through, page.version,
        "the subtree lies outside u"
    );
    h.resume(held);
    assert_eq!(h.writer.detached(), 0);
    let page = h.list_freed(ADMIN, None, 10);
    assert_eq!(page.freed_through, page.version);
    assert!(page.freed_through > detach.version);
    assert_eq!(page.entries.len(), 2, "the subtree's top and its file");
}

/// A queue that holds one consumer's entries behind thousands of another's.
fn a_long_queue() -> (Harness, Caller, [Ino; 2]) {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a"), mkdir("u")]);
    for chunk in 0..3 {
        h.ops((0..1_000).map(|n| put(&format!("u/f{chunk}-{n}"), "v")));
        h.ops((0..1_000).map(|n| unlink(&format!("u/f{chunk}-{n}"))));
    }
    h.ops([put("t/x", "1"), put("t/a/y", "2")]);
    let mine = [ino(&mut h, "t/x"), ino(&mut h, "t/a/y")];
    h.ops([unlink("t/x"), unlink("t/a/y")]);
    let at_t = h.mint(ADMIN, "t", Grant::Write, 0);
    (h, at_t, mine)
}

/// A consumer reads the part of the queue under its root, however much
/// the queue holds for others.
#[test]
fn a_claim_reads_its_own_roots_entries_alone() {
    let (mut h, at_t, [x, y]) = a_long_queue();
    assert!(h.writer.queue_indexed());
    let before = h.reads;
    let (claim, mine) = h.claim(at_t, 10);
    assert_eq!(
        mine.iter().map(|entry| entry.freed).collect::<Vec<_>>(),
        [Freed::Inode(x), Freed::Inode(y)]
    );
    assert_eq!(
        h.reads - before,
        2,
        "a page of its part of the queue, then its two entries' records"
    );
    h.ack(at_t, claim, &mine).expect("acknowledges");
    let before = h.reads;
    assert_eq!(h.claim(at_t, 10).1, []);
    assert_eq!(h.reads - before, 1);
    // The root's consumer reads the queue itself.
    let before = h.reads;
    assert_eq!(h.claim(ADMIN, 10).1.len(), 10);
    assert_eq!(h.reads - before, 1);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_listing_reads_its_own_roots_entries_alone() {
    let (mut h, at_t, [x, y]) = a_long_queue();
    let before = h.reads;
    let page = h.list_freed(at_t, None, 10);
    assert_eq!(listed(&page), [Freed::Inode(x), Freed::Inode(y)]);
    assert_eq!(page.next, None, "its first page reaches its entries' end");
    assert_eq!(h.reads - before, 2);
    // A page of the whole queue covers a page's worth of other entries.
    assert_eq!(h.list_freed(ADMIN, None, 2_000).entries.len(), 1_024);
}

/// An entry lies under every directory above what it freed: each one's
/// consumer finds it, and an acknowledgement takes it from them all.
#[test]
fn an_entry_is_queued_under_every_directory_above_it() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a/b"), mkdir_p("u/c"), put("t/a/b/f", "v")]);
    let f = ino(&mut h, "t/a/b/f");
    // A value that `Put` replaces lies under every directory holding a
    // link to its file.
    h.ops([put("t/a/g", "old"), link("t/a/g", "u/c/g")]);
    let g = ino(&mut h, "t/a/g");
    let QueryResult::Stat { inode, .. } = h.stat("t/a/g") else {
        panic!()
    };
    h.ops([unlink("t/a/b/f"), put("u/c/g", "new")]);
    let roots = ["t", "t/a", "t/a/b", "u", "u/c"].map(|root| (root, ino(&mut h, root)));
    let mut under = |root: Ino| listed(&h.list_freed(Caller::Trusted(root), None, 10));
    let (file, value) = (Freed::Inode(f), Freed::Value(g, inode.value));
    for (path, root) in roots {
        let expected = match path {
            "t" | "t/a" => vec![file, value],
            "t/a/b" => vec![file],
            _ => vec![value],
        };
        assert_eq!(under(root), expected, "under {path}");
    }
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    // A consumer deep in the tree acknowledges the file's entry, and one
    // beside it the value's.
    let at_b = Caller::Trusted(roots[2].1);
    let (claim, entries) = h.claim(at_b, 10);
    assert_eq!(entries.len(), 1);
    h.ack(at_b, claim, &entries).expect("acknowledges");
    let at_c = Caller::Trusted(roots[4].1);
    let (claim, entries) = h.claim(at_c, 10);
    assert_eq!(entries.len(), 1);
    h.ack(at_c, claim, &entries).expect("acknowledges");
    for (path, root) in roots {
        assert_eq!(
            listed(&h.list_freed(Caller::Trusted(root), None, 10)),
            [],
            "under {path}"
        );
    }
    assert_eq!(h.list_freed(ADMIN, None, 10).entries, []);
    let stored = h.storage.dump();
    assert!(
        stored.iter().all(|(key, _)| !matches!(
            inodedb_core::keys::parse(key),
            Some(inodedb_core::keys::Parsed::FreeUnder(..))
        )),
        "nothing of the entries is left under a directory"
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A root's next claim goes on from the last key its last one examined in
/// its part of the queue, and back to the part's start once it reaches
/// the end, for what earlier claims let go.
#[test]
fn a_roots_claims_go_on_through_its_part_of_the_queue() {
    const FILES: usize = 1_100;
    let config = Config {
        claim_ttl: 1_000,
        ..Config::default()
    };
    let mut h = Harness::with(Memory::new(), config);
    h.ops([mkdir("t"), mkdir("u")]);
    h.ops((0..FILES).map(|n| put(&format!("t/f{n:04}"), "v")));
    h.ops((0..FILES).map(|n| put(&format!("u/f{n:04}"), "v")));
    let files: Vec<Ino> = (0..FILES)
        .map(|n| ino(&mut h, &format!("t/f{n:04}")))
        .collect();
    h.ops((0..FILES).map(|n| unlink(&format!("t/f{n:04}"))));
    h.ops((0..FILES).map(|n| unlink(&format!("u/f{n:04}"))));
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    let inodes = |entries: &[inodedb_core::lease::FreeEntry]| -> Vec<Ino> {
        entries.iter().map(ino_of_freed).collect()
    };
    // A claim takes the part's first entries: a page of keys, then the
    // records of the two it names.
    let before = h.reads;
    let (_, first) = h.claim(at_t, 2);
    assert_eq!(inodes(&first), files[..2]);
    assert_eq!(h.reads - before, 2);
    let (_, second) = h.claim(at_t, 2);
    assert_eq!(inodes(&second), files[2..4]);
    // Both claims run out. The next goes on from the cursor all the same,
    // passing nothing between.
    h.tick(1_000);
    let (_, third) = h.claim(at_t, 10);
    assert_eq!(inodes(&third), files[4..14]);
    // A claim for everything reads on through the part's pages to its
    // end, then goes back to the start for what the first two let go.
    let (_, fourth) = h.claim(at_t, FILES);
    assert_eq!(fourth.len(), FILES - 10);
    assert_eq!(inodes(&fourth)[..FILES - 14], files[14..]);
    assert_eq!(inodes(&fourth)[FILES - 14..], files[..4]);
    assert_eq!(h.claim(at_t, 10).1, []);
}

/// A claim for more than a page of its root's part holds reads on through
/// the part.
#[test]
fn a_claim_reads_on_through_its_part_until_it_has_its_fill() {
    const FILES: usize = 1_100;
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    h.ops((0..FILES).map(|n| put(&format!("t/f{n:04}"), "v")));
    h.ops((0..FILES).map(|n| unlink(&format!("t/f{n:04}"))));
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    let before = h.reads;
    let (_, mine) = h.claim(at_t, 1_050);
    assert_eq!(mine.len(), 1_050);
    assert_eq!(
        h.reads - before,
        4,
        "two pages of the part, and the records each named"
    );
}

/// Runs the writer until it asks for the records of the entries a page of
/// a root's part of the queue named, and returns that read unanswered.
fn until_it_reads_records(h: &mut Harness) -> (u64, Vec<KvRead>) {
    loop {
        let actions = h.writer.drain();
        assert!(!actions.is_empty(), "the writer reads the records");
        for action in actions {
            match action {
                Action::Read { id, reads } if matches!(reads.get(1), Some(KvRead::Get(_))) => {
                    return (id, reads);
                }
                Action::Read { id, reads } => {
                    let results = h.storage.read_now(&reads);
                    h.writer.read_done(id, results);
                }
                other => panic!("the writer asked for {other:?}"),
            }
        }
    }
}

/// A claim and a listing under a directory read their entries' records
/// after the keys that name them. An entry acknowledged in between is gone,
/// and stays out of both.
#[test]
fn an_entry_acknowledged_as_a_claim_or_a_listing_reads_stays_out_of_it() {
    let config = Config {
        claim_ttl: 1_000,
        ..Config::default()
    };
    let mut h = Harness::with(Memory::new(), config);
    h.ops([mkdir("t"), mkdir("u")]);
    h.ops((0..4).map(|n| put(&format!("t/f{n}"), "v")));
    let files: Vec<Ino> = (0..4).map(|n| ino(&mut h, &format!("t/f{n}"))).collect();
    h.ops((0..4).map(|n| unlink(&format!("t/f{n}"))));
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    // Storage holds the release horizon before the scripted claims.
    let at_u = Caller::Trusted(ino(&mut h, "u"));
    assert_eq!(h.claim(at_u, 1).1, []);
    for listing in [false, true] {
        let token = h.token();
        if listing {
            h.writer.list_freed(token, at_t, None, 10);
        } else {
            h.writer.claim(token, at_t, 10);
        }
        let (id, reads) = until_it_reads_records(&mut h);
        // The root's consumer claims the queue's first entry and
        // acknowledges it.
        let (claim, first) = h.claim(ADMIN, 1);
        assert_eq!(first.len(), 1);
        let gone = ino_of_freed(&first[0]);
        h.ack(ADMIN, claim, &first).expect("acknowledges");
        let results = h.storage.read_now(&reads);
        h.writer.read_done(id, results);
        h.run();
        let entries = match h.answer(token) {
            Answer::Claimed { entries, .. } | Answer::FreeQueue { entries, .. } => entries,
            other => panic!("{other:?}"),
        };
        let left: Vec<Ino> = entries.iter().map(ino_of_freed).collect();
        assert!(!left.contains(&gone), "{gone} in {left:?}");
        assert_eq!(left.len(), if listing { 2 } else { 3 });
        assert!(left.iter().all(|file| files.contains(file)));
        // The claim's hold on the rest runs out before the listing's turn.
        h.tick(1_000);
    }
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A claim whose named entry is acknowledged before it reads the entry's
/// record goes on through its part of the queue for another.
#[test]
fn a_claim_goes_on_past_an_entry_acknowledged_as_it_read() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), mkdir("u")]);
    h.ops((0..3).map(|n| put(&format!("t/f{n}"), "v")));
    let files: Vec<Ino> = (0..3).map(|n| ino(&mut h, &format!("t/f{n}"))).collect();
    h.ops((0..3).map(|n| unlink(&format!("t/f{n}"))));
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    let at_u = Caller::Trusted(ino(&mut h, "u"));
    assert_eq!(h.claim(at_u, 1).1, []);
    let token = h.token();
    h.writer.claim(token, at_t, 1);
    let (id, reads) = until_it_reads_records(&mut h);
    assert_eq!(
        reads.len(),
        2,
        "the head, and the one entry the claim named"
    );
    let (claim, first) = h.claim(ADMIN, 1);
    assert_eq!(
        first.iter().map(ino_of_freed).collect::<Vec<_>>(),
        [files[0]]
    );
    h.ack(ADMIN, claim, &first).expect("acknowledges");
    let results = h.storage.read_now(&reads);
    h.writer.read_done(id, results);
    h.run();
    let Answer::Claimed { entries, .. } = h.answer(token) else {
        panic!("the claim answers")
    };
    assert_eq!(
        entries.iter().map(ino_of_freed).collect::<Vec<_>>(),
        [files[1]],
        "the next entry of its part"
    );
}

/// Strips a store back to what a writer from before the queue kept its
/// parts under directories left: no key under a directory, and no record
/// of when they began.
fn as_an_earlier_writer_left_it(storage: &Memory) {
    let mut writes = inodedb_core::keys::WriteSet::default();
    for (key, _) in storage.dump() {
        if matches!(
            inodedb_core::keys::parse(&key),
            Some(inodedb_core::keys::Parsed::FreeUnder(..))
        ) {
            writes.clear(key);
        }
    }
    writes.clear(inodedb_core::keys::free_index());
    let head = storage.head_now();
    assert_eq!(
        storage.commit_now(head.as_deref(), &writes),
        CommitOutcome::Committed
    );
}

/// A store an earlier writer filled holds entries under no directory. A
/// consumer finds them in the whole queue until they are gone, and reads
/// its root's own part from then on.
#[test]
fn a_store_from_before_the_queue_had_parts_keeps_its_entries_claimable() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), mkdir("u")]);
    h.ops([put("t/old", "1"), put("u/old", "2"), put("t/new", "3")]);
    let (old, new) = (ino(&mut h, "t/old"), ino(&mut h, "t/new"));
    h.ops([unlink("t/old"), unlink("u/old")]);
    as_an_earlier_writer_left_it(&h.storage);
    let config = Config {
        claim_ttl: 1_000,
        ..Config::default()
    };
    let mut h = Harness::with(h.storage.clone(), config);
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    assert!(!h.writer.queue_indexed(), "older entries remain");
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    // What this writer frees lies under its directories at once.
    h.ops([unlink("t/new")]);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    // A listing and a claim from the queue's start see the older entries
    // and go on reading the whole queue.
    assert_eq!(
        listed(&h.list_freed(at_t, None, 10)),
        [Freed::Inode(old), Freed::Inode(new)]
    );
    // A page from past the older entries says nothing of them.
    let all = h.list_freed(ADMIN, None, 10);
    assert_eq!(all.entries.len(), 3);
    let rest = h.list_freed(ADMIN, Some(all.entries[1].key()), 10);
    assert_eq!(listed(&rest), [Freed::Inode(new)]);
    assert!(!h.writer.queue_indexed());
    let (claim, mine) = h.claim(at_t, 1);
    assert_eq!(mine.iter().map(ino_of_freed).collect::<Vec<_>>(), [old]);
    assert!(!h.writer.queue_indexed());
    h.ack(at_t, claim, &mine).expect("acknowledges");
    // The root's consumer takes the other older entry. The next read from
    // the queue's start finds none left.
    let (claim, theirs) = h.claim(ADMIN, 1);
    assert_eq!(theirs.len(), 1);
    h.ack(ADMIN, claim, &theirs).expect("acknowledges");
    assert!(
        !h.writer.queue_indexed(),
        "no read has seen the queue since"
    );
    assert_eq!(listed(&h.list_freed(ADMIN, None, 10)), [Freed::Inode(new)]);
    assert!(h.writer.queue_indexed());
    // A claim under t reads t's part from its start: t's cursor names a
    // key of the whole queue, which says nothing of that part.
    let before = h.reads;
    let (_, mine) = h.claim(at_t, 1);
    assert_eq!(mine.iter().map(ino_of_freed).collect::<Vec<_>>(), [new]);
    assert_eq!(h.reads - before, 2);
    // A writer that starts over the store finds the record and the queue
    // whole.
    let h = Harness::with(h.storage.clone(), Config::default());
    assert!(h.writer.queue_indexed());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_claim_from_the_queues_start_shows_a_store_from_before_holds_nothing_older() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), put("t/old", "1"), put("t/new", "2")]);
    let new = ino(&mut h, "t/new");
    h.ops([unlink("t/old")]);
    as_an_earlier_writer_left_it(&h.storage);
    let config = Config {
        claim_ttl: 1_000,
        ..Config::default()
    };
    let mut h = Harness::with(h.storage.clone(), config);
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    h.ops([unlink("t/new")]);
    // The root's consumer claims both entries and acknowledges the older.
    let (claim, both) = h.claim(ADMIN, 10);
    assert_eq!(both.len(), 2);
    h.ack(ADMIN, claim, &both[..1]).expect("acknowledges");
    h.tick(1_000);
    assert!(!h.writer.queue_indexed());
    let (_, mine) = h.claim(at_t, 10);
    assert_eq!(mine.iter().map(ino_of_freed).collect::<Vec<_>>(), [new]);
    assert!(h.writer.queue_indexed());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn an_empty_listing_shows_a_store_from_before_holds_nothing_older() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), put("t/f", "v")]);
    h.ops([unlink("t/f")]);
    as_an_earlier_writer_left_it(&h.storage);
    let mut h = Harness::with(h.storage.clone(), Config::default());
    assert!(!h.writer.queue_indexed());
    let (claim, entries) = h.claim(ADMIN, 10);
    h.ack(ADMIN, claim, &entries).expect("acknowledges");
    assert!(!h.writer.queue_indexed());
    assert_eq!(h.list_freed(ADMIN, None, 10).entries, []);
    assert!(h.writer.queue_indexed());
}

#[test]
fn a_store_whose_queue_is_empty_has_nothing_older() {
    let mut h = Harness::new();
    h.ops([mkdir("t"), put("t/f", "v")]);
    as_an_earlier_writer_left_it(&h.storage);
    let mut h = Harness::with(h.storage.clone(), Config::default());
    assert!(h.writer.queue_indexed());
    let at_t = Caller::Trusted(ino(&mut h, "t"));
    h.ops([unlink("t/f")]);
    assert_eq!(h.claim(at_t, 10).1.len(), 1);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}
