//! Scripted scenarios for keys: roots, grants, minting, revocation,
//! expiry, and reads by handle.

use inodedb_core::Bytes;
use inodedb_core::auth::{Caller, Denied, Grant, KeyError, KeyId, KeyOp, KeyRecord};
use inodedb_core::exec::{self, Input};
use inodedb_core::ops::{Batch, CommitResult, Outcome, RenameFlag, Skip};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Entry, Error, Ino, Inode, Name, ROOT, Version};
use inodedb_core::view::{Lookup, View};
use inodedb_core::writer::Answer;
use inodedb_sim::harness::*;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::ops::Bound;

const ADMIN: Caller = Caller::Trusted(ROOT);

fn committed(answer: Answer) -> CommitResult {
    match answer {
        Answer::Commit(result) => result,
        other => panic!("expected a commit, got {other:?}"),
    }
}

fn stat_as(h: &mut Harness, caller: Caller, target: Target) -> Answer {
    h.query_as(caller, Query::Stat(target))
}

fn ino(h: &mut Harness, path: &str) -> Ino {
    match h.stat(path) {
        QueryResult::Stat { ino, .. } => ino,
        other => panic!("{path}: {other:?}"),
    }
}

fn found(answer: &Answer) -> bool {
    matches!(
        answer,
        Answer::Query {
            result: QueryResult::Stat { .. },
            ..
        }
    )
}

fn denied(answer: &Answer) -> Option<Denied> {
    match answer {
        Answer::Denied(denied) => Some(*denied),
        _ => None,
    }
}

#[test]
fn a_key_resolves_paths_from_its_root() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a"), put("t/a/f", "x"), put("g", "y")]);
    let key = h.mint(ADMIN, "t", Grant::Write, 0);
    assert!(found(&stat_as(&mut h, key, Target::Path(p("a/f")))));
    assert!(!found(&stat_as(&mut h, key, Target::Path(p("g")))));
    let result = committed(h.ops_as(key, [put("a/h", "z"), mkdir("b")]));
    assert!(result.outcomes.iter().all(Outcome::applied));
    assert!(matches!(h.stat("t/a/h"), QueryResult::Stat { .. }));
    // `Link` and `Rename` resolve both paths from the key's root.
    let result = committed(h.ops_as(key, [link("a/f", "f2"), mv("a/h", "b/h")]));
    assert!(result.outcomes.iter().all(Outcome::applied));
    assert!(matches!(h.stat("t/f2"), QueryResult::Stat { .. }));
    assert!(matches!(h.stat("t/b/h"), QueryResult::Stat { .. }));
    // The key's root itself takes no ops.
    let result = committed(h.ops_as(key, [rmdir("")]));
    assert_eq!(
        result.outcomes[0],
        Outcome::Skipped(Skip::Error(Error::Invalid))
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn grants_bound_what_a_key_does() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let reader = h.mint(ADMIN, "t", Grant::Read, 0);
    let writer = h.mint(ADMIN, "t", Grant::Write, 0);
    assert!(found(&stat_as(&mut h, reader, Target::Path(p("")))));
    assert_eq!(denied(&h.ops_as(reader, [mkdir("a")])), Some(Denied::Grant));
    assert!(matches!(h.ops_as(writer, [mkdir("a")]), Answer::Commit(_)));
    let mint = KeyOp::Mint {
        id: None,
        hash: key_hash(3),
        root: p("a"),
        grant: Grant::Read,
        ttl: 0,
    };
    assert_eq!(denied(&h.key_op(writer, mint)), Some(Denied::Grant));
    let impostor = Caller::Key {
        id: key_id(writer),
        hash: key_hash(9),
    };
    assert_eq!(
        denied(&stat_as(&mut h, impostor, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
}

#[test]
fn minted_keys_stay_within_their_minter() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a/b"), put("t/f", "x")]);
    let admin = h.mint(ADMIN, "t", Grant::Admin, 10_000);
    let mint = |root: &str, grant, ttl| KeyOp::Mint {
        id: None,
        hash: key_hash(2),
        root: p(root),
        grant,
        ttl,
    };
    assert_eq!(
        h.key_op(admin, mint("f", Grant::Read, 0)),
        Answer::Key(Err(KeyError::Fs(Error::NotDir)))
    );
    assert_eq!(
        h.key_op(admin, mint("x", Grant::Read, 0)),
        Answer::Key(Err(KeyError::Fs(Error::NotFound)))
    );
    let nested = h.mint(admin, "a", Grant::Admin, 0);
    let deeper = h.mint(nested, "b", Grant::Read, 50_000);
    assert!(found(&stat_as(&mut h, deeper, Target::Path(p("")))));
    assert_eq!(
        h.key_op(deeper, mint("", Grant::Read, 0)),
        Answer::Denied(Denied::Grant)
    );
    // Neither key outlives its minter.
    let keys = h.writer.keys();
    let expires = |caller| keys.get(key_id(caller)).expect("minted").expires;
    assert_eq!(
        (expires(admin), expires(nested), expires(deeper)),
        (11_000, 11_000, 11_000)
    );
    // An expired key is refused at once, and the writer's next deletion
    // takes it with the keys it minted.
    h.tick(9_999);
    h.now += 1;
    h.writer.set_now(h.now);
    assert_eq!(
        denied(&stat_as(&mut h, deeper, Target::Path(p("")))),
        Some(Denied::Expired)
    );
    h.tick(0);
    assert_eq!(
        denied(&stat_as(&mut h, deeper, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert!(h.writer.keys().is_empty());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_key_follows_its_root_through_renames() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a"), put("t/a/f", "x"), mkdir("u")]);
    let key = h.mint(ADMIN, "t/a", Grant::Write, 0);
    h.ops([mv("t/a", "u/b")]);
    assert!(found(&stat_as(&mut h, key, Target::Path(p("f")))));
    committed(h.ops_as(key, [put("g", "y")]));
    assert!(matches!(h.stat("u/b/g"), QueryResult::Stat { .. }));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn freeing_or_detaching_a_root_revokes_its_keys() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a/b"), mkdir_p("u/c/d")]);
    let at_a = h.mint(ADMIN, "t/a", Grant::Admin, 0);
    let minted = h.mint(at_a, "b", Grant::Read, 0);
    let at_b = h.mint(ADMIN, "t/a/b", Grant::Read, 0);
    let at_d = h.mint(ADMIN, "u/c/d", Grant::Read, 0);
    let at_u = h.mint(ADMIN, "u", Grant::Read, 0);
    // Freeing b takes the keys rooted there, whoever minted them.
    h.ops([rmdir("t/a/b")]);
    assert_eq!(
        denied(&stat_as(&mut h, minted, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert_eq!(
        denied(&stat_as(&mut h, at_b, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert!(found(&stat_as(&mut h, at_a, Target::Path(p("")))));
    // Detaching c takes every key rooted beneath it.
    h.ops([remove_all("u/c")]);
    assert_eq!(
        denied(&stat_as(&mut h, at_d, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert!(found(&stat_as(&mut h, at_u, Target::Path(p("")))));
    // Renaming onto an empty directory frees it.
    h.ops([mkdir("t/x"), mkdir("t/y")]);
    let at_y = h.mint(ADMIN, "t/y", Grant::Read, 0);
    h.ops([mv("t/x", "t/y")]);
    assert_eq!(
        denied(&stat_as(&mut h, at_y, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_batch_that_detaches_several_directories_revokes_the_keys_beneath_each() {
    let mut h = Harness::new();
    h.ops([
        mkdir_p("t/a/x/m/deep"),
        mkdir_p("t/a/y"),
        mkdir_p("t/b/x"),
        mkdir_p("t/c/x"),
        mkdir_p("u/d"),
    ]);
    let mut at = |root: &str| h.mint(ADMIN, root, Grant::Read, 0);
    // Beneath the first directory the batch detaches: a root, a root under
    // that one, which two keys share, and the directory itself.
    let gone = [
        at("t/a/x/m"),
        at("t/a/x/m/deep"),
        at("t/a/x/m/deep"),
        at("t/a/x"),
        // Beneath the second and the third.
        at("t/b/x"),
        at("u/d"),
    ];
    // Above a detached directory, beside one, and under a directory the
    // search has already passed on its way up from another root.
    let stay = [at("t/a"), at("t/a/y"), at("t/c/x"), at("t")];
    let result = h.ops([remove_all("t/a/x"), remove_all("t/b"), remove_all("u")]);
    assert!(result.outcomes.iter().all(Outcome::applied));
    // The detaching commit revokes them all itself, ahead of the freeing
    // that would reach each root in time.
    assert_eq!(h.key_deletions(), vec![(result.version, gone.len())]);
    for key in gone {
        assert_eq!(
            denied(&stat_as(&mut h, key, Target::Path(p("")))),
            Some(Denied::Unknown)
        );
    }
    for key in stay {
        assert!(found(&stat_as(&mut h, key, Target::Path(p("")))));
    }
    assert_eq!(h.writer.keys().len(), stay.len());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_detach_revokes_a_key_minted_in_its_own_group() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a/b")]);
    // Both arrive before either commits, so the detach runs against a key
    // only the writer's uncommitted changes hold.
    let (mint, detach) = (h.token(), h.token());
    h.writer.submit_key(
        mint,
        ADMIN,
        KeyOp::Mint {
            id: None,
            hash: key_hash(5),
            root: p("t/a/b"),
            grant: Grant::Read,
            ttl: 0,
        },
    );
    h.writer.submit(
        detach,
        ADMIN,
        inodedb_core::ops::Batch {
            ops: vec![remove_all("t/a")],
            ..Default::default()
        },
    );
    h.run();
    let Answer::Key(Ok(id)) = h.answer(mint) else {
        panic!("the mint commits");
    };
    let detached = committed(h.answer(detach));
    assert!(detached.committed);
    assert_eq!(h.key_deletions(), vec![(detached.version, 1)]);
    let minted = Caller::Key {
        id,
        hash: key_hash(5),
    };
    assert_eq!(
        denied(&stat_as(&mut h, minted, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert!(h.writer.keys().is_empty());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A view that counts the inode records asked of it, and can hide the keys.
struct Counting<'a> {
    base: &'a dyn View,
    keyless: bool,
    inodes: Cell<usize>,
}

impl View for Counting<'_> {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>> {
        self.inodes.set(self.inodes.get() + 1);
        self.base.inode(ino)
    }
    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        self.base.entry(dir, name)
    }
    fn has_entry_outside(&self, dir: Ino, removed: &BTreeSet<Name>) -> Lookup<bool> {
        self.base.has_entry_outside(dir, removed)
    }
    fn backptrs(&self, ino: Ino) -> Lookup<BTreeSet<(Ino, Name)>> {
        self.base.backptrs(ino)
    }
    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        self.base.value(ino, version)
    }
    fn request(&self, id: u128) -> Lookup<Option<CommitResult>> {
        self.base.request(id)
    }
    fn listing(&self, dir: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>> {
        self.base.listing(dir, start, limit)
    }
    fn free_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.base.free_entry(version, ino)
    }
    fn detach_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.base.detach_entry(version, ino)
    }
    fn key(&self, id: KeyId) -> Option<KeyRecord> {
        self.base.key(id)
    }
    fn key_retired(&self, id: KeyId) -> bool {
        self.base.key_retired(id)
    }
    fn keys_rooted_at(&self, dir: Ino) -> Vec<KeyId> {
        self.base.keys_rooted_at(dir)
    }
    fn keys_minted_by(&self, id: KeyId) -> Vec<KeyId> {
        self.base.keys_minted_by(id)
    }
    fn key_roots(&self) -> BTreeSet<Ino> {
        if self.keyless {
            BTreeSet::new()
        } else {
            self.base.key_roots()
        }
    }
    fn covers(&self, dir: Ino) -> Option<bool> {
        self.base.covers(dir)
    }
}

#[test]
fn a_detach_looks_up_each_key_root_once_however_many_directories_go() {
    const TENANTS: usize = 200;
    const PARTS: usize = 50;
    let mut h = Harness::new();
    let tenant = |n: usize| format!("a/b/c/d/t{n}");
    h.ops((0..TENANTS).map(|n| mkdir_p(&tenant(n))));
    h.ops((0..PARTS).map(|n| mkdir_p(&format!("{}/parts/p{n}", tenant(0)))));
    for n in 0..TENANTS {
        h.mint(ADMIN, &tenant(n), Grant::Write, 0);
        h.mint(ADMIN, &tenant(n), Grant::Read, 0);
    }
    // One batch detaches every part of one tenant.
    let batch = Batch {
        ops: (0..PARTS)
            .map(|n| remove_all(&format!("{}/parts/p{n}", tenant(0))))
            .collect(),
        ..Batch::default()
    };
    let lookups = |keyless: bool| {
        let view = Counting {
            base: h.writer.cache(),
            keyless,
            inodes: Cell::new(0),
        };
        let input = Input {
            batch: &batch,
            root: ROOT,
            now: h.now,
            version: h.writer.version() + 1,
            next_ino: h.writer.head().next_ino,
            ahead: false,
        };
        let executed = exec::execute(input, &view).expect("the cache holds the tree");
        assert_eq!(executed.delta.detached.len(), PARTS);
        assert!(executed.delta.keys.is_empty(), "no key lies beneath a part");
        view.inodes.get()
    };
    // The search for keys beneath the parts reads each tenant's record
    // once, and the four directories above the tenants once among them.
    let searched = lookups(false) - lookups(true);
    assert!(
        searched <= TENANTS + 4,
        "{searched} inode lookups for {TENANTS} key roots"
    );
}

#[test]
fn a_detach_reads_the_key_roots_a_cold_cache_lacks_in_rounds() {
    const ROOTS: usize = 300;
    let mut h = Harness::new();
    h.ops((0..ROOTS).map(|n| mkdir_p(&format!("a/b/t{n}"))));
    h.ops([mkdir_p("parts/p")]);
    for n in 0..ROOTS {
        h.mint(ADMIN, &format!("a/b/t{n}"), Grant::Read, 0);
    }
    // A restarted writer holds every key and none of the tree.
    let mut h = Harness::with(h.storage.clone(), Default::default());
    let started = h.reads;
    let result = h.ops([remove_all("parts/p")]);
    assert!(result.outcomes.iter().all(Outcome::applied));
    // The search reads every root's record in one round, then each level
    // above them in a round of its own.
    let rounds = h.reads - started;
    assert!(
        rounds <= 12,
        "{rounds} rounds of reads for {ROOTS} key roots"
    );
    assert_eq!(h.writer.keys().len(), ROOTS);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// Tenants `a/tN`, each a key's root, under a writer whose first detach
/// has searched for keys and left it the directories at and above the
/// roots.
fn covered(tenants: usize) -> Harness {
    let mut h = Harness::new();
    h.ops((0..tenants).map(|n| mkdir_p(&format!("a/t{n}"))));
    h.ops([mkdir("scrap")]);
    for n in 0..tenants {
        h.mint(ADMIN, &format!("a/t{n}"), Grant::Read, 0);
    }
    assert_eq!(h.writer.covered(), None);
    h.ops([remove_all("scrap")]);
    // The tenants, `a` and the tree's root.
    assert_eq!(h.writer.covered(), Some(tenants + 2));
    h
}

fn gone(h: &mut Harness, key: Caller) -> bool {
    denied(&stat_as(h, key, Target::Path(p("")))) == Some(Denied::Unknown)
}

/// Once a search has found the directories at and above the key roots, a
/// detach of directories outside them looks at no root.
#[test]
fn a_detach_outside_every_key_root_searches_for_no_keys() {
    const TENANTS: usize = 200;
    const PARTS: usize = 50;
    let mut h = covered(TENANTS);
    h.ops((0..PARTS).map(|n| mkdir_p(&format!("a/t0/parts/p{n}"))));
    let batch = Batch {
        ops: (0..PARTS)
            .map(|n| remove_all(&format!("a/t0/parts/p{n}")))
            .collect(),
        ..Batch::default()
    };
    let lookups = |keyless: bool| {
        let view = Counting {
            base: h.writer.cache(),
            keyless,
            inodes: Cell::new(0),
        };
        let input = Input {
            batch: &batch,
            root: ROOT,
            now: h.now,
            version: h.writer.version() + 1,
            next_ino: h.writer.head().next_ino,
            ahead: false,
        };
        let executed = exec::execute(input, &view).expect("the cache holds the tree");
        assert_eq!(executed.delta.detached.len(), PARTS);
        assert!(executed.delta.keys.is_empty(), "no key lies beneath a part");
        view.inodes.get()
    };
    assert_eq!(lookups(false), lookups(true));
    // A directory beneath a root lies outside them too, and the commit
    // leaves the keys and the cover as they were.
    let result = h.commit(batch);
    assert!(result.outcomes.iter().all(Outcome::applied));
    assert!(h.key_deletions().is_empty());
    assert_eq!(h.writer.keys().len(), TENANTS);
    assert_eq!(h.writer.covered(), Some(TENANTS + 2));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A key minted after the search brings its root, and the directories
/// above it, among those a detach searches under.
#[test]
fn a_detach_takes_a_key_minted_since_the_search() {
    let mut h = covered(3);
    h.ops([mkdir_p("b/c/d/e"), mkdir_p("a/t0/x/y")]);
    let deep = h.mint(ADMIN, "b/c/d/e", Grant::Read, 0);
    // `e` and the three directories above it.
    assert_eq!(h.writer.covered(), Some(5 + 4));
    // A key minted from a key: its root and the one directory between it
    // and its minter's.
    let at_t0 = h.mint(ADMIN, "a/t0", Grant::Admin, 0);
    let inner = h.mint(at_t0, "x/y", Grant::Read, 0);
    assert_eq!(h.writer.covered(), Some(9 + 2));
    let result = h.ops([remove_all("b/c"), remove_all("a/t0/x")]);
    assert!(result.outcomes.iter().all(Outcome::applied));
    assert_eq!(h.key_deletions(), vec![(result.version, 2)]);
    assert!(gone(&mut h, deep));
    assert!(gone(&mut h, inner));
    assert!(found(&stat_as(&mut h, at_t0, Target::Path(p("")))));
    // The freeing takes what the batch detached out of the cover. `b`
    // stays, though it lies above no root now.
    assert_eq!(h.writer.covered(), Some(5 + 1));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A key's root, or a directory above one, that moves under other
/// directories brings them above the root.
#[test]
fn a_detach_takes_a_key_whose_root_moved_beneath_it() {
    for exchange in [None, Some(false), Some(true)] {
        let mut h = covered(3);
        h.ops([mkdir_p("b/c/x"), mkdir_p("d/e/z")]);
        let keys: Vec<Caller> = (0..3)
            .map(|n| h.mint(ADMIN, &format!("a/t{n}"), Grant::Read, 0))
            .collect();
        // A directory with no root at or beneath it covers nothing where
        // it lands.
        h.ops([mv("d/e/z", "b/z")]);
        assert_eq!(h.writer.covered(), Some(5));
        match exchange {
            // A root moves.
            None => h.ops([mv("a/t0", "b/c/t0")]),
            // A root and a directory outside the cover change places,
            // named either way round.
            Some(false) => h.ops([rename("a/t0", "b/c/x", RenameFlag::Exchange)]),
            Some(true) => h.ops([rename("b/c/x", "a/t0", RenameFlag::Exchange)]),
        };
        // `c` and `b` now lie above a root.
        assert_eq!(h.writer.covered(), Some(5 + 2), "{exchange:?}");
        let result = h.ops([remove_all("b")]);
        assert_eq!(h.key_deletions(), vec![(result.version, 2)], "{exchange:?}");
        assert!(gone(&mut h, keys[0]));
        assert!(found(&stat_as(&mut h, keys[1], Target::Path(p("")))));
        // A directory above the other roots moves, and takes them with it.
        h.ops([mv("a", "d/e/a")]);
        let result = h.ops([remove_all("d")]);
        assert_eq!(h.key_deletions().last(), Some(&(result.version, 4)));
        assert!(gone(&mut h, keys[1]));
        assert!(gone(&mut h, keys[2]));
        assert!(h.writer.keys().is_empty());
        assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    }
}

/// A batch that moves a key's root under a directory and then detaches the
/// directory revokes the key.
#[test]
fn a_batch_that_moves_a_root_and_detaches_what_it_moved_under_revokes_the_key() {
    let mut h = covered(3);
    h.ops([mkdir_p("b/c")]);
    let at_t0 = h.mint(ADMIN, "a/t0", Grant::Read, 0);
    let result = h.ops([mv("a/t0", "b/c/t0"), remove_all("b")]);
    assert!(result.outcomes.iter().all(Outcome::applied));
    assert_eq!(h.key_deletions(), vec![(result.version, 2)]);
    assert!(gone(&mut h, at_t0));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A detach runs against what batches not yet committed brought above a
/// key's root: a mint, and a move.
#[test]
fn a_detach_takes_a_key_an_uncommitted_batch_brought_beneath_it() {
    for moved in [false, true] {
        let mut h = covered(3);
        h.ops([mkdir_p("b/c/d")]);
        let at_t0 = h.mint(ADMIN, "a/t0", Grant::Read, 0);
        let before = h.key_deletions().len();
        // Both arrive before either commits.
        let (first, detach) = (h.token(), h.token());
        if moved {
            h.writer.submit(
                first,
                ADMIN,
                Batch {
                    ops: vec![mv("a/t0", "b/c/d/t0")],
                    ..Batch::default()
                },
            );
        } else {
            h.writer.submit_key(
                first,
                ADMIN,
                KeyOp::Mint {
                    id: None,
                    hash: key_hash(5),
                    root: p("b/c/d"),
                    grant: Grant::Read,
                    ttl: 0,
                },
            );
        }
        h.writer.submit(
            detach,
            ADMIN,
            Batch {
                ops: vec![remove_all("b")],
                ..Batch::default()
            },
        );
        h.run();
        let detached = committed(h.answer(detach));
        assert!(detached.committed);
        let expected = if moved { 2 } else { 1 };
        assert_eq!(
            h.key_deletions()[before..],
            [(detached.version, expected)],
            "moved: {moved}"
        );
        assert_eq!(gone(&mut h, at_t0), moved);
        assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    }
}

/// A key minted while the first search has yet to commit joins the cover
/// that search leaves.
#[test]
fn a_key_minted_beside_the_first_search_joins_its_cover() {
    let mut h = Harness::new();
    h.ops([mkdir_p("a/t0"), mkdir("scrap"), mkdir_p("b/c/d")]);
    h.mint(ADMIN, "a/t0", Grant::Read, 0);
    let (search, mint) = (h.token(), h.token());
    h.writer.submit(
        search,
        ADMIN,
        Batch {
            ops: vec![remove_all("scrap")],
            ..Batch::default()
        },
    );
    h.writer.submit_key(
        mint,
        ADMIN,
        KeyOp::Mint {
            id: None,
            hash: key_hash(5),
            root: p("b/c/d"),
            grant: Grant::Read,
            ttl: 0,
        },
    );
    h.run();
    let Answer::Key(Ok(id)) = h.answer(mint) else {
        panic!("the mint commits");
    };
    // `t0`, `a` and the root from the search; `d`, `c` and `b` from the
    // mint.
    assert_eq!(h.writer.covered(), Some(3 + 3));
    let result = h.ops([remove_all("b/c")]);
    assert_eq!(h.key_deletions(), vec![(result.version, 1)]);
    let minted = Caller::Key {
        id,
        hash: key_hash(5),
    };
    assert!(gone(&mut h, minted));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A directory freed while empty leaves the cover, and a restarted writer
/// knows none until its first detach searches again.
#[test]
fn the_cover_drops_freed_directories_and_starts_over_with_its_writer() {
    let mut h = covered(3);
    h.ops([rmdir("a/t0")]);
    assert_eq!(h.writer.keys().len(), 2);
    assert_eq!(h.writer.covered(), Some(4));
    let mut h = Harness::with(h.storage.clone(), Default::default());
    assert_eq!(h.writer.covered(), None);
    let result = h.ops([remove_all("a")]);
    assert_eq!(h.key_deletions().last(), Some(&(result.version, 2)));
    assert!(h.writer.keys().is_empty());
    // The root alone, which no key is rooted at now.
    assert_eq!(h.writer.covered(), Some(1));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A batch run ahead of its turn, for the reads it lacks, leaves out the
/// search for keys beneath what it detaches: the search would run again
/// each round the batch before it waits. Its rounds of reads then follow
/// that batch's.
#[test]
fn a_batch_run_ahead_leaves_out_its_search_for_keys() {
    const ROOTS: usize = 300;
    let mut h = Harness::new();
    h.ops((0..ROOTS).map(|n| mkdir_p(&format!("a/b/t{n}"))));
    h.ops([mkdir_p("parts/p"), mkdir_p("cold/dir")]);
    for n in 0..ROOTS {
        h.mint(ADMIN, &format!("a/b/t{n}"), Grant::Read, 0);
    }
    let storage = h.storage.copy_now();
    let write = Batch {
        ops: vec![put("cold/dir/f", "v")],
        ..Batch::default()
    };
    let detach = Batch {
        ops: vec![remove_all("parts/p")],
        ..Batch::default()
    };
    // The rounds of reads a restarted writer takes to commit batches that
    // arrive together. It holds every key and none of the tree, and first
    // reads what the detach itself needs.
    let rounds = |batches: &[&Batch]| {
        let mut h = Harness::with(storage.copy_now(), Default::default());
        for path in ["", "parts", "parts/p"] {
            assert!(matches!(h.stat(path), QueryResult::Stat { .. }));
        }
        // In its turn the detach lacks the roots' records alone.
        let input = Input {
            batch: &detach,
            root: ROOT,
            now: h.now,
            version: h.writer.version() + 1,
            next_ino: h.writer.head().next_ino,
            ahead: false,
        };
        let Err(lacked) = exec::execute(input, h.writer.cache()) else {
            panic!("the search lacks the roots' records");
        };
        assert_eq!(lacked.len(), ROOTS, "{lacked:?}");
        let started = h.reads;
        let tokens: Vec<u64> = batches
            .iter()
            .map(|batch| {
                let token = h.token();
                h.writer.submit(token, ADMIN, (*batch).clone());
                token
            })
            .collect();
        h.run();
        for token in tokens {
            assert!(committed(h.answer(token)).committed);
        }
        assert_eq!(h.writer.keys().len(), ROOTS);
        assert!(h.fsck().is_empty(), "{:?}", h.fsck());
        (h.reads - started, h.writer.read_ahead_ops())
    };
    let (write_alone, _) = rounds(&[&write]);
    let (detach_alone, _) = rounds(&[&detach]);
    // The write reads a level of its path a round. The search reads the
    // roots, then each of the two levels above them, and the freeing that
    // follows reads the directory it frees.
    assert!(write_alone >= 2, "{write_alone} rounds");
    assert_eq!(detach_alone, 4);
    let (together, ahead) = rounds(&[&write, &detach]);
    assert!(ahead > 0, "the detach runs ahead");
    assert_eq!(together, write_alone + detach_alone);
}

#[test]
fn revoking_a_key_revokes_what_it_minted() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a/b")]);
    let top = h.mint(ADMIN, "t", Grant::Admin, 0);
    let middle = h.mint(top, "a", Grant::Admin, 0);
    let bottom = h.mint(middle, "b", Grant::Admin, 0);
    let sibling = h.mint(top, "a", Grant::Read, 0);
    // A key revokes only itself and what came from it.
    let revoke = |caller| KeyOp::Revoke { id: key_id(caller) };
    assert_eq!(
        h.key_op(bottom, revoke(middle)),
        Answer::Key(Err(KeyError::NotFound))
    );
    assert_eq!(
        h.key_op(middle, revoke(sibling)),
        Answer::Key(Err(KeyError::NotFound))
    );
    assert!(matches!(h.key_op(top, revoke(middle)), Answer::Key(Ok(_))));
    for gone in [middle, bottom] {
        assert_eq!(
            denied(&stat_as(&mut h, gone, Target::Path(p("")))),
            Some(Denied::Unknown)
        );
    }
    assert!(found(&stat_as(&mut h, sibling, Target::Path(p("")))));
    assert!(matches!(h.key_op(top, revoke(top)), Answer::Key(Ok(_))));
    assert!(h.writer.keys().is_empty());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_batch_queued_behind_a_revocation_is_refused() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let key = h.mint(ADMIN, "t", Grant::Write, 0);
    // Both arrive before either commits; the batch runs after the revoke.
    let (revoke, batch) = (h.token(), h.token());
    h.writer
        .submit_key(revoke, ADMIN, KeyOp::Revoke { id: key_id(key) });
    h.writer.submit(
        batch,
        key,
        inodedb_core::ops::Batch {
            ops: vec![mkdir("a")],
            ..Default::default()
        },
    );
    h.run();
    assert!(matches!(h.answer(revoke), Answer::Key(Ok(_))));
    assert_eq!(denied(&h.answer(batch)), Some(Denied::Unknown));
    assert!(matches!(
        h.stat("t/a"),
        QueryResult::Failed(Error::NotFound)
    ));
}

#[test]
fn a_read_by_handle_stays_within_the_key() {
    let mut h = Harness::new();
    h.ops([
        mkdir_p("t/a"),
        mkdir_p("u/b"),
        put("t/a/f", "x"),
        put("u/g", "y"),
    ]);
    h.ops([link("u/g", "t/a/g")]);
    let key = h.mint(ADMIN, "t", Grant::Read, 0);
    let (dir_in, file_in, dir_out, file_out) = (
        ino(&mut h, "t/a"),
        ino(&mut h, "t/a/f"),
        ino(&mut h, "u/b"),
        ino(&mut h, "u/g"),
    );
    for inside in [dir_in, file_in, file_out] {
        assert!(
            found(&stat_as(&mut h, key, Target::Ino(inside))),
            "{inside}"
        );
    }
    let outside = stat_as(&mut h, key, Target::Ino(dir_out));
    assert!(matches!(
        outside,
        Answer::Query {
            result: QueryResult::Failed(Error::NotFound),
            ..
        }
    ));
    // Unlinking the link inside the key's root puts the file out of reach.
    h.ops([unlink("t/a/g")]);
    assert!(!found(&stat_as(&mut h, key, Target::Ino(file_out))));
    // Moving a directory out takes it out of reach, and back in returns it.
    h.ops([mv("t/a", "u/a")]);
    assert!(!found(&stat_as(&mut h, key, Target::Ino(file_in))));
    h.ops([mv("u/a", "t/a")]);
    assert!(found(&stat_as(&mut h, key, Target::Ino(file_in))));
    let read = h.query_as(
        key,
        Query::Read {
            target: Target::Ino(file_out),
            version: None,
        },
    );
    assert!(matches!(
        read,
        Answer::Query {
            result: QueryResult::Failed(Error::NotFound),
            ..
        }
    ));
}

#[test]
fn keys_survive_a_restart() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let key = h.mint(ADMIN, "t", Grant::Write, 0);
    let secret = h.writer.handle_secret();
    let mut restarted = Harness::with(h.storage.clone(), Default::default());
    assert!(matches!(
        restarted.ops_as(key, [mkdir("a")]),
        Answer::Commit(_)
    ));
    assert_eq!(restarted.writer.handle_secret(), secret);
    assert!(restarted.fsck().is_empty(), "{:?}", restarted.fsck());
}

#[test]
fn a_freed_file_stays_within_the_keys_that_held_it() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/a"), mkdir("u"), put("t/a/x", "gone soon")]);
    let at_t = h.mint(ADMIN, "t", Grant::Read, 0);
    let at_a = h.mint(ADMIN, "t/a", Grant::Read, 0);
    let at_u = h.mint(ADMIN, "u", Grant::Read, 0);
    let x = ino(&mut h, "t/a/x");
    h.ops([unlink("t/a/x")]);
    for key in [at_t, at_a] {
        let read = h.query_as(
            key,
            Query::Read {
                target: Target::Ino(x),
                version: None,
            },
        );
        assert!(
            matches!(
                read,
                Answer::Query {
                    result: QueryResult::Read { .. },
                    ..
                }
            ),
            "{read:?}"
        );
    }
    assert!(!found(&stat_as(&mut h, at_u, Target::Ino(x))));
    // A directory freed inside a key's root stays within it too.
    h.ops([mkdir("t/a/d")]);
    let d = ino(&mut h, "t/a/d");
    h.ops([rmdir("t/a/d")]);
    assert!(found(&stat_as(&mut h, at_a, Target::Ino(d))));
    assert!(!found(&stat_as(&mut h, at_u, Target::Ino(d))));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_revoked_id_never_mints_again() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    h.mint_named(1, "t", Grant::Read, 0);
    assert!(matches!(
        h.key_op(ADMIN, KeyOp::Revoke { id: 1 }),
        Answer::Key(Ok(_))
    ));
    let again = KeyOp::Mint {
        id: Some(1),
        hash: key_hash(1),
        root: p("t"),
        grant: Grant::Admin,
        ttl: 0,
    };
    assert_eq!(
        h.key_op(ADMIN, again.clone()),
        Answer::Key(Err(KeyError::Exists))
    );
    let mut restarted = Harness::with(h.storage.clone(), Default::default());
    assert_eq!(
        restarted.key_op(ADMIN, again),
        Answer::Key(Err(KeyError::Exists))
    );
    assert!(restarted.fsck().is_empty(), "{:?}", restarted.fsck());
}

#[test]
fn a_request_id_answers_only_its_own_key() {
    let mut h = Harness::new();
    h.ops([mkdir("a"), mkdir("b")]);
    let at_a = h.mint(ADMIN, "a", Grant::Write, 0);
    let at_b = h.mint(ADMIN, "b", Grant::Write, 0);
    let batch = |path: &str| inodedb_core::ops::Batch {
        ops: vec![put(path, "x")],
        request_id: 77,
        ..Default::default()
    };
    let first = committed(h.submit_as(at_a, batch("f")));
    let second = committed(h.submit_as(at_b, batch("g")));
    assert_ne!(first.version, second.version, "b's batch ran");
    assert!(matches!(h.stat("b/g"), QueryResult::Stat { .. }));
    let retry = committed(h.submit_as(at_a, batch("f")));
    assert_eq!(retry, first, "a's retry gets a's answer");
}

/// `cargo run --release -p inodedb-tests --example revoke` times this at
/// scale.
#[test]
fn revoking_a_key_revokes_thousands_minted_from_it() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let top = h.mint(ADMIN, "t", Grant::Admin, 0);
    let middle = h.mint(top, "", Grant::Admin, 0);
    for n in 0..2_000 {
        let minter = if n % 2 == 0 { top } else { middle };
        h.mint(minter, "", Grant::Read, 0);
    }
    assert!(matches!(
        h.key_op(ADMIN, KeyOp::Revoke { id: key_id(top) }),
        Answer::Key(Ok(_))
    ));
    assert!(h.writer.keys().is_empty());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn a_key_minted_by_a_key_takes_its_version_as_its_id() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let operator = h.mint_named(0x5eed, "t", Grant::Admin, 0);
    assert_eq!(key_id(operator), 0x5eed);
    let mint = |id| KeyOp::Mint {
        id,
        hash: key_hash(1),
        root: p(""),
        grant: Grant::Read,
        ttl: 0,
    };
    // A key's mint takes its version, whatever id it names.
    for named in [None, Some(0x5eed)] {
        let Answer::Key(Ok(version)) = h.key_op(operator, mint(named)) else {
            panic!("mints");
        };
        let minted = Caller::Key {
            id: version,
            hash: key_hash(1),
        };
        assert!(found(&stat_as(&mut h, minted, Target::Path(p("")))));
        assert_eq!(
            h.writer.keys().get(version).map(|record| record.minted_by),
            Some(Some(0x5eed))
        );
    }
    assert_eq!(h.writer.keys().len(), 3);
}

#[test]
fn an_expired_key_leaves_nothing_behind() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let operator = h.mint(ADMIN, "t", Grant::Admin, 0);
    let (records, tombstones) = h.stored_keys();
    let short: Vec<Caller> = (0..10)
        .map(|n| h.mint(operator, "", Grant::Read, 1_000 + n))
        .collect();
    let revoked = h.mint(operator, "", Grant::Read, 0);
    assert!(matches!(
        h.key_op(
            operator,
            KeyOp::Revoke {
                id: key_id(revoked)
            }
        ),
        Answer::Key(Ok(_))
    ));
    assert_eq!(h.stored_keys(), (records + short.len(), tombstones));
    // Expired keys go once the writer's next deletion runs, a second at
    // most after they expire, and keys minted by keys leave no tombstones.
    h.tick(2_000);
    for key in &short {
        assert_eq!(
            denied(&stat_as(&mut h, *key, Target::Path(p("")))),
            Some(Denied::Unknown)
        );
    }
    assert_eq!(h.stored_keys(), (records, tombstones));
    assert_eq!(h.writer.keys().len(), 1);
    assert_eq!(h.writer.keys().tombstones(), 0);
    // The operator's own keys leave tombstones.
    let named = h.mint_named(0x5eed, "t", Grant::Read, 500);
    h.tick(1_000);
    assert_eq!(
        denied(&stat_as(&mut h, named, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert_eq!(h.stored_keys(), (records, tombstones + 1));
    let again = KeyOp::Mint {
        id: Some(0x5eed),
        hash: key_hash(1),
        root: p("t"),
        grant: Grant::Read,
        ttl: 0,
    };
    assert_eq!(h.key_op(ADMIN, again), Answer::Key(Err(KeyError::Exists)));
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn an_expired_key_takes_what_it_minted_in_one_commit() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let top = h.mint(ADMIN, "t", Grant::Admin, 1_000);
    // More than a deletion's budget, all expiring with the key that
    // minted them.
    for _ in 0..inodedb_core::writer::EXPIRE_KEYS_BUDGET + 500 {
        h.mint(top, "", Grant::Read, 0);
    }
    h.tick(1_000);
    assert_eq!(h.key_deletions().len(), 1);
    assert!(h.writer.keys().is_empty());
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

#[test]
fn expired_keys_go_in_bounded_commits_one_after_another() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let budget = inodedb_core::writer::EXPIRE_KEYS_BUDGET;
    for _ in 0..2 * budget + 500 {
        h.mint(ADMIN, "t", Grant::Read, 1_000);
    }
    // A backlog's commits follow one another without waiting.
    h.tick(1_000);
    let deleted: Vec<usize> = h.key_deletions().iter().map(|(_, count)| *count).collect();
    assert_eq!(deleted, vec![budget, budget, 500]);
    assert!(h.writer.keys().is_empty());
}

#[test]
fn a_writer_deletes_keys_that_expired_while_none_ran() {
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    let operator = h.mint(ADMIN, "t", Grant::Admin, 0);
    let short = h.mint(operator, "", Grant::Read, 5_000);
    let writer = inodedb_core::writer::Writer::new(
        Default::default(),
        Harness::writer_id(),
        h.now + 60_000,
        [7; 32],
    );
    let mut h = Harness::with_writer(h.storage.clone(), writer);
    h.now += 60_000;
    h.tick(0);
    assert_eq!(
        denied(&stat_as(&mut h, short, Target::Path(p("")))),
        Some(Denied::Unknown)
    );
    assert!(found(&stat_as(&mut h, operator, Target::Path(p("")))));
    assert_eq!(h.stored_keys().0, 1);
}
