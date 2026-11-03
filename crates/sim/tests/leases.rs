//! Scripted scenarios for leases, release, claims and acknowledgements.

use inodedb_core::auth::{Caller, Grant};
use inodedb_core::delta::Freed;
use inodedb_core::lease::{ClaimError, LeaseError};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Error, Ino, ROOT};
use inodedb_core::writer::{Action, CommitOutcome, Config, Writer};
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
    let at_t = h.mint(ADMIN, 1, "t", Grant::Write, 0);
    let reader = h.mint(ADMIN, 2, "t", Grant::Read, 0);
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
    let at_a = h.mint(ADMIN, 1, "a", Grant::Write, 0);
    let at_b = h.mint(ADMIN, 2, "b", Grant::Write, 0);
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
