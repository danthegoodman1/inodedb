//! Scripted scenarios for keys: roots, grants, minting, revocation,
//! expiry, and reads by handle.

use inodedb_core::auth::{Caller, Denied, Grant, KeyError, KeyOp};
use inodedb_core::ops::{CommitResult, Outcome, Skip};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Error, Ino, ROOT};
use inodedb_core::writer::Answer;
use inodedb_sim::harness::*;

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
