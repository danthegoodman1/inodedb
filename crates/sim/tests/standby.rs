//! Scripted scenarios for standbys: a follower of the log that answers
//! reads at a version, sends the rest to the writer, and takes over.

use inodedb_core::auth::{Caller, Grant};
use inodedb_core::log::LogEntry;
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::standby::Follower;
use inodedb_core::writer::{Answer, Config, Takeover, Writer};
use inodedb_sim::harness::*;

fn config() -> Config {
    Config {
        address: "writer:7070".into(),
        ..Config::default()
    }
}

/// Applies every committed entry after the follower's version.
fn follow(h: &Harness, follower: &mut Follower) {
    for (_, bytes) in h.storage.log_now(follower.version() + 1, usize::MAX) {
        follower.apply(&LogEntry::decode(&bytes).expect("decodes"));
    }
}

/// Carries out the follower's reads against storage, returning its
/// answers.
fn run(h: &Harness, follower: &mut Follower) -> Vec<(u64, Answer)> {
    let mut answers = Vec::new();
    loop {
        let actions = follower.drain();
        if actions.is_empty() {
            return answers;
        }
        for action in actions {
            match action {
                inodedb_core::writer::Action::Read { id, reads } => follower
                    .read_done(id, h.storage.read_now(&reads))
                    .expect("decodes"),
                inodedb_core::writer::Action::Answer { token, answer } => {
                    answers.push((token, answer));
                }
                other => panic!("a follower asked for {other:?}"),
            }
        }
    }
}

fn read(path: &str) -> Query {
    Query::Read {
        target: Target::Path(p(path)),
        version: None,
    }
}

fn value(answer: &Answer) -> String {
    match answer {
        Answer::Query {
            result: QueryResult::Read { value, .. },
            ..
        } => String::from_utf8_lossy(value).into_owned(),
        other => panic!("expected a value, got {other:?}"),
    }
}

#[test]
fn a_standby_answers_reads_at_a_version_and_sends_the_rest_on() {
    let mut h = Harness::with(inodedb_storage::Memory::new(), config());
    h.ops([mkdir("a"), put("a/f", "1")]);
    let mut follower = Follower::new(config(), h.now);
    assert!(run(&h, &mut follower).is_empty());
    assert!(follower.is_started());
    assert_eq!(follower.version(), h.writer.version());
    let trusted = Caller::Trusted(inodedb_core::types::ROOT);
    // The newest state is the writer's to give.
    follower.query(1, trusted, read("a/f"), 0);
    assert_eq!(
        run(&h, &mut follower),
        [(
            1,
            Answer::NotWriter {
                address: "writer:7070".into()
            }
        )]
    );
    // A read past the follower waits for the log.
    let key = h.mint(trusted, 9, "a", Grant::Read, 0);
    // A key newer than the log the follower has read is the writer's to
    // judge.
    follower.query(2, key, read("f"), follower.version());
    assert_eq!(
        run(&h, &mut follower),
        [(
            2,
            Answer::NotWriter {
                address: "writer:7070".into()
            }
        )]
    );
    h.ops([put("a/f", "2")]);
    let at = h.writer.version();
    follower.query(3, trusted, read("a/f"), at);
    assert!(run(&h, &mut follower).is_empty());
    follow(&h, &mut follower);
    let answers = run(&h, &mut follower);
    assert_eq!(answers.len(), 1);
    assert_eq!(value(&answers[0].1), "2");
    // Keys minted after the follower started work through it.
    follower.query(4, key, read("f"), at);
    let answers = run(&h, &mut follower);
    assert_eq!(value(&answers[0].1), "2");
}

#[test]
fn a_standby_takes_over_with_its_cache() {
    let mut h = Harness::with(inodedb_storage::Memory::new(), config());
    h.ops([mkdir_p("a/b"), put("a/b/f", "1")]);
    let mut follower = Follower::new(config(), h.now);
    run(&h, &mut follower);
    let trusted = Caller::Trusted(inodedb_core::types::ROOT);
    follower.query(1, trusted, read("a/b/f"), h.writer.version());
    assert_eq!(value(&run(&h, &mut follower)[0].1), "1");
    // The head goes unchanged for a writer lease plus the margin.
    let wait = config().takeover_wait();
    assert_eq!(
        follower.observe(h.storage.head_now()).expect("decodes"),
        None
    );
    follower.set_now(h.now + wait - 1);
    assert_eq!(
        follower.observe(h.storage.head_now()).expect("decodes"),
        None
    );
    follower.set_now(h.now + wait);
    let takeover = follower
        .observe(h.storage.head_now())
        .expect("decodes")
        .expect("takes over");
    assert!(matches!(takeover, Takeover::From(Some(_))));
    let secret = follower.handle_secret().expect("the store holds one");
    let handover = follower.hand_over();
    assert!(handover.waiting.is_empty());
    let writer = Writer::new(config(), Harness::writer_id(), h.now + wait, secret)
        .after(takeover)
        .warm(handover.cache, handover.version);
    let mut successor = Harness::with_writer(h.storage.clone(), writer);
    let reads = successor.reads;
    let (_, result) = successor.query(read("a/b/f"));
    assert!(
        matches!(&result, QueryResult::Read { value, .. } if &value[..] == b"1"),
        "{result:?}"
    );
    assert_eq!(successor.reads, reads, "answered from the standby's cache");
    successor.ops([put("a/b/f", "2")]);
    // The old writer's next commit conflicts, and it stops.
    let token = h.token();
    h.writer.submit(
        token,
        trusted,
        inodedb_core::ops::Batch {
            ops: vec![mkdir("late")],
            ..Default::default()
        },
    );
    h.run();
    assert_eq!(h.answer(token), Answer::Unavailable);
    assert!(h.writer.is_stopped());
    assert!(successor.fsck().is_empty(), "{:?}", successor.fsck());
}

#[test]
fn a_standby_waits_while_the_writer_renews() {
    let mut h = Harness::with(inodedb_storage::Memory::new(), config());
    let mut follower = Follower::new(config(), h.now);
    run(&h, &mut follower);
    let renew = config().renew_every;
    for _ in 0..20 {
        assert_eq!(
            follower.observe(h.storage.head_now()).expect("decodes"),
            None
        );
        h.tick(renew);
        follower.set_now(h.now);
    }
}

#[test]
fn a_standby_takes_over_only_after_the_writers_lease_on_drifting_clocks() {
    let config = Config {
        writer_lease: 1_000,
        renew_every: 200,
        takeover_margin: 200,
        ..config()
    };
    let mut h = Harness::with(inodedb_storage::Memory::new(), config.clone());
    // The writer's last commit; it renews no more.
    h.ops([mkdir("a")]);
    let start = h.now;
    let mut follower = Follower::new(config, start);
    run(&h, &mut follower);
    // The writer's clock runs 9% slow and the standby's 9% fast: as far
    // apart as a margin of a fifth of the lease covers.
    let mut real = 0;
    loop {
        real += 1;
        h.writer.set_now(start + real * 91 / 100);
        follower.set_now(start + real * 109 / 100);
        if follower
            .observe(h.storage.head_now())
            .expect("decodes")
            .is_some()
        {
            break;
        }
    }
    assert!(
        !h.writer.is_fresh(),
        "the standby may take over at {real} ms, while the writer still answers reads"
    );
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

fn ino(h: &mut Harness, path: &str) -> inodedb_core::types::Ino {
    match h.stat(path) {
        QueryResult::Stat { ino, .. } => ino,
        other => panic!("{other:?}"),
    }
}

/// A standby's reads by handle follow the file's names through renames and
/// links, and a writer that takes over with its cache keeps them right.
#[test]
fn a_standby_follows_links_through_renames() {
    let mut h = Harness::with(inodedb_storage::Memory::new(), config());
    h.ops([mkdir("a"), mkdir("b"), mkdir("c"), put("a/x", "1")]);
    let trusted = Caller::Trusted(inodedb_core::types::ROOT);
    let in_a = h.mint(trusted, 1, "a", Grant::Read, 0);
    let in_b = h.mint(trusted, 2, "b", Grant::Read, 0);
    let file = ino(&mut h, "a/x");
    let mut follower = Follower::new(config(), h.now);
    run(&h, &mut follower);
    let stat = |h: &mut Harness, follower: &mut Follower, key, token| {
        follow(h, follower);
        follower.query(
            token,
            key,
            Query::Stat(Target::Ino(file)),
            h.writer.version(),
        );
        let answers = run(h, follower);
        assert_eq!(answers.len(), 1, "{answers:?}");
        found(&answers[0].1)
    };
    assert!(stat(&mut h, &mut follower, in_a, 1), "cached under a");
    assert!(!stat(&mut h, &mut follower, in_b, 2));
    h.ops([mv("a/x", "b/y")]);
    assert!(!stat(&mut h, &mut follower, in_a, 3), "renamed out of a");
    assert!(stat(&mut h, &mut follower, in_b, 4), "renamed into b");
    h.ops([link("b/y", "c/z")]);
    assert!(stat(&mut h, &mut follower, in_b, 5));
    h.ops([unlink("b/y")]);
    assert!(!stat(&mut h, &mut follower, in_b, 6), "unlinked from b");
    h.ops([link("c/z", "a/w")]);
    assert!(stat(&mut h, &mut follower, in_a, 7), "linked into a");
    // The writer that takes over removes the file's last names with the
    // links the standby kept.
    follow(&h, &mut follower);
    let wait = config().takeover_wait();
    follower.set_now(h.now);
    assert_eq!(
        follower.observe(h.storage.head_now()).expect("decodes"),
        None
    );
    follower.set_now(h.now + wait);
    let takeover = follower
        .observe(h.storage.head_now())
        .expect("decodes")
        .expect("takes over");
    let secret = follower.handle_secret().expect("the store holds one");
    let handover = follower.hand_over();
    let writer = Writer::new(config(), Harness::writer_id(), h.now + wait, secret)
        .after(takeover)
        .warm(handover.cache, handover.version);
    let mut successor = Harness::with_writer(h.storage.clone(), writer);
    successor.ops([unlink("c/z"), unlink("a/w")]);
    assert!(successor.fsck().is_empty(), "{:?}", successor.fsck());
}

/// A standby whose cache holds less than a read fetches still answers the
/// read while commits keep landing ahead of it.
#[test]
fn a_standby_with_a_small_cache_answers_under_steady_writes() {
    let config = Config {
        cache: inodedb_core::cache::Budget {
            dirs: 1,
            inodes: 1,
            value_bytes: 1,
            ..inodedb_core::cache::Budget::default()
        },
        ..config()
    };
    let mut h = Harness::with(inodedb_storage::Memory::new(), config.clone());
    h.ops([mkdir("a"), mkdir("b"), put("a/f", "value")]);
    let mut follower = Follower::new(config, h.now);
    run(&h, &mut follower);
    let trusted = Caller::Trusted(inodedb_core::types::ROOT);
    follower.query(1, trusted, read("a/f"), h.writer.version());
    let mut answers = Vec::new();
    for round in 0..20 {
        for action in follower.drain() {
            match action {
                inodedb_core::writer::Action::Read { id, reads } => {
                    // A commit lands before each read does.
                    h.ops([put(&format!("b/x{round}"), "")]);
                    follower
                        .read_done(id, h.storage.read_now(&reads))
                        .expect("decodes");
                }
                inodedb_core::writer::Action::Answer { answer, .. } => answers.push(answer),
                other => panic!("a follower asked for {other:?}"),
            }
        }
        if !answers.is_empty() {
            break;
        }
        follow(&h, &mut follower);
    }
    assert_eq!(answers.len(), 1, "answered within the rounds");
    assert_eq!(value(&answers[0]), "value");
}

/// A read of a value under a version the standby hasn't reached waits for
/// it, and the cache never takes the value as absent.
#[test]
fn a_standby_waits_for_a_value_version_it_hasnt_reached() {
    let mut h = Harness::with(inodedb_storage::Memory::new(), config());
    h.ops([mkdir("a"), put("a/f", "1")]);
    let mut follower = Follower::new(config(), h.now);
    run(&h, &mut follower);
    let trusted = Caller::Trusted(inodedb_core::types::ROOT);
    let next = h.writer.version() + 1;
    follower.query(
        1,
        trusted,
        Query::Read {
            target: Target::Path(p("a/f")),
            version: Some(next),
        },
        h.writer.version(),
    );
    assert!(run(&h, &mut follower).is_empty(), "it waits for {next}");
    h.ops([put("a/f", "2")]);
    assert_eq!(h.writer.version(), next);
    follow(&h, &mut follower);
    let answers = run(&h, &mut follower);
    assert_eq!(answers.len(), 1);
    assert_eq!(value(&answers[0].1), "2");
    follower.query(2, trusted, read("a/f"), next);
    assert_eq!(value(&run(&h, &mut follower)[0].1), "2");
}
