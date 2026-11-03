//! Scripted scenarios for group commit, storage faults, request ids and
//! restarts. A driver here holds each commit until the test settles it, so
//! groups form behind the one in flight.

use inodedb_core::Bytes;
use inodedb_core::keys::{self, KvWrite, Parsed, WriteSet};
use inodedb_core::ops::{Batch, CommitResult, Op, OpSpec, Outcome, PutMode, Skip};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Error, Ino, Millis, ROOT};
use inodedb_core::writer::{Action, Answer, CommitOutcome, Config, Head, Token, Writer};
use inodedb_sim::harness::*;
use inodedb_storage::Memory;
use std::collections::{BTreeMap, VecDeque};

/// A commit the writer sent and the test has yet to settle.
struct Pending {
    id: u64,
    expect: Option<Vec<u8>>,
    writes: WriteSet,
}

/// Runs reads and resolves at once and holds commits.
struct Driver {
    writer: Writer,
    storage: Memory,
    commits: VecDeque<Pending>,
    answers: BTreeMap<Token, Answer>,
    next_token: Token,
    /// Rounds of reads the writer has sent.
    reads: usize,
}

impl Driver {
    fn new(storage: Memory, config: Config, id: u64) -> Driver {
        let mut driver = Driver {
            writer: Writer::new(config, id, 1_000, [7; 32]),
            storage,
            commits: VecDeque::new(),
            answers: BTreeMap::new(),
            next_token: 1,
            reads: 0,
        };
        driver.pump();
        // A fresh store's first commit is the root.
        if !driver.writer.is_ready() {
            driver.settle(true, CommitOutcome::Committed);
        }
        assert!(driver.writer.is_ready());
        driver
    }

    fn pump(&mut self) {
        loop {
            let actions = self.writer.drain();
            if actions.is_empty() {
                return;
            }
            for action in actions {
                match action {
                    Action::Read { id, reads } => {
                        self.reads += 1;
                        let results = self.storage.read_now(&reads);
                        self.writer.read_done(id, results);
                    }
                    Action::Commit { id, expect, writes } => {
                        self.commits.push_back(Pending { id, expect, writes })
                    }
                    Action::Resolve { id } => {
                        let head = self.storage.head_now();
                        self.writer.resolve_done(id, head);
                    }
                    Action::Answer { token, answer } => {
                        self.answers.insert(token, answer);
                    }
                }
            }
        }
    }

    fn submit(&mut self, ops: impl IntoIterator<Item = OpSpec>) -> Token {
        self.submit_batch(Batch {
            ops: ops.into_iter().collect(),
            ..Batch::default()
        })
    }

    /// Submits a batch without carrying out what the writer asks for, so
    /// batches queue behind one that waits on reads.
    fn queue(&mut self, ops: Vec<OpSpec>) -> Token {
        let token = self.next_token;
        self.next_token += 1;
        self.writer.submit(
            token,
            inodedb_core::auth::Caller::Trusted(inodedb_core::types::ROOT),
            Batch {
                ops,
                ..Batch::default()
            },
        );
        token
    }

    /// Runs a query for a caller trusted at `root`, carrying out its reads.
    fn query(&mut self, root: Ino, query: Query) -> QueryResult {
        let token = self.next_token;
        self.next_token += 1;
        self.writer
            .query(token, inodedb_core::auth::Caller::Trusted(root), query, 0);
        self.pump();
        match self.answers.remove(&token) {
            Some(Answer::Query { result, .. }) => result,
            other => panic!("token {token}: {other:?}"),
        }
    }

    fn ino(&mut self, path: &str) -> Ino {
        match self.query(ROOT, Query::Stat(Target::Path(p(path)))) {
            QueryResult::Stat { ino, .. } => ino,
            other => panic!("{path}: {other:?}"),
        }
    }

    fn submit_batch(&mut self, batch: Batch) -> Token {
        let token = self.next_token;
        self.next_token += 1;
        self.writer.submit(
            token,
            inodedb_core::auth::Caller::Trusted(inodedb_core::types::ROOT),
            batch,
        );
        self.pump();
        token
    }

    /// Settles the oldest commit: `apply` decides whether it reaches
    /// storage, and `outcome` is what the writer hears.
    fn settle(&mut self, apply: bool, outcome: CommitOutcome) -> WriteSet {
        let pending = self.commits.pop_front().expect("a commit in flight");
        if apply {
            assert_eq!(
                self.storage
                    .commit_now(pending.expect.as_deref(), &pending.writes),
                CommitOutcome::Committed
            );
        }
        self.writer.commit_done(pending.id, outcome);
        self.pump();
        pending.writes
    }

    fn committed(&mut self, token: Token) -> CommitResult {
        match self.answers.remove(&token) {
            Some(Answer::Commit(result)) => result,
            other => panic!("token {token}: {other:?}"),
        }
    }

    fn fsck(&self) -> Vec<String> {
        let dump = self.storage.dump();
        inodedb_core::fsck::check(
            dump.iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
    }
}

fn put_bytes(path: &str, len: usize) -> OpSpec {
    OpSpec::new(Op::Put {
        path: p(path),
        value: Bytes::from(vec![7u8; len]),
        mode: PutMode::Upsert,
        mtime: None,
    })
}

/// Request ids with a record in storage, and how many parts each has.
fn records(storage: &Memory) -> BTreeMap<u128, usize> {
    let mut found = BTreeMap::new();
    for (key, _) in storage.dump() {
        if let Some(Parsed::System(b'r', rest)) = keys::parse(&key) {
            let id = u128::from_be_bytes(rest[..16].try_into().expect("16 bytes"));
            *found.entry(id).or_insert(0) += 1;
        }
    }
    found
}

/// An owner that hands the writer several batches before asking for its
/// actions gets one transaction holding them all.
#[test]
fn batches_queued_before_a_drain_commit_together() {
    let mut d = Driver::new(Memory::new(), Config::default(), 1);
    d.submit([mkdir("a")]);
    d.settle(true, CommitOutcome::Committed);
    assert!(d.commits.is_empty());
    for (token, name) in (100..).zip(["a/1", "a/2", "a/3"]) {
        let batch = Batch {
            ops: vec![mkdir(name)],
            ..Batch::default()
        };
        d.writer
            .submit(token, inodedb_core::auth::Caller::Trusted(ROOT), batch);
    }
    d.pump();
    assert_eq!(d.commits.len(), 1, "one transaction");
    let writes = d.settle(true, CommitOutcome::Committed);
    assert_eq!(writes.log.len(), 3, "holding every batch");
    assert!(d.commits.is_empty(), "and nothing behind it");
}

#[test]
fn batches_wait_for_their_group_and_groups_close_at_their_size() {
    let config = Config {
        group_max_bytes: 700,
        ..Config::default()
    };
    let mut d = Driver::new(Memory::new(), config, 1);
    let first = d.submit([mkdir("a")]);
    assert_eq!(d.commits.len(), 1, "the first batch commits alone");
    let puts: Vec<Token> = (0..6)
        .map(|index| d.submit([put_bytes(&format!("a/f{index}"), 200)]))
        .collect();
    assert_eq!(d.commits.len(), 1, "one transaction in flight");
    assert!(
        d.answers.is_empty(),
        "nothing answers before its group commits"
    );
    d.settle(true, CommitOutcome::Committed);
    assert_eq!(d.committed(first).version, 2);
    let mut groups = Vec::new();
    while !d.commits.is_empty() {
        let writes = d.settle(true, CommitOutcome::Committed);
        assert!(
            writes.log.len() == 1 || writes.bytes() <= 700 + 100,
            "a group stays near its size"
        );
        groups.push(writes.log.len());
    }
    assert_eq!(groups.iter().sum::<usize>(), 6);
    assert!(groups.len() > 1, "the cap splits the batches: {groups:?}");
    for (index, token) in puts.into_iter().enumerate() {
        let result = d.committed(token);
        assert_eq!(
            (result.version, result.committed),
            (3 + index as u64, true),
            "arrival order"
        );
    }
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
}

#[test]
fn a_failed_group_runs_its_batches_again() {
    let mut d = Driver::new(Memory::new(), Config::default(), 1);
    let make = d.submit([mkdir("x")]);
    let file = d.submit([put("x/f", "v")]);
    let unchanged = d.submit([mkdir_p("x")]);
    d.settle(false, CommitOutcome::Failed);
    assert!(
        d.answers.is_empty(),
        "batches built on the failed group wait"
    );
    let writes = d.settle(true, CommitOutcome::Committed);
    assert_eq!(
        writes.log.len(),
        2,
        "both changing batches ran again as one group"
    );
    assert_eq!(d.committed(make).version, 2);
    assert_eq!(d.committed(file).version, 3);
    let unchanged = d.committed(unchanged);
    assert_eq!((unchanged.committed, unchanged.version), (false, 3));
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
}

#[test]
fn an_unknown_commit_that_applied_is_answered_as_committed() {
    let mut d = Driver::new(Memory::new(), Config::default(), 1);
    let token = d.submit([mkdir("u")]);
    d.settle(true, CommitOutcome::Unknown);
    assert_eq!(d.committed(token).version, 2);
    let next = d.submit([mkdir("u/v")]);
    d.settle(true, CommitOutcome::Committed);
    assert_eq!(d.committed(next).version, 3);
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
}

#[test]
fn an_unknown_commit_that_did_not_apply_runs_again() {
    let mut d = Driver::new(Memory::new(), Config::default(), 1);
    let token = d.submit([mkdir("u")]);
    d.settle(false, CommitOutcome::Unknown);
    assert!(d.answers.is_empty());
    d.settle(true, CommitOutcome::Committed);
    assert_eq!(d.committed(token).version, 2);
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
}

/// Two writers commit at one version: the head's writer id tells the
/// first that the commit it couldn't confirm was the other's.
#[test]
fn a_resolve_that_finds_another_writers_commit_stops() {
    let storage = Memory::new();
    let mut first = Driver::new(storage.clone(), Config::default(), 1);
    let lost = first.submit([mkdir("x")]);
    let mut second = Driver::new(storage.clone(), Config::default(), 2);
    let won = second.submit([mkdir("y")]);
    second.settle(true, CommitOutcome::Committed);
    assert_eq!(second.committed(won).version, 2);
    first.settle(false, CommitOutcome::Unknown);
    assert_eq!(first.answers.remove(&lost), Some(Answer::Unavailable));
    assert!(first.writer.is_stopped());
    assert!(first.fsck().is_empty(), "{:?}", first.fsck());
}

#[test]
fn a_restarted_writer_answers_retries_from_records() {
    let mut h = Harness::new();
    let batch = Batch {
        ops: vec![mkdir("r"), put_mode("r/f", "1", PutMode::Create)],
        request_id: 41,
        ..Batch::default()
    };
    let first = h.commit(batch.clone());
    assert!(first.committed);
    let mut restarted = Harness::with(h.storage.clone(), Config::default());
    assert_eq!(restarted.commit(batch), first, "the record answers");
    let next = restarted.ops([mkdir("s")]);
    assert_eq!(next.version, first.version + 1, "the retry took no version");
}

#[test]
fn a_retry_of_a_batch_that_changed_nothing_gets_the_first_answer() {
    let mut h = Harness::new();
    h.ops([mkdir("a")]);
    let batch = Batch {
        ops: vec![mkdir("a")],
        request_id: 52,
        ..Batch::default()
    };
    let first = h.commit(batch.clone());
    assert!(!first.committed);
    assert_eq!(error(&first.outcomes[0]), Some(Error::Exists));
    h.ops([rmdir("a")]);
    assert_eq!(
        h.commit(batch.clone()),
        first,
        "the writer remembers the answer"
    );
    // A restarted writer has no record of it, so the batch runs again.
    let mut restarted = Harness::with(h.storage.clone(), Config::default());
    assert!(restarted.commit(batch).committed);
}

fn error(outcome: &Outcome) -> Option<Error> {
    match outcome {
        Outcome::Skipped(Skip::Error(error)) => Some(*error),
        _ => None,
    }
}

/// A store whose next inode id, near 2^62, makes each outcome long enough that
/// ten thousand of them pass one record part.
fn a_store_of_long_outcomes() -> Memory {
    let h = Harness::new();
    let head = h.storage.head_now().expect("a head");
    let mut writes = WriteSet::default();
    let mut moved = Head::decode(&head).expect("decodes");
    moved.next_ino = 1 << 62;
    writes.set(keys::head(), moved.encode());
    assert_eq!(
        h.storage.commit_now(Some(&head), &writes),
        CommitOutcome::Committed
    );
    h.storage.clone()
}

fn a_long_batch(request_id: u128) -> Batch {
    Batch {
        ops: (0..10_000)
            .map(|index| mkdir(&format!("d{index}")))
            .collect(),
        request_id,
        ..Batch::default()
    }
}

/// Every part of a long record goes when its lifetime ends, by the keys its
/// writer remembers, and the writer forgets the count.
#[test]
fn a_long_record_goes_whole() {
    let ttl: Millis = 10_000;
    let config = Config {
        request_ttl: ttl,
        ..Config::default()
    };
    let mut h = Harness::with(a_store_of_long_outcomes(), config);
    assert!(h.commit(a_long_batch(63)).committed);
    assert!(records(&h.storage)[&63] > 1, "the record spans parts");
    assert_eq!(h.writer.long_records(), 1);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    h.tick(ttl);
    h.ops([mkdir("after")]);
    assert!(!records(&h.storage).contains_key(&63), "every part went");
    assert_eq!(h.writer.long_records(), 0);
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
}

/// A restarted writer learns a record's parts from its expiry key.
#[test]
fn a_restarted_writer_deletes_every_part() {
    let ttl: Millis = 10_000;
    let config = Config {
        request_ttl: ttl,
        ..Config::default()
    };
    let mut h = Harness::with(a_store_of_long_outcomes(), config.clone());
    assert!(h.commit(a_long_batch(63)).committed);
    let mut restarted = Harness::with(h.storage.clone(), config);
    assert_eq!(restarted.writer.long_records(), 1);
    restarted.tick(ttl);
    restarted.ops([mkdir("after")]);
    assert!(!records(&restarted.storage).contains_key(&63));
    assert!(restarted.fsck().is_empty(), "{:?}", restarted.fsck());
}

/// A group that fails puts its deletions back, with what they need to delete
/// every part.
#[test]
fn a_failed_group_keeps_a_long_records_parts() {
    let ttl: Millis = 10_000;
    let config = Config {
        request_ttl: ttl,
        ..Config::default()
    };
    let mut d = Driver::new(a_store_of_long_outcomes(), config, 1);
    let token = d.submit_batch(a_long_batch(63));
    d.settle(true, CommitOutcome::Committed);
    assert!(d.committed(token).committed);
    d.writer.set_now(1_000 + ttl + 1);
    let blocker = d.submit([mkdir("blocker")]);
    d.settle(false, CommitOutcome::Failed);
    d.settle(true, CommitOutcome::Committed);
    assert!(d.committed(blocker).committed);
    assert!(!records(&d.storage).contains_key(&63), "every part went");
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
}

#[test]
fn a_long_result_spans_record_parts() {
    let mut h = Harness::with(a_store_of_long_outcomes(), Config::default());
    let batch = a_long_batch(63);
    let first = h.commit(batch.clone());
    assert!(first.committed && first.outcomes.iter().all(Outcome::applied));
    assert!(records(&h.storage)[&63] > 1, "the record spans parts");
    let mut restarted = Harness::with(h.storage.clone(), Config::default());
    assert_eq!(restarted.commit(batch), first, "the parts join again");
    assert!(restarted.fsck().is_empty(), "{:?}", restarted.fsck());
}

#[test]
fn records_go_once_their_lifetime_ends() {
    let ttl: Millis = 10_000;
    let config = Config {
        request_ttl: ttl,
        ..Config::default()
    };
    let mut h = Harness::with(Memory::new(), config.clone());
    let batch = Batch {
        ops: vec![put_mode("once", "1", PutMode::Create)],
        request_id: 74,
        ..Batch::default()
    };
    assert!(h.commit(batch.clone()).committed);
    h.tick(ttl / 2);
    h.ops([mkdir("a")]);
    assert!(
        records(&h.storage).contains_key(&74),
        "the record lasts its lifetime"
    );
    assert_eq!(
        h.commit(batch.clone()).outcomes[0],
        Outcome::Applied {
            ino: h_ino(&mut h, "once")
        }
    );
    h.tick(ttl);
    h.ops([mkdir("b")]);
    assert!(
        !records(&h.storage).contains_key(&74),
        "a later commit deletes it"
    );
    assert!(h.fsck().is_empty(), "{:?}", h.fsck());
    let rerun = h.commit(batch);
    assert_eq!(
        error(&rerun.outcomes[0]),
        Some(Error::Exists),
        "a retry runs again"
    );
    // A restarted writer loads only live records.
    let mut restarted = Harness::with(h.storage.clone(), config);
    restarted.ops([mkdir("c")]);
    assert!(restarted.fsck().is_empty(), "{:?}", restarted.fsck());
}

fn h_ino(h: &mut Harness, path: &str) -> Option<u64> {
    match h.stat(path) {
        QueryResult::Stat { ino, .. } => Some(ino),
        other => panic!("{other:?}"),
    }
}

/// Deletions of expired records come before the group's writes. A group
/// that fails puts its deletions back, and a retry run in the meantime may
/// join the group that deletes its old record; its new record stays.
#[test]
fn a_record_written_again_survives_its_old_records_deletion() {
    let ttl: Millis = 10_000;
    let config = Config {
        request_ttl: ttl,
        ..Config::default()
    };
    let mut d = Driver::new(Memory::new(), config, 1);
    let batch = Batch {
        ops: vec![mkdir("again")],
        request_id: 85,
        ..Batch::default()
    };
    let token = d.submit_batch(batch.clone());
    d.settle(true, CommitOutcome::Committed);
    assert!(d.committed(token).committed);
    d.submit([rmdir("again")]);
    d.settle(true, CommitOutcome::Committed);
    d.writer.set_now(1_000 + ttl + 1);
    let blocker = d.submit([mkdir("blocker")]);
    let retry = d.submit_batch(batch);
    d.settle(false, CommitOutcome::Failed);
    let writes = d.settle(true, CommitOutcome::Committed);
    assert_eq!(
        writes.log.len(),
        2,
        "the retry ran again beside the deletion"
    );
    assert!(
        writes
            .writes
            .iter()
            .any(|write| matches!(write, KvWrite::Clear(key) if *key == keys::request(85, 0)))
    );
    assert!(d.committed(blocker).committed);
    assert!(
        d.committed(retry).committed,
        "the expired request ran again"
    );
    assert!(
        records(&d.storage).contains_key(&85),
        "its new record stays"
    );
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
}

/// A writer reads storage only for what its cache lacks: names it has not
/// looked up, in directories it did not create.
#[test]
fn a_warm_writer_reads_nothing() {
    let mut h = Harness::new();
    h.ops([mkdir_p("t/p/q"), put("t/p/q/f", "v")]);
    let before = h.reads;
    h.ops([put("t/p/q/g", "w"), mv("t/p/q/g", "t/p/g"), mkdir("t/p/r")]);
    h.ops([put("t/p/r/h", "x"), unlink("t/p/q/f")]);
    h.stat("t/p/g");
    assert_eq!(
        h.reads, before,
        "the writer created every directory it touched"
    );

    let mut restarted = Harness::with(h.storage.clone(), Config::default());
    restarted.ops([put("t/p/r/h", "y")]);
    assert!(restarted.reads > 0, "a cold writer reads what it lacks");
    let before = restarted.reads;
    restarted.ops([put("t/p/r/h", "z")]);
    restarted.stat("t/p/r/h");
    assert_eq!(restarted.reads, before, "the second visit reads nothing");
}

/// A lease whose opening fails and runs out before the opening runs again
/// stays stored once the second run commits.
#[test]
fn a_lease_reopened_after_it_ran_out_stays_stored() {
    let config = Config {
        lease_ttl: 1_000,
        ..Config::default()
    };
    let mut d = Driver::new(Memory::new(), config, 1);
    let token = d.next_token;
    d.next_token += 1;
    d.writer.open_lease(
        token,
        inodedb_core::auth::Caller::Trusted(inodedb_core::types::ROOT),
        None,
    );
    d.pump();
    d.writer.set_now(2_000);
    d.settle(false, CommitOutcome::Failed);
    d.settle(true, CommitOutcome::Committed);
    let granted = match d.answers.remove(&token) {
        Some(Answer::Lease(Ok(granted))) => granted,
        other => panic!("{other:?}"),
    };
    assert!(
        d.storage
            .dump()
            .iter()
            .any(|(key, _)| *key == keys::lease(granted.lease)),
        "the second run's record stays"
    );
}

/// Rounds of reads a restarted writer sends to run `batches` puts into a
/// directory it didn't create, all queued at once.
fn rounds_for(batches: usize) -> usize {
    let storage = Memory::new();
    let mut first = Driver::new(storage.clone(), Config::default(), 1);
    first.submit([mkdir("d")]);
    first.settle(true, CommitOutcome::Committed);
    let mut d = Driver::new(storage, Config::default(), 2);
    let before = d.reads;
    let tokens: Vec<Token> = (0..batches)
        .map(|index| d.queue(vec![put(&format!("d/f{index}"), "v")]))
        .collect();
    d.pump();
    let rounds = d.reads - before;
    while !d.commits.is_empty() {
        d.settle(true, CommitOutcome::Committed);
    }
    for token in tokens {
        assert!(d.committed(token).committed);
    }
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
    rounds
}

/// Batches queued behind one that misses the cache read what they lack in
/// the same round.
#[test]
fn queued_batches_that_miss_the_cache_read_together() {
    let alone = rounds_for(1);
    let queued = rounds_for(32);
    assert!(
        queued <= alone + 1,
        "one batch takes {alone} rounds, and 32 take {queued}"
    );
}

/// Running ahead of a batch that waits on reads costs each round a bounded
/// number of ops, however much cached work queues behind it.
#[test]
fn reading_ahead_runs_a_bounded_number_of_ops_a_round() {
    let storage = Memory::new();
    let deep: Vec<String> = (0..20).map(|level| format!("p{level}")).collect();
    let deep = deep.join("/");
    let mut first = Driver::new(storage.clone(), Config::default(), 1);
    first.submit([mkdir_p(&deep)]);
    first.settle(true, CommitOutcome::Committed);
    let mut d = Driver::new(storage, Config::default(), 2);
    // A directory this writer creates stays cached.
    d.submit([mkdir("hot"), put("hot/f", "v")]);
    d.settle(true, CommitOutcome::Committed);
    let before = d.reads;
    let mut tokens = vec![d.queue(vec![put(&format!("{deep}/f"), "v")])];
    for batch in 0..64 {
        tokens.push(
            d.queue(
                (0..100)
                    .map(|op| set_mtime("hot/f", batch * 100 + op))
                    .collect(),
            ),
        );
    }
    d.pump();
    let rounds = (d.reads - before) as u64;
    assert!(rounds >= 20, "a round for each level: {rounds}");
    // READ_AHEAD_OPS a round, where the queue behind holds 6,400.
    assert!(
        d.writer.read_ahead_ops() <= rounds * 1_024,
        "{} ops ahead over {rounds} rounds",
        d.writer.read_ahead_ops()
    );
    while !d.commits.is_empty() {
        d.settle(true, CommitOutcome::Committed);
    }
    for token in tokens {
        assert!(d.committed(token).committed);
    }
    assert!(d.fsck().is_empty(), "{:?}", d.fsck());
}

/// A read by inode inside a detached subtree reaches what the directories
/// above it held as it detached, not those above where they have moved
/// since.
#[test]
fn a_detached_subtree_stays_within_the_directories_above_it_as_it_detached() {
    let mut d = Driver::new(Memory::new(), Config::default(), 1);
    d.submit([mkdir_p("p/t"), put("p/t/f", "v"), mkdir("q")]);
    d.settle(true, CommitOutcome::Committed);
    let (p_ino, q, f) = (d.ino("p"), d.ino("q"), d.ino("p/t/f"));
    d.submit([remove_all("p/t"), mv("p", "q/p")]);
    d.settle(true, CommitOutcome::Committed);
    // The freeing waits behind the commit in flight, so f stays detached.
    assert_eq!(d.commits.len(), 1, "the freeing's commit is held");
    let stat = Query::Stat(Target::Ino(f));
    assert!(
        matches!(d.query(p_ino, stat.clone()), QueryResult::Stat { ino, .. } if ino == f),
        "p held the subtree as it detached"
    );
    assert_eq!(
        d.query(q, stat),
        QueryResult::Failed(Error::NotFound),
        "q came above p only after the subtree detached"
    );
}

/// A link inside a removed subtree keeps its file under the directories above
/// the subtree, whatever names the file keeps elsewhere, until freeing reaches
/// the link.
#[test]
fn a_detached_link_keeps_its_file_under_the_directories_above_it_until_freed() {
    let mut d = Driver::new(Memory::new(), Config::default(), 1);
    d.submit([mkdir_p("p/t"), put("p/t/f", "v"), mkdir("q")]);
    d.settle(true, CommitOutcome::Committed);
    d.submit([link("p/t/f", "q/f")]);
    d.settle(true, CommitOutcome::Committed);
    let (p, q, f) = (d.ino("p"), d.ino("q"), d.ino("p/t/f"));
    d.submit([remove_all("p/t")]);
    d.settle(true, CommitOutcome::Committed);
    let stat = Query::Stat(Target::Ino(f));
    // The freeing waits behind the commit in flight, so the link stays.
    assert_eq!(d.commits.len(), 1, "the freeing's commit is held");
    assert!(
        matches!(d.query(p, stat.clone()), QueryResult::Stat { ino, .. } if ino == f),
        "p held the link as it detached"
    );
    assert!(
        matches!(d.query(q, stat.clone()), QueryResult::Stat { ino, .. } if ino == f),
        "q holds the file's other name"
    );
    d.settle(true, CommitOutcome::Committed);
    assert_eq!(
        d.query(p, stat.clone()),
        QueryResult::Failed(Error::NotFound),
        "freeing the link leaves the file outside p"
    );
    assert!(
        matches!(d.query(q, stat), QueryResult::Stat { ino, .. } if ino == f),
        "q still holds the file's other name"
    );
}

/// A starting writer keeps the request records it finds for a whole
/// lifetime by its own clock, however far ahead of the clocks that stamped
/// them it runs, so a retry still gets its first answer.
#[test]
fn a_starting_writer_keeps_records_a_lifetime_by_its_own_clock() {
    let ttl: Millis = 10_000;
    let config = Config {
        request_ttl: ttl,
        ..Config::default()
    };
    let mut first = Harness::with(Memory::new(), config.clone());
    let batch = Batch {
        ops: vec![put_mode("once", "1", PutMode::Create)],
        request_id: 74,
        ..Batch::default()
    };
    assert!(first.commit(batch.clone()).committed);
    let ino = h_ino(&mut first, "once");
    // The next writer's clock runs two lifetimes ahead of the first's.
    let writer = Writer::new(config, Harness::writer_id(), 1_000 + 2 * ttl, [7; 32]);
    let mut second = Harness::with_writer(first.storage.clone(), writer);
    second.ops([mkdir("a")]);
    assert!(
        records(&second.storage).contains_key(&74),
        "the record outlives the new writer's first commit"
    );
    assert_eq!(second.commit(batch).outcomes[0], Outcome::Applied { ino });
}
