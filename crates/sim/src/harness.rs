//! A writer driven synchronously against the memory backend, for scripted
//! scenarios: every action runs as soon as the writer asks.

use inodedb_core::Bytes;
use inodedb_core::auth::{Caller, Grant, KeyId, KeyOp};
use inodedb_core::delta::Freed;
use inodedb_core::fsck;
use inodedb_core::lease::{
    ClaimError, ClaimId, FreeEntry, Granted, LeaseError, LeaseId, LeaseRecord,
};
use inodedb_core::ops::{Batch, CommitResult, Op, OpSpec, Pred, PutMode, RenameFlag};
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Ino, Millis, Path, ROOT, Version};
use inodedb_core::writer::{Action, Answer, Config, Writer};
use inodedb_storage::Memory;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Request ids unique across every harness in the process, so a writer
/// restarted over old storage never mistakes a new batch for a retry.
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
/// Writer ids unique across every harness in the process.
static NEXT_WRITER: AtomicU64 = AtomicU64::new(1);

/// A page of the free queue, as `Answer::FreeQueue` holds it.
#[derive(Debug)]
pub struct FreePage {
    pub version: Version,
    pub entries: Vec<FreeEntry>,
    pub next: Option<(Version, Freed)>,
    pub freed_through: Version,
}

pub struct Harness {
    pub writer: Writer,
    pub storage: Memory,
    pub now: Millis,
    pub root: Ino,
    /// Storage reads the writer asked for.
    pub reads: usize,
    next_token: u64,
    answers: BTreeMap<u64, Answer>,
}

impl Default for Harness {
    fn default() -> Harness {
        Harness::new()
    }
}

impl Harness {
    pub fn new() -> Harness {
        Harness::with(Memory::new(), Config::default())
    }

    /// A writer starting over existing storage.
    pub fn with(storage: Memory, config: Config) -> Harness {
        let writer = Writer::new(config, Harness::writer_id(), 1_000, [7; 32]);
        Harness::with_writer(storage, writer)
    }

    /// An id no other harness's writer has.
    pub fn writer_id() -> u64 {
        NEXT_WRITER.fetch_add(1, Ordering::Relaxed)
    }

    /// Runs `writer`, started at time 1,000, over existing storage.
    pub fn with_writer(storage: Memory, writer: Writer) -> Harness {
        let mut harness = Harness {
            writer,
            storage,
            now: 1_000,
            root: ROOT,
            reads: 0,
            next_token: 1,
            answers: BTreeMap::new(),
        };
        harness.run();
        assert!(harness.writer.is_ready(), "the writer starts");
        harness
    }

    pub fn run(&mut self) {
        loop {
            let actions = self.writer.drain();
            if actions.is_empty() {
                return;
            }
            for action in actions {
                self.carry_out(action);
            }
        }
    }

    /// Runs the writer as `run` does until `token` has its answer, then
    /// holds back the commits it asks for, and returns them.
    pub fn run_until(&mut self, token: u64) -> Vec<Action> {
        let mut held = Vec::new();
        loop {
            let actions = self.writer.drain();
            if actions.is_empty() {
                return held;
            }
            for action in actions {
                match action {
                    Action::Commit { .. } | Action::Resolve { .. }
                        if self.answers.contains_key(&token) =>
                    {
                        held.push(action);
                    }
                    action => self.carry_out(action),
                }
            }
        }
    }

    /// Carries out the actions `run_until` held back, then runs on.
    pub fn resume(&mut self, held: Vec<Action>) {
        for action in held {
            self.carry_out(action);
        }
        self.run();
    }

    fn carry_out(&mut self, action: Action) {
        match action {
            Action::Read { id, reads } => {
                self.reads += 1;
                let results = self.storage.read_now(&reads);
                self.writer.read_done(id, results);
            }
            Action::Commit { id, expect, writes } => {
                let outcome = self.storage.commit_now(expect.as_deref(), &writes);
                self.writer.commit_done(id, outcome);
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

    /// Moves time on, letting the writer act on it as a server's timer
    /// does.
    pub fn tick(&mut self, by: Millis) {
        self.now += by;
        self.writer.set_now(self.now);
        self.writer.tick();
        self.run();
    }

    /// Submits a batch and returns the writer's answer.
    pub fn submit(&mut self, batch: Batch) -> Answer {
        self.submit_as(Caller::Trusted(self.root), batch)
    }

    pub fn submit_as(&mut self, caller: Caller, mut batch: Batch) -> Answer {
        if batch.request_id == 0 {
            batch.request_id = u128::from(NEXT_REQUEST.fetch_add(1, Ordering::Relaxed)) | 1 << 64;
        }
        let token = self.next_token;
        self.next_token += 1;
        self.writer.submit(token, caller, batch);
        self.run();
        self.answers.remove(&token).expect("the writer answered")
    }

    /// Runs ops as one batch under a caller.
    pub fn ops_as(&mut self, caller: Caller, ops: impl IntoIterator<Item = OpSpec>) -> Answer {
        self.submit_as(
            caller,
            Batch {
                ops: ops.into_iter().collect(),
                ..Batch::default()
            },
        )
    }

    /// A token for driving the writer directly.
    pub fn token(&mut self) -> u64 {
        self.next_token += 1;
        self.next_token - 1
    }

    /// The writer's answer for a token.
    pub fn answer(&mut self, token: u64) -> Answer {
        self.answers.remove(&token).expect("the writer answered")
    }

    pub fn key_op(&mut self, caller: Caller, op: KeyOp) -> Answer {
        let token = self.next_token;
        self.next_token += 1;
        self.writer.submit_key(token, caller, op);
        self.run();
        self.answers.remove(&token).expect("the writer answered")
    }

    /// Mints a key rooted at `root` and returns it as a caller. Its hash
    /// is its id's bytes, repeated.
    pub fn mint(&mut self, by: Caller, id: KeyId, root: &str, grant: Grant, ttl: Millis) -> Caller {
        let hash = key_hash(id);
        match self.key_op(
            by,
            KeyOp::Mint {
                id,
                hash,
                root: p(root),
                grant,
                ttl,
            },
        ) {
            Answer::Key(Ok(_)) => Caller::Key { id, hash },
            other => panic!("expected a minted key, got {other:?}"),
        }
    }

    pub fn open_lease(&mut self, caller: Caller, oldest: Version) -> Result<Granted, LeaseError> {
        let token = self.token();
        let record = LeaseRecord {
            oldest,
            position: oldest,
        };
        self.writer.open_lease(token, caller, Some(record));
        self.run();
        match self.answer(token) {
            Answer::Lease(result) => result,
            other => panic!("expected a lease, got {other:?}"),
        }
    }

    pub fn beat(
        &mut self,
        caller: Caller,
        lease: LeaseId,
        oldest: Version,
    ) -> Result<Granted, LeaseError> {
        let token = self.token();
        let record = LeaseRecord {
            oldest,
            position: oldest,
        };
        self.writer.beat_lease(token, caller, lease, record);
        self.run();
        match self.answer(token) {
            Answer::Lease(result) => result,
            other => panic!("expected a lease, got {other:?}"),
        }
    }

    pub fn claim(&mut self, caller: Caller, limit: usize) -> (ClaimId, Vec<FreeEntry>) {
        let token = self.token();
        self.writer.claim(token, caller, limit);
        self.run();
        match self.answer(token) {
            Answer::Claimed { claim, entries } => (claim, entries),
            other => panic!("expected a claim, got {other:?}"),
        }
    }

    pub fn ack(
        &mut self,
        caller: Caller,
        claim: ClaimId,
        entries: &[FreeEntry],
    ) -> Result<Version, ClaimError> {
        let token = self.token();
        let keys: Vec<_> = entries.iter().map(FreeEntry::key).collect();
        self.writer.acknowledge(token, caller, claim, &keys);
        self.run();
        match self.answer(token) {
            Answer::Acked(result) => result,
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    /// Lists a page of the free queue.
    pub fn list_freed(
        &mut self,
        caller: Caller,
        after: Option<(Version, Freed)>,
        limit: usize,
    ) -> FreePage {
        let token = self.token();
        self.writer.list_freed(token, caller, after, limit);
        self.run();
        match self.answer(token) {
            Answer::FreeQueue {
                version,
                entries,
                next,
                freed_through,
            } => FreePage {
                version,
                entries,
                next,
                freed_through,
            },
            other => panic!("expected a page of the free queue, got {other:?}"),
        }
    }

    pub fn query_as(&mut self, caller: Caller, query: Query) -> Answer {
        let token = self.next_token;
        self.next_token += 1;
        self.writer.query(token, caller, query, 0);
        self.run();
        self.answers.remove(&token).expect("the writer answered")
    }

    pub fn commit(&mut self, batch: Batch) -> CommitResult {
        match self.submit(batch) {
            Answer::Commit(result) => result,
            other => panic!("expected a commit, got {other:?}"),
        }
    }

    /// Runs ops as one batch.
    pub fn ops(&mut self, ops: impl IntoIterator<Item = OpSpec>) -> CommitResult {
        self.commit(Batch {
            ops: ops.into_iter().collect(),
            ..Batch::default()
        })
    }

    pub fn query(&mut self, query: Query) -> (Version, QueryResult) {
        match self.query_as(Caller::Trusted(self.root), query) {
            Answer::Query { version, result } => (version, result),
            other => panic!("expected a query answer, got {other:?}"),
        }
    }

    pub fn stat(&mut self, path: &str) -> QueryResult {
        self.query(Query::Stat(Target::Path(p(path)))).1
    }

    pub fn fsck(&self) -> Vec<String> {
        let dump = self.storage.dump();
        let log = self.storage.log_now(0, usize::MAX);
        fsck::check_with_log(
            dump.iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
            log.iter()
                .map(|(version, bytes)| (*version, bytes.as_slice())),
        )
    }
}

/// The hash the harness gives a key.
pub fn key_hash(id: KeyId) -> [u8; 32] {
    let mut hash = [0; 32];
    for chunk in hash.chunks_mut(8) {
        chunk.copy_from_slice(&id.to_be_bytes());
    }
    hash
}

pub fn p(text: &str) -> Path {
    text.parse().expect("a valid path")
}

pub fn mkdir(path: &str) -> OpSpec {
    OpSpec::new(Op::Mkdir {
        path: p(path),
        parents: false,
        if_absent: false,
    })
}

pub fn mkdir_p(path: &str) -> OpSpec {
    OpSpec::new(Op::Mkdir {
        path: p(path),
        parents: true,
        if_absent: true,
    })
}

pub fn put(path: &str, value: &str) -> OpSpec {
    put_mode(path, value, PutMode::Upsert)
}

pub fn put_mode(path: &str, value: &str, mode: PutMode) -> OpSpec {
    OpSpec::new(Op::Put {
        path: p(path),
        value: Bytes::copy_from_slice(value.as_bytes()),
        mode,
        mtime: None,
    })
}

pub fn link(from: &str, to: &str) -> OpSpec {
    OpSpec::new(Op::Link {
        from: p(from),
        to: p(to),
    })
}

pub fn unlink(path: &str) -> OpSpec {
    OpSpec::new(Op::Unlink {
        path: p(path),
        if_exists: false,
    })
}

pub fn rmdir(path: &str) -> OpSpec {
    OpSpec::new(Op::Rmdir {
        path: p(path),
        if_exists: false,
    })
}

pub fn remove_all(path: &str) -> OpSpec {
    OpSpec::new(Op::RemoveAll {
        path: p(path),
        if_exists: false,
    })
}

pub fn rename(from: &str, to: &str, flag: RenameFlag) -> OpSpec {
    OpSpec::new(Op::Rename {
        from: p(from),
        to: p(to),
        flag,
    })
}

pub fn mv(from: &str, to: &str) -> OpSpec {
    rename(from, to, RenameFlag::Replace)
}

pub fn set_mtime(path: &str, mtime: Millis) -> OpSpec {
    OpSpec::new(Op::SetAttr {
        path: p(path),
        mtime: Some(mtime),
        mode: None,
    })
}

pub fn version_is(path: &str, version: Version) -> Pred {
    Pred::VersionIs(p(path), version)
}
