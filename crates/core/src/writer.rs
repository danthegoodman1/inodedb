//! The writer: runs batches in arrival order, commits them in groups, and
//! answers reads at its committed version.
//!
//! One storage transaction is in flight at a time, since storage orders no
//! two independent transactions and a batch must never commit without the
//! batches it built on. While one commits, the writer runs the batches that
//! arrive against the cache plus every change not yet committed, and
//! gathers them into the next group. A batch hears back once its group
//! commits; a batch that changed nothing waits for every group it was run
//! against, since its outcomes depend on their changes.
//!
//! Every request runs under a caller's key, checked when it runs, so a
//! revocation stops the requests that run after it.
//!
//! A request id answers once: a committed batch's answer is stored with
//! it, and a retry is answered from that record until it expires; the
//! answer of a batch that changed nothing is kept in memory as long.

use crate::auth::{self, Caller, Denied, Grant, KeyError, KeyId, KeyOp, KeyRecord, Keys, Scope};
use crate::cache::{Budget, Cache, rebase};
use crate::codec::{Dec, DecodeError, Enc};
use crate::delta::{Delta, Freed};
use crate::exec::{self, Input};
use crate::keys::{self, KvRead, KvResult, Pairs, Parsed, WriteSet};
use crate::lease::{
    ClaimError, ClaimId, FreeEntry, Granted, LeaseError, LeaseId, LeaseRecord, Leases, Release,
    StoredLease,
};
use crate::log::{Chains, LogEntry};
use crate::ops::{Batch, BatchError, CommitResult, Recorded};
use crate::overlay::{Overlaid, Overlay};
use crate::preload::Cursor;
use crate::query::{self, Query, QueryResult};
use crate::types::{Ino, Inode, MAX_BATCH_BYTES, Millis, ROOT, Version};
use crate::view::{Read, View};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

/// The owner's name for a request it handed the writer.
pub type Token = u64;

/// The most a request-record value holds; longer results span keys.
const REQUEST_PART: usize = 64 * 1024;
/// The most expired request records one group deletes.
const EXPIRE_PER_GROUP: usize = 256;
/// The most a group's batches write, as FoundationDB charges a transaction
/// for them (`WriteSet::charged`). With the head, the group's expiry
/// deletions and `LEASE_BYTES` of heartbeats, a group stays under a storage
/// transaction's 10,000,000 bytes.
pub const MAX_GROUP_BYTES: usize = 9_000_000;
/// The most lease records one commit writes; the rest go with later ones.
const LEASE_BYTES: usize = 512 * 1024;
/// The most ops the writer runs ahead of a batch waiting on reads, to find
/// what the batches behind it lack: a queue of small batches fits, and
/// batches that hit the cache cost each round of reads a bounded stall.
const READ_AHEAD_OPS: usize = 1_024;

#[derive(Clone, Debug)]
pub struct Config {
    /// A group closes once what its writes are charged would pass this,
    /// at most `MAX_GROUP_BYTES`.
    pub group_max_bytes: usize,
    /// How long a request record lasts.
    pub request_ttl: Millis,
    /// What the writer's cache holds.
    pub cache: Budget,
    /// Whether the writer reads the tree into its cache once it starts, up
    /// to the cache's budget.
    pub preload: bool,
    /// How long a lease lasts past its last heartbeat.
    pub lease_ttl: Millis,
    /// How long a claim lasts unless acknowledged or renewed.
    pub claim_ttl: Millis,
    /// How long the writer answers reads after the start of its last
    /// successful commit. A standby takes over once the head record has
    /// gone unchanged this long plus `takeover_margin`.
    pub writer_lease: Millis,
    /// How often an idle writer commits to renew its lease.
    pub renew_every: Millis,
    /// What a standby waits beyond `writer_lease`, which covers clock rates
    /// that drift apart over one lease.
    pub takeover_margin: Millis,
    /// Where clients reach this writer; standbys name it in `NotWriter`.
    pub address: String,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            group_max_bytes: MAX_GROUP_BYTES,
            request_ttl: 10 * 60 * 1000,
            cache: Budget::default(),
            preload: false,
            lease_ttl: 30 * 1000,
            claim_ttl: 60 * 1000,
            writer_lease: 5 * 1000,
            renew_every: 1000,
            takeover_margin: 1000,
            address: String::new(),
        }
    }
}

impl Config {
    /// How long a standby must see the head record unchanged before it
    /// takes over.
    pub fn takeover_wait(&self) -> Millis {
        self.writer_lease + self.takeover_margin
    }
}

/// Free-queue entries one read of a claim's scan, or of a listing's page,
/// covers.
const CLAIM_PAGE: usize = 1024;

/// The head record, which is also the writer record: the last committed
/// version and the next inode id, and the writer holding the store. Every
/// commit checks it and rewrites it, so a writer's commits conflict once
/// another writer's commit lands, and a standby that sees it unchanged
/// knows nothing has committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    pub version: Version,
    pub next_ino: Ino,
    /// The writer's id, unique among writers of one store.
    pub writer: u64,
    /// Counts the writers that have held the store.
    pub epoch: u64,
    /// Counts the writer's commits, so each one changes the record.
    pub beat: u64,
    /// Where clients reach the writer.
    pub address: String,
    /// The writer gave up the store on a clean shutdown, so a standby takes
    /// over without waiting out the lease.
    pub released: bool,
}

impl Head {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(48 + self.address.len());
        enc.varint(self.version)
            .varint(self.next_ino)
            .varint(self.writer)
            .varint(self.epoch)
            .varint(self.beat)
            .bytes(self.address.as_bytes());
        // A head its writer holds ends at the address, as before releases.
        if self.released {
            enc.varint(1);
        }
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Head, DecodeError> {
        let mut dec = Dec::new(bytes);
        let head = Head {
            version: dec.varint()?,
            next_ino: dec.varint()?,
            writer: dec.varint()?,
            epoch: dec.varint()?,
            beat: dec.varint()?,
            address: String::from_utf8(dec.bytes()?.to_vec()).map_err(|_| DecodeError)?,
            released: !dec.is_empty() && dec.varint()? == 1,
        };
        dec.end()?;
        Ok(head)
    }

    /// The head after one more commit by its writer.
    fn next(&self, version: Version, next_ino: Ino) -> Head {
        Head {
            version,
            next_ino,
            beat: self.beat + 1,
            ..self.clone()
        }
    }
}

/// What a starting writer may take over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Takeover {
    /// Whatever writer the head names, which a lock on the store has
    /// already stopped.
    Now,
    /// The head record a standby saw go unchanged for `takeover_wait`, or
    /// None for an empty store; the writer takes over only from it.
    From(Option<Vec<u8>>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    Commit(CommitResult),
    Rejected(BatchError),
    Query {
        version: Version,
        result: QueryResult,
    },
    /// The writer has stopped: a newer writer took over, or storage held
    /// something it can't read.
    Unavailable,
    /// A query asked for a version past the writer's committed one.
    NotYet {
        version: Version,
    },
    /// A standby takes only reads at a version a client names; the writer
    /// at `address` takes the rest.
    NotWriter {
        address: String,
    },
    Denied(Denied),
    /// A key op committed at a version, or failed. A mint's key takes the
    /// version as its id, unless a trusted caller named one.
    Key(Result<Version, KeyError>),
    Lease(Result<Granted, LeaseError>),
    /// Released entries a consumer claimed; no entries, and claim zero,
    /// when none are released under its root.
    Claimed {
        claim: ClaimId,
        entries: Vec<FreeEntry>,
    },
    /// Acknowledged entries are gone as of a version.
    Acked(Result<Version, ClaimError>),
    /// A page of the free queue under the caller's root, read at `version`
    /// or later. `next` is the last entry the page covers, None once it
    /// reached the queue's end. Every subtree under the root detached at or
    /// before `freed_through` has all its contents in the queue as of the
    /// page.
    FreeQueue {
        version: Version,
        entries: Vec<FreeEntry>,
        next: Option<(Version, Freed)>,
        freed_through: Version,
    },
}

/// How a commit ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed,
    /// The transaction never applied, and never will.
    Failed,
    /// The transaction may or may not have applied.
    Unknown,
    /// Head held something other than the writer expected: another writer
    /// committed.
    Fenced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Read from one snapshot. The first read is always the head record.
    Read {
        id: u64,
        reads: Vec<KvRead>,
    },
    /// Commit one transaction that first checks head holds `expect`
    /// (absent for None).
    Commit {
        id: u64,
        expect: Option<Vec<u8>>,
        writes: WriteSet,
    },
    /// Learn whether an in-doubt commit applied: read the head record and
    /// write it back in one transaction, which also keeps the in-doubt one
    /// from ever committing later.
    Resolve {
        id: u64,
    },
    Answer {
        token: Token,
        answer: Answer,
    },
}

#[derive(Clone, Debug)]
enum Work {
    Batch(Batch),
    Key(KeyOp),
    /// Opens a lease under a reserved id.
    Lease(LeaseId, Option<LeaseRecord>),
    /// Deletes acknowledged entries and what they kept.
    Ack(Vec<FreeEntry>),
    /// Frees part of the subtrees detached at these versions under these
    /// directories, in order.
    Reap(Vec<(Version, Ino)>),
    /// Deletes these keys if they have expired, with what they minted.
    Expire(Vec<KeyId>),
}

/// The tokens of the writer's own work, whose answers go to no one: the
/// freeing of detached subtrees, and the deletion of expired keys.
const INTERNAL: Token = Token::MAX;
const EXPIRY: Token = Token::MAX - 1;

/// A request in the writer's hands. Its work is shared, so holding it for
/// its answer, or to run again after a failed group, copies nothing.
#[derive(Clone, Debug)]
struct Submitted {
    seq: u64,
    token: Token,
    caller: Caller,
    work: Arc<Work>,
}

/// A batch run and waiting for its group to commit.
#[derive(Debug)]
struct Prepared {
    sub: Submitted,
    delta: Arc<Delta>,
    version: Version,
    answer: Answer,
    writes: WriteSet,
    /// When the batch's request record expires, and how many parts it has,
    /// if it writes one.
    expires: Option<(Millis, u16)>,
}

#[derive(Debug)]
struct Group {
    id: u64,
    batches: Vec<Prepared>,
    next_ino: Ino,
    /// Expired request records the group deletes.
    expiring: Vec<(Millis, u128)>,
    /// Lease records the group writes, or deletes.
    leases: BTreeMap<LeaseId, Option<StoredLease>>,
    /// The release record the group writes.
    release: Option<Release>,
}

/// A commit of records alone, sent when no group is ready: heartbeats and
/// lease endings, and the release record.
#[derive(Debug)]
struct Chore {
    id: u64,
    leases: BTreeMap<LeaseId, Option<StoredLease>>,
    release: Option<Release>,
}

/// The transaction in flight: the head it writes, the head it expects,
/// and when it went out.
#[derive(Debug)]
struct Sending {
    head: Head,
    expect: Option<Vec<u8>>,
    at: Millis,
}

/// The least time between commits that carry heartbeats alone.
const CHORE_EVERY: Millis = 1_000;

/// The most time detached subtrees wait for their next freeing while the
/// writer is busy.
const REAP_EVERY: Millis = 100;

/// The least time between deletions of expired keys, unless the last one
/// left more behind.
const EXPIRE_KEYS_EVERY: Millis = 1_000;

/// The most expired keys one deletion takes, beyond what they minted.
pub const EXPIRE_KEYS_BUDGET: usize = 1_000;

/// An answer held until the writer has committed `until`.
#[derive(Debug)]
struct Held {
    until: Version,
    sub: Submitted,
    answer: Answer,
}

#[derive(Debug)]
enum Purpose {
    Startup,
    Batch,
    Query(u64),
    Claim(u64),
    /// A page of the free queue for a listing.
    List(u64),
    /// A page of the tree read into the cache.
    Preload(Cursor),
}

/// The detached subtrees still to free.
#[derive(Default)]
struct Detached {
    /// Each by the version that detached it and its top, with the
    /// directories above the top as it detached.
    subtrees: BTreeMap<(Version, Ino), Vec<Ino>>,
    /// How many of them lie under each directory, by the version that
    /// detached them, so the oldest under a directory takes one lookup
    /// however many wait.
    under: BTreeMap<(Ino, Version), usize>,
}

impl Detached {
    fn insert(&mut self, at: Version, top: Ino, above: Vec<Ino>) {
        self.remove(&(at, top));
        for dir in &above {
            *self.under.entry((*dir, at)).or_default() += 1;
        }
        self.subtrees.insert((at, top), above);
    }

    fn remove(&mut self, subtree: &(Version, Ino)) {
        let Some(above) = self.subtrees.remove(subtree) else {
            return;
        };
        for dir in above {
            let key = (dir, subtree.0);
            if let Some(count) = self.under.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    self.under.remove(&key);
                }
            }
        }
    }

    /// The version that detached the oldest subtree waiting under `root`.
    fn oldest_under(&self, root: Ino) -> Option<Version> {
        if root == ROOT {
            return self.subtrees.keys().next().map(|(at, _)| *at);
        }
        self.under
            .range((root, 0)..=(root, Version::MAX))
            .next()
            .map(|((_, at), _)| *at)
    }
}

/// A claim scanning the free queue.
#[derive(Debug)]
struct WaitingClaim {
    token: Token,
    root: Ino,
    limit: usize,
    /// The last key scanned.
    after: Option<Vec<u8>>,
    found: Vec<FreeEntry>,
    /// Whether its scan has begun; it waits until storage holds the
    /// horizon.
    scanning: bool,
    /// Whether the scan has gone back to the queue's start, for entries
    /// behind where it began.
    wrapped: bool,
    /// Whether the scan reads the root's own part of the queue, or the
    /// whole queue for the entries under the root.
    own: bool,
    /// The entries a page of the root's part named, whose records the
    /// claim is reading from the queue, and whether that page was full.
    naming: Option<(Vec<(Version, Freed)>, bool)>,
}

/// A listing of the free queue awaiting its page.
#[derive(Debug)]
struct WaitingList {
    token: Token,
    root: Ino,
    limit: usize,
    /// The committed version when the page's read went out.
    version: Version,
    freed_through: Version,
    /// Whether the page starts at the queue's start, and whether it reads
    /// the root's own part of the queue.
    from_start: bool,
    own: bool,
    /// The entries the page of the root's part named, whose records the
    /// listing is reading from the queue, and where the next page starts.
    naming: Option<Named>,
}

/// Entries a page named, and the last of those the page covers unless it
/// reached the queue's end.
type Named = (Vec<(Version, Freed)>, Option<(Version, Freed)>);

#[derive(Debug)]
struct Outstanding {
    issued_at: Version,
    reads: Vec<Read>,
    purpose: Purpose,
}

#[derive(Debug)]
struct WaitingQuery {
    token: Token,
    caller: Caller,
    query: Query,
    /// The query waits until the writer has committed this version.
    at_least: Version,
    /// The cache's mark when the query first needed a read.
    since: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Starting,
    /// Committing the head that names this writer, after the root's
    /// creation in an empty store.
    Claiming,
    Ready,
    Stopped,
}

pub struct Writer {
    config: Config,
    /// This writer's id, unique among writers of one store.
    writer_id: u64,
    phase: Phase,
    /// The writer is giving up the store, and the id of the commit that
    /// marks its head released, once sent.
    releasing: Option<Option<u64>>,
    /// Where the preload's next page starts, once its last page is read.
    preload: Option<Cursor>,
    /// The directories the preload knows of: the root, and those its pages
    /// of entries name.
    preload_dirs: BTreeSet<Ino>,
    now: Millis,
    cache: Cache,
    /// The committed version, which the cache reflects.
    version: Version,
    /// The head record as of the committed version.
    head: Head,
    next_version: Version,
    next_ino: Ino,
    overlay: Overlay,
    queue: VecDeque<Submitted>,
    /// The read the batch at the queue's front awaits.
    blocked: Option<u64>,
    /// The cache's mark when the batch at the queue's front first needed a
    /// read; eviction spares what came after.
    blocked_since: Option<u64>,
    forming: Vec<Prepared>,
    forming_bytes: usize,
    groups: VecDeque<Group>,
    in_flight: Option<u64>,
    sending: Option<Sending>,
    /// When the last commit that applied went out. The writer answers
    /// reads until `writer_lease` after it.
    renewed_at: Option<Millis>,
    takeover: Takeover,
    /// A standby's cache, and the version it reflects, to start from if
    /// the store is still there.
    warm: Option<(Cache, Version)>,
    bootstrap: Option<Arc<Delta>>,
    held: Vec<Held>,
    reads: BTreeMap<u64, Outstanding>,
    /// Queries that wait for the writer to renew its lease.
    stale: Vec<u64>,
    /// Committed deltas that outstanding reads may straddle.
    acked: VecDeque<Arc<Delta>>,
    queries: BTreeMap<u64, WaitingQuery>,
    /// How many parts each committed request record has, for the few with
    /// more than one.
    long_records: BTreeMap<u128, u16>,
    /// Every committed request record, by when it expires.
    expiries: BTreeSet<(Millis, u128)>,
    /// No record expires before this: a request lifetime past the
    /// writer's start, so the records it found, stamped by other writers'
    /// clocks, each live a whole lifetime on its own. Its own records
    /// expire later anyway.
    expiring_from: Millis,
    /// Answers of batches with request ids that changed nothing, with when
    /// each expires, so a retry gets the first answer while this writer
    /// runs. They write no record, so a restarted writer runs them again.
    unchanged: BTreeMap<u128, (Millis, Recorded)>,
    unchanged_expiries: BTreeSet<(Millis, u128)>,
    /// The secret handles' MACs are made under, and whether storage holds
    /// it yet.
    handle_secret: [u8; 32],
    secret_stored: bool,
    leases: Leases,
    claiming: BTreeMap<u64, WaitingClaim>,
    listing: BTreeMap<u64, WaitingList>,
    detached: Detached,
    reaping: bool,
    /// A deletion of expired keys is in the writer's hands.
    expiring_keys: bool,
    /// When the last deletion of expired keys went into the queue, and
    /// whether it took a whole budget, leaving more behind.
    keys_expired_at: Millis,
    keys_left: bool,
    /// Ops run ahead of a waiting batch, for tests and metrics.
    read_ahead_ops: u64,
    /// When a freeing of a detached subtree last went into the queue.
    reaped_at: Millis,
    chore: Option<Chore>,
    /// When the last chore went out.
    chored_at: Millis,
    /// Where each consumer root's next claim scan begins: a key, and
    /// whether it lies in the root's own part of the queue or in the
    /// whole queue.
    claim_cursors: BTreeMap<Ino, (bool, Vec<u8>)>,
    /// The version from which every free-queue entry has its keys under
    /// the directories above it, once storage holds the record.
    indexed_from: Option<Version>,
    /// Whether the queue holds only such entries, so a consumer under a
    /// directory reads that directory's part of the queue alone. A store
    /// an earlier writer filled gets there once its older entries go.
    queue_indexed: bool,
    next_id: u64,
    next_seq: u64,
    actions: Vec<Action>,
    /// Batches have joined the queue since the last `send`.
    send_due: bool,
}

impl Writer {
    /// A writer that loads its head before it serves anything. `id` must
    /// differ from every earlier writer's over the same store. A store
    /// without a handle secret gets `secret`, which must be random.
    pub fn new(config: Config, id: u64, now: Millis, secret: [u8; 32]) -> Writer {
        let budget = config.cache;
        let leases = Leases::new(config.lease_ttl, config.claim_ttl, id.rotate_left(32) | 1);
        let mut writer = Writer {
            config,
            writer_id: id,
            phase: Phase::Starting,
            releasing: None,
            preload: None,
            preload_dirs: BTreeSet::new(),
            now,
            cache: Cache::with_budget(budget),
            version: 0,
            head: Head {
                version: 0,
                next_ino: ROOT + 1,
                writer: id,
                epoch: 0,
                beat: 0,
                address: String::new(),
                released: false,
            },
            next_version: 1,
            next_ino: ROOT + 1,
            overlay: Overlay::default(),
            queue: VecDeque::new(),
            blocked: None,
            blocked_since: None,
            forming: Vec::new(),
            forming_bytes: 0,
            groups: VecDeque::new(),
            in_flight: None,
            sending: None,
            renewed_at: None,
            takeover: Takeover::Now,
            warm: None,
            bootstrap: None,
            held: Vec::new(),
            reads: BTreeMap::new(),
            stale: Vec::new(),
            acked: VecDeque::new(),
            queries: BTreeMap::new(),
            long_records: BTreeMap::new(),
            expiries: BTreeSet::new(),
            expiring_from: 0,
            unchanged: BTreeMap::new(),
            unchanged_expiries: BTreeSet::new(),
            handle_secret: secret,
            secret_stored: false,
            leases,
            claiming: BTreeMap::new(),
            listing: BTreeMap::new(),
            detached: Detached::default(),
            reaping: false,
            expiring_keys: false,
            keys_expired_at: 0,
            keys_left: false,
            read_ahead_ops: 0,
            reaped_at: 0,
            chore: None,
            chored_at: now,
            claim_cursors: BTreeMap::new(),
            indexed_from: None,
            queue_indexed: false,
            next_id: 1,
            next_seq: 0,
            actions: Vec::new(),
            send_due: false,
        };
        let id = writer.id();
        writer.reads.insert(
            id,
            Outstanding {
                issued_at: 0,
                reads: Vec::new(),
                purpose: Purpose::Startup,
            },
        );
        writer.actions.push(Action::Read {
            id,
            reads: startup_reads(),
        });
        writer
    }

    /// Takes over only from the head record a standby saw go unchanged.
    pub fn after(mut self, takeover: Takeover) -> Writer {
        self.takeover = takeover;
        self
    }

    /// Starts from a standby's cache at `version`, if storage's head is
    /// still there when the writer starts.
    pub fn warm(mut self, cache: Cache, version: Version) -> Writer {
        self.warm = Some((cache, version));
        self
    }

    pub fn version(&self) -> Version {
        self.version
    }

    /// The head record as of the committed version.
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// Whether the writer's lease runs: it answers reads only then, since
    /// a successor may take over once it lapses.
    pub fn is_fresh(&self) -> bool {
        self.releasing.is_none()
            && self
                .renewed_at
                .is_some_and(|at| self.now < at + self.config.writer_lease)
    }

    /// Gives up the store on a clean shutdown. The writer answers what
    /// waits, and whatever arrives, with `Unavailable`, lets the commit in
    /// flight finish, then commits its head marked released, so a standby
    /// takes over without waiting out the lease. It stops once that commit
    /// returns, whatever the outcome.
    pub fn release(&mut self) {
        if self.phase != Phase::Ready {
            return self.stop();
        }
        if self.releasing.is_some() {
            return;
        }
        self.releasing = Some(None);
        let mut tokens: Vec<Token> = std::mem::take(&mut self.queue)
            .iter()
            .map(|sub| sub.token)
            .collect();
        tokens.extend(
            std::mem::take(&mut self.queries)
                .values()
                .map(|query| query.token),
        );
        tokens.extend(
            std::mem::take(&mut self.claiming)
                .values()
                .map(|claim| claim.token),
        );
        tokens.extend(
            std::mem::take(&mut self.listing)
                .values()
                .map(|list| list.token),
        );
        for token in tokens {
            self.answer(token, Answer::Unavailable);
        }
        self.send();
    }

    pub fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    pub fn is_stopped(&self) -> bool {
        self.phase == Phase::Stopped
    }

    pub fn set_now(&mut self, now: Millis) {
        self.now = self.now.max(now);
        self.leases.expire(self.now);
        self.leases.advance(self.version);
    }

    /// Lets the writer act on the time alone: renew its lease, commit
    /// heartbeats, free detached subtrees.
    pub fn tick(&mut self) {
        self.send();
    }

    /// The release horizon: entries freed at or before it are claimable.
    pub fn horizon(&self) -> Version {
        self.leases.horizon()
    }

    /// The lowest changefeed position a live lease reports, which bounds
    /// log truncation.
    pub fn lease_position(&self) -> Option<Version> {
        self.leases.min_position()
    }

    pub fn leases(&self) -> &Leases {
        &self.leases
    }

    /// Opens a lease, or with no record one at the committed version,
    /// which the release horizon never passes; the answer comes once the
    /// opening commits.
    pub fn open_lease(&mut self, token: Token, caller: Caller, record: Option<LeaseRecord>) {
        // Ids come after the stored ones, which a starting writer has yet
        // to load.
        if self.phase != Phase::Ready {
            return self.answer(token, Answer::Unavailable);
        }
        let id = self.leases.reserve();
        self.enqueue(token, caller, Work::Lease(id, record));
    }

    pub fn beat_lease(
        &mut self,
        token: Token,
        caller: Caller,
        lease: LeaseId,
        record: LeaseRecord,
    ) {
        // A starting writer has yet to load the leases, and one whose own
        // lease lapsed may have a successor.
        if self.phase != Phase::Ready || !self.is_fresh() {
            return self.answer(token, Answer::Unavailable);
        }
        let answer = match self.authorize(&caller, Grant::Read) {
            Ok(scope) => {
                let beat = self.leases.beat(lease, record, scope.key, self.now);
                self.leases.advance(self.version);
                Answer::Lease(beat)
            }
            Err(denied) => Answer::Denied(denied),
        };
        self.answer(token, answer);
        self.send();
    }

    /// Ends a lease, which answers as expired.
    pub fn release_lease(&mut self, token: Token, caller: Caller, lease: LeaseId) {
        if self.phase != Phase::Ready || !self.is_fresh() {
            return self.answer(token, Answer::Unavailable);
        }
        let answer = match self.authorize(&caller, Grant::Read) {
            Ok(scope) => {
                self.leases.release_lease(lease, scope.key);
                self.leases.advance(self.version);
                Answer::Lease(Err(LeaseError::Expired))
            }
            Err(denied) => Answer::Denied(denied),
        };
        self.answer(token, answer);
        self.send();
    }

    /// Claims up to `limit` released entries under the caller's root.
    pub fn claim(&mut self, token: Token, caller: Caller, limit: usize) {
        // Claims live with the leases: a starting writer has yet to load
        // the horizon, and one whose lease lapsed may have a successor.
        if self.phase != Phase::Ready || !self.is_fresh() {
            return self.answer(token, Answer::Unavailable);
        }
        let scope = match self.authorize(&caller, Grant::Write) {
            Ok(scope) => scope,
            Err(denied) => return self.answer(token, Answer::Denied(denied)),
        };
        let id = self.id();
        // A claim goes on from where the root's last one stopped, so what
        // it skipped before stays skipped. A cursor into the whole queue
        // says nothing of the root's own part, which the claim then reads
        // from its start.
        let own = self.reads_own(scope.root);
        let after = self
            .claim_cursors
            .get(&scope.root)
            .filter(|(part, _)| *part == own)
            .map(|(_, key)| key.clone());
        self.claiming.insert(
            id,
            WaitingClaim {
                token,
                root: scope.root,
                limit: limit.max(1),
                wrapped: after.is_none(),
                after,
                found: Vec::new(),
                scanning: false,
                own,
                naming: None,
            },
        );
        if self.leases.release_due() {
            self.send();
        } else {
            self.scan_claim(id);
        }
    }

    /// Starts the scans of claims that waited for storage to hold the
    /// horizon, once it does.
    fn scan_waiting(&mut self) {
        if self.leases.release_due() {
            return;
        }
        let waiting: Vec<u64> = self
            .claiming
            .iter()
            .filter(|(_, waiting)| !waiting.scanning)
            .map(|(id, _)| *id)
            .collect();
        for id in waiting {
            self.scan_claim(id);
        }
    }

    fn scan_claim(&mut self, id: u64) {
        let Some(waiting) = self.claiming.get_mut(&id) else {
            return;
        };
        waiting.scanning = true;
        let horizon = self.leases.claim_horizon();
        let range = if waiting.own {
            keys::released_under(waiting.root, waiting.after.as_deref(), horizon)
        } else {
            keys::released(waiting.after.as_deref(), horizon)
        };
        self.reads.insert(
            id,
            Outstanding {
                issued_at: self.version,
                reads: Vec::new(),
                purpose: Purpose::Claim(id),
            },
        );
        self.actions.push(Action::Read {
            id,
            reads: vec![
                KvRead::Get(keys::head()),
                KvRead::Scan {
                    range,
                    limit: CLAIM_PAGE,
                },
            ],
        });
    }

    /// Reads the records of entries a page of a root's part of the queue
    /// named, which hold the directories above what each frees.
    fn read_named(&mut self, id: u64, named: &[(Version, Freed)], purpose: Purpose) {
        let mut reads = Vec::with_capacity(named.len() + 1);
        reads.push(KvRead::Get(keys::head()));
        reads.extend(
            named
                .iter()
                .map(|(version, freed)| KvRead::Get(freed.key(*version))),
        );
        self.reads.insert(
            id,
            Outstanding {
                issued_at: self.version,
                reads: Vec::new(),
                purpose,
            },
        );
        self.actions.push(Action::Read { id, reads });
    }

    fn claim_read(&mut self, id: u64, results: Vec<KvResult>) -> Result<(), DecodeError> {
        let mut results = results.into_iter().skip(1);
        let naming = self
            .claiming
            .get_mut(&id)
            .and_then(|waiting| waiting.naming.take());
        let full = match naming {
            // The records of the entries a page of the root's part named.
            Some((named, full)) => {
                let Some(waiting) = self.claiming.get_mut(&id) else {
                    return Ok(());
                };
                for ((version, freed), result) in named.into_iter().zip(results) {
                    let KvResult::Get(value) = result else {
                        return Err(DecodeError);
                    };
                    // An acknowledgement took the entry as the claim read.
                    let Some(value) = value else {
                        continue;
                    };
                    let entry = FreeEntry {
                        version,
                        freed,
                        ancestors: crate::delta::decode_ancestors(&value)?,
                    };
                    if waiting.found.len() < waiting.limit
                        && self.leases.claimable(waiting.root, &entry)
                    {
                        waiting.found.push(entry);
                    }
                }
                full
            }
            None => {
                let Some(KvResult::Scan(pairs)) = results.next() else {
                    return Err(DecodeError);
                };
                let from_start = self
                    .claiming
                    .get(&id)
                    .is_some_and(|waiting| !waiting.own && waiting.after.is_none());
                if from_start && let Some((key, value)) = pairs.first() {
                    let first = free_entry(key, value)?.version;
                    self.queue_starts(Some(first));
                }
                let Some(waiting) = self.claiming.get_mut(&id) else {
                    return Ok(());
                };
                let mut full = pairs.len() == CLAIM_PAGE;
                if waiting.own {
                    // The root's part holds keys alone: take the entries
                    // free to claim, as many as the claim still lacks, and
                    // read their records. The scan stops at the key that
                    // fills the claim, so the cursor passes no key
                    // unexamined, and the part goes on past it.
                    let mut named = Vec::new();
                    let mut keys = pairs.into_iter();
                    for (key, _) in keys.by_ref() {
                        let entry = part_key(&key)?;
                        waiting.after = Some(key);
                        if !self.leases.is_claimed(&entry)
                            && !waiting.found.iter().any(|found| found.key() == entry)
                        {
                            named.push(entry);
                            if waiting.found.len() + named.len() == waiting.limit {
                                break;
                            }
                        }
                    }
                    full |= keys.next().is_some();
                    if !named.is_empty() {
                        waiting.naming = Some((named.clone(), full));
                        self.read_named(id, &named, Purpose::Claim(id));
                        return Ok(());
                    }
                } else {
                    for (key, value) in pairs {
                        let entry = free_entry(&key, &value)?;
                        waiting.after = Some(key);
                        if waiting.found.len() < waiting.limit
                            && self.leases.claimable(waiting.root, &entry)
                            && !waiting.found.iter().any(|found| found.key() == entry.key())
                        {
                            waiting.found.push(entry);
                        }
                    }
                }
                full
            }
        };
        let Some(waiting) = self.claiming.get_mut(&id) else {
            return Ok(());
        };
        if waiting.found.len() < waiting.limit {
            if full {
                self.scan_claim(id);
                return Ok(());
            }
            if !waiting.wrapped {
                waiting.wrapped = true;
                waiting.after = None;
                self.scan_claim(id);
                return Ok(());
            }
        }
        let waiting = self.claiming.remove(&id).expect("checked");
        // The scan took time, and a writer whose lease lapsed meanwhile may
        // have a successor handing out the same entries; the root's next
        // claim scans them again.
        if !self.is_fresh() {
            self.answer(waiting.token, Answer::Unavailable);
            return Ok(());
        }
        match (&waiting.after, waiting.found.len() == waiting.limit) {
            (Some(after), true) => {
                self.claim_cursors
                    .insert(waiting.root, (waiting.own, after.clone()));
            }
            _ => {
                self.claim_cursors.remove(&waiting.root);
            }
        }
        let answer = if waiting.found.is_empty() {
            Answer::Claimed {
                claim: 0,
                entries: Vec::new(),
            }
        } else {
            let claim = self
                .leases
                .claim(waiting.root, waiting.found.clone(), self.now);
            Answer::Claimed {
                claim,
                entries: waiting.found,
            }
        };
        self.answer(waiting.token, answer);
        Ok(())
    }

    /// Lists up to `limit` free-queue entries under the caller's root after
    /// `after`, whatever the horizon and whether claimed, and claims none.
    /// A listing reads one page of the queue, so it may hold fewer entries
    /// than `limit`, or none, and still go on.
    pub fn list_freed(
        &mut self,
        token: Token,
        caller: Caller,
        after: Option<(Version, Freed)>,
        limit: usize,
    ) {
        if self.phase != Phase::Ready || !self.is_fresh() {
            return self.answer(token, Answer::Unavailable);
        }
        let scope = match self.authorize(&caller, Grant::Write) {
            Ok(scope) => scope,
            Err(denied) => return self.answer(token, Answer::Denied(denied)),
        };
        // From the committed state, which the page's read sees at least: a
        // subtree the writer no longer holds detached has committed every
        // entry its freeing wrote.
        let root = scope.root;
        let freed_through = self
            .detached
            .oldest_under(root)
            .map_or(self.version, |at| at - 1);
        let id = self.id();
        let own = self.reads_own(root);
        self.listing.insert(
            id,
            WaitingList {
                token,
                root,
                limit: limit.max(1),
                version: self.version,
                freed_through,
                from_start: after.is_none(),
                own,
                naming: None,
            },
        );
        self.reads.insert(
            id,
            Outstanding {
                issued_at: self.version,
                reads: Vec::new(),
                purpose: Purpose::List(id),
            },
        );
        let range = if own {
            let after = after.map(|(version, freed)| freed.key_under(root, version));
            keys::free_under_after(root, after.as_deref())
        } else {
            let after = after.map(|(version, freed)| freed.key(version));
            keys::free_after(after.as_deref())
        };
        self.actions.push(Action::Read {
            id,
            reads: vec![
                KvRead::Get(keys::head()),
                KvRead::Scan {
                    range,
                    limit: CLAIM_PAGE,
                },
            ],
        });
    }

    /// Whether a consumer at `root` reads the root's own part of the free
    /// queue. The tree's root reads the queue itself, and so does every
    /// root while the queue holds entries from before it kept its parts.
    fn reads_own(&self, root: Ino) -> bool {
        root != ROOT && self.queue_indexed
    }

    /// Takes what a read from the queue's start found there: the version
    /// that freed its first entry, or None for an empty queue. Once
    /// nothing is left from before the queue kept its parts under
    /// directories, consumers read those parts.
    fn queue_starts(&mut self, first: Option<Version>) {
        if self.queue_indexed {
            return;
        }
        let Some(from) = self.indexed_from else {
            return;
        };
        if first.is_none_or(|first| first > from) {
            self.queue_indexed = true;
        }
    }

    fn list_read(&mut self, id: u64, results: Vec<KvResult>) -> Result<(), DecodeError> {
        let mut results = results.into_iter().skip(1);
        let Some(mut waiting) = self.listing.remove(&id) else {
            return Ok(());
        };
        let (entries, next) = match waiting.naming.take() {
            // The records of the entries the page of the root's part named.
            Some((named, next)) => {
                let mut entries = Vec::with_capacity(named.len());
                for ((version, freed), result) in named.into_iter().zip(results) {
                    let KvResult::Get(value) = result else {
                        return Err(DecodeError);
                    };
                    // An acknowledgement took the entry as the page read.
                    if let Some(value) = value {
                        entries.push(FreeEntry {
                            version,
                            freed,
                            ancestors: crate::delta::decode_ancestors(&value)?,
                        });
                    }
                }
                (entries, next)
            }
            None => {
                let Some(KvResult::Scan(pairs)) = results.next() else {
                    return Err(DecodeError);
                };
                let full = pairs.len() == CLAIM_PAGE;
                if waiting.own {
                    // The root's part holds keys alone, each an entry under
                    // the root: read the records of as many as the page
                    // takes.
                    let named = pairs
                        .iter()
                        .take(waiting.limit)
                        .map(|(key, _)| part_key(key))
                        .collect::<Result<Vec<_>, _>>()?;
                    let next = if named.len() < waiting.limit && !full {
                        None
                    } else {
                        named.last().copied()
                    };
                    if !named.is_empty() {
                        self.read_named(id, &named, Purpose::List(id));
                        waiting.naming = Some((named, next));
                        self.listing.insert(id, waiting);
                        return Ok(());
                    }
                    (Vec::new(), None)
                } else {
                    if waiting.from_start {
                        let first = match pairs.first() {
                            Some((key, value)) => Some(free_entry(key, value)?.version),
                            None => None,
                        };
                        self.queue_starts(first);
                    }
                    let mut entries = Vec::new();
                    let mut next = None;
                    for (key, value) in pairs {
                        let entry = free_entry(&key, &value)?;
                        next = Some(entry.key());
                        if entry.within(waiting.root) {
                            entries.push(entry);
                            if entries.len() == waiting.limit {
                                break;
                            }
                        }
                    }
                    if entries.len() < waiting.limit && !full {
                        next = None;
                    }
                    (entries, next)
                }
            }
        };
        // A writer whose lease lapsed during the read may have a successor.
        let answer = if self.phase == Phase::Ready && self.is_fresh() {
            Answer::FreeQueue {
                version: waiting.version,
                entries,
                next,
                freed_through: waiting.freed_through,
            }
        } else {
            Answer::Unavailable
        };
        self.answer(waiting.token, answer);
        Ok(())
    }

    /// Acknowledges claimed entries, which deletes them and what they kept;
    /// acknowledging none renews the claim.
    pub fn acknowledge(
        &mut self,
        token: Token,
        caller: Caller,
        claim: ClaimId,
        entries: &[(Version, Freed)],
    ) {
        if self.phase != Phase::Ready || !self.is_fresh() {
            return self.answer(token, Answer::Unavailable);
        }
        let scope = match self.authorize(&caller, Grant::Write) {
            Ok(scope) => scope,
            Err(denied) => return self.answer(token, Answer::Denied(denied)),
        };
        match self
            .leases
            .acknowledge(claim, scope.root, entries, self.now)
        {
            Err(error) => self.answer(token, Answer::Acked(Err(error))),
            // Renewal alone, or a retry of what committed.
            Ok(found) if found.is_empty() => {
                let version = self.version;
                self.answer(token, Answer::Acked(Ok(version)));
            }
            Ok(found) => self.enqueue(token, caller, Work::Ack(found)),
        }
    }

    /// The actions to carry out, in order. Batches queued since the last
    /// call go out here as one group, and the cache evicts here, sparing what
    /// reads in progress fetched. An owner may hand over several inputs
    /// between calls.
    pub fn drain(&mut self) -> Vec<Action> {
        if self.send_due {
            self.send();
        }
        self.cache.evict(self.spare());
        std::mem::take(&mut self.actions)
    }

    /// The earliest cache write a read in progress needs.
    fn spare(&self) -> u64 {
        self.queries
            .values()
            .filter_map(|query| query.since)
            .chain(self.blocked_since)
            .min()
            .unwrap_or(u64::MAX)
    }

    fn id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub fn submit(&mut self, token: Token, caller: Caller, batch: Batch) {
        self.enqueue(token, caller, Work::Batch(batch));
    }

    /// Mints or revokes a key, in order with the batches around it.
    pub fn submit_key(&mut self, token: Token, caller: Caller, op: KeyOp) {
        self.enqueue(token, caller, Work::Key(op));
    }

    fn enqueue(&mut self, token: Token, caller: Caller, work: Work) {
        if self.phase == Phase::Stopped {
            self.answer(token, Answer::Unavailable);
            return;
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push_back(Submitted {
            seq,
            token,
            caller,
            work: Arc::new(work),
        });
        // Everything queued before the next `drain` commits together.
        self.run_queue();
        self.send_due = true;
    }

    /// The caller's scope under the committed keys, if it holds `need`.
    pub fn authorize(&self, caller: &Caller, need: Grant) -> Result<Scope, Denied> {
        auth::authorize(&self.cache, caller, self.now, need)
    }

    /// The secret handles' MACs are made under, once the writer is ready.
    pub fn handle_secret(&self) -> [u8; 32] {
        self.handle_secret
    }

    pub fn keys(&self) -> &Keys {
        self.cache.keys()
    }

    /// Answers a query at the committed version, which must be at least
    /// `at_least`. Every version a client sees is committed by then, so a
    /// later one means the client saw a store this writer doesn't hold.
    pub fn query(&mut self, token: Token, caller: Caller, query: Query, at_least: Version) {
        if self.phase == Phase::Stopped {
            self.answer(token, Answer::Unavailable);
            return;
        }
        let id = self.id();
        self.queries.insert(
            id,
            WaitingQuery {
                token,
                caller,
                query,
                at_least,
                since: None,
            },
        );
        if self.phase == Phase::Ready {
            self.run_query(id);
            // A query waiting for the lease needs a renewal on its way.
            if !self.stale.is_empty() {
                self.send();
            }
        }
    }

    /// The answer to a query the cache holds, as of `now`, or None when the
    /// query needs `query`: the writer isn't ready or its lease has lapsed,
    /// the floor is past the committed version, the key is refused, or the
    /// cache lacks a record. It changes nothing, so an owner can run it on
    /// any thread between the writer's other calls. A read answered here
    /// costs no trip to the thread that runs the writer.
    pub fn answer_cached(
        &self,
        caller: &Caller,
        query: &Query,
        at_least: Version,
        now: Millis,
    ) -> Option<Answer> {
        let now = now.max(self.now);
        let fresh = self.releasing.is_none()
            && self
                .renewed_at
                .is_some_and(|at| now < at + self.config.writer_lease);
        if self.phase != Phase::Ready || !fresh || at_least > self.version {
            return None;
        }
        let scope = auth::authorize(&self.cache, caller, now, Grant::Read).ok()?;
        let result = query::run(&self.cache, scope.root, query).ok()?;
        Some(Answer::Query {
            version: self.version,
            result,
        })
    }

    fn answer(&mut self, token: Token, answer: Answer) {
        match token {
            INTERNAL => self.reaping = false,
            EXPIRY => self.expiring_keys = false,
            _ => self.actions.push(Action::Answer { token, answer }),
        }
    }

    /// Detached subtrees still to free, for tests and metrics.
    pub fn detached(&self) -> usize {
        self.detached.subtrees.len()
    }

    /// Whether a consumer under a directory reads that directory's own
    /// part of the free queue, for tests and metrics.
    pub fn queue_indexed(&self) -> bool {
        self.queue_indexed
    }

    /// Live request records of more than one part, for tests and metrics.
    pub fn long_records(&self) -> usize {
        self.long_records.len()
    }

    /// How many directories the writer counts as holding or lying above
    /// a key's root, once a search has found them, for tests and metrics.
    pub fn covered(&self) -> Option<usize> {
        self.cache.covered()
    }

    /// Ops run ahead of batches waiting on reads, for tests and metrics.
    pub fn read_ahead_ops(&self) -> u64 {
        self.read_ahead_ops
    }

    fn issue_read(&mut self, reads: Vec<Read>, purpose: Purpose) -> u64 {
        let id = self.id();
        let mut kv = Vec::with_capacity(reads.len() + 1);
        kv.push(KvRead::Get(keys::head()));
        kv.extend(reads.iter().map(Read::kv));
        self.reads.insert(
            id,
            Outstanding {
                issued_at: self.version,
                reads,
                purpose,
            },
        );
        self.actions.push(Action::Read { id, reads: kv });
        id
    }

    fn run_query(&mut self, id: u64) {
        let Some(waiting) = self.queries.get(&id) else {
            return;
        };
        // Once the lease lapses a successor may commit, so the cache may be
        // behind; the query waits for a renewal.
        if !self.is_fresh() {
            self.stale.push(id);
            return;
        }
        if waiting.at_least > self.version {
            let token = waiting.token;
            self.queries.remove(&id);
            let version = self.version;
            self.answer(token, Answer::NotYet { version });
            return;
        }
        let scope = match self.authorize(&waiting.caller, Grant::Read) {
            Ok(scope) => scope,
            Err(denied) => {
                let token = waiting.token;
                self.queries.remove(&id);
                self.answer(token, Answer::Denied(denied));
                return;
            }
        };
        match query::run(&self.cache, scope.root, &waiting.query) {
            Ok(result) => {
                let token = waiting.token;
                self.queries.remove(&id);
                let version = self.version;
                self.answer(token, Answer::Query { version, result });
            }
            Err(reads) => {
                let mark = self.cache.mark();
                if let Some(waiting) = self.queries.get_mut(&id) {
                    waiting.since.get_or_insert(mark);
                }
                self.issue_read(reads, Purpose::Query(id));
            }
        }
    }

    /// Runs queued batches until one needs a read or the queue empties,
    /// then commits whatever is ready.
    fn pump(&mut self) {
        self.run_queue();
        self.send();
    }

    /// Runs queued batches until one needs a read or the queue empties.
    fn run_queue(&mut self) {
        while self.phase == Phase::Ready && self.blocked.is_none() {
            let Some(sub) = self.queue.pop_front() else {
                break;
            };
            match self.prepare(&sub) {
                Ok(()) => self.blocked_since = None,
                Err(reads) => {
                    self.queue.push_front(sub);
                    let mark = self.cache.mark();
                    self.blocked_since.get_or_insert(mark);
                    let reads = self.read_ahead(reads);
                    let id = self.issue_read(reads, Purpose::Batch);
                    self.blocked = Some(id);
                }
            }
        }
    }

    /// `reads`, which the batch at the queue's front needs, and what the
    /// batches queued behind it lack, up to `READ_AHEAD_OPS`, so a queue of
    /// batches that each miss the cache costs a round of reads rather than
    /// one per batch. Those behind run here against state without the
    /// batches before them, so they may need other reads once those apply;
    /// what they fetch now is current all the same.
    fn read_ahead(&mut self, mut reads: Vec<Read>) -> Vec<Read> {
        let view = Overlaid {
            overlay: &self.overlay,
            base: &self.cache,
        };
        let mut wanted: BTreeSet<Read> = reads.iter().cloned().collect();
        let mut ran = 0;
        for sub in self.queue.iter().skip(1) {
            if wanted.len() >= exec::MAX_READS {
                break;
            }
            let Work::Batch(batch) = &*sub.work else {
                continue;
            };
            let cost = batch.ops.len().max(1);
            if ran + cost > READ_AHEAD_OPS {
                break;
            }
            ran += cost;
            let Ok(scope) = auth::authorize(&view, &sub.caller, self.now, Grant::Write) else {
                continue;
            };
            if batch.validate().is_err() {
                continue;
            }
            let mut needs = Vec::new();
            let request = scoped_request(batch.request_id, scope.key);
            if request != 0
                && let Err(read) = view.request(request)
            {
                needs.push(read);
            }
            let input = Input {
                batch,
                root: scope.root,
                now: self.now,
                version: self.next_version,
                next_ino: self.next_ino,
                ahead: true,
            };
            if let Err(more) = exec::execute(input, &view) {
                needs.extend(more);
            }
            for read in needs {
                if wanted.insert(read.clone()) {
                    reads.push(read);
                }
            }
        }
        self.read_ahead_ops += ran as u64;
        reads
    }

    /// The latest version a batch run now sees.
    fn seen(&self) -> Version {
        self.next_version - 1
    }

    fn prepare(&mut self, sub: &Submitted) -> Result<(), Vec<Read>> {
        let need = match *sub.work {
            Work::Batch(_) | Work::Ack(_) | Work::Reap(..) => Grant::Write,
            Work::Key(_) | Work::Expire(_) => Grant::Admin,
            Work::Lease(..) => Grant::Read,
        };
        let authorized = {
            let view = Overlaid {
                overlay: &self.overlay,
                base: &self.cache,
            };
            auth::authorize(&view, &sub.caller, self.now, need)
        };
        let scope = match authorized {
            Ok(scope) => scope,
            Err(denied) => {
                // The keys it ran against may not commit.
                let seen = self.seen();
                self.hold(seen, sub.clone(), Answer::Denied(denied));
                return Ok(());
            }
        };
        match &*sub.work {
            Work::Batch(batch) => self.prepare_batch(sub, batch, &scope),
            Work::Key(op) => self.prepare_key(sub, op, &scope),
            Work::Lease(id, record) => {
                let record = record.unwrap_or(LeaseRecord {
                    oldest: self.version,
                    position: self.version,
                });
                match self.leases.open(*id, record, scope.key, self.now) {
                    Ok(stored) => {
                        let mut delta = Delta::new(self.next_version);
                        delta.leases.insert(*id, Some(stored));
                        let answer = Answer::Lease(Ok(self.leases.granted(*id)));
                        self.add_change(sub, delta, answer);
                    }
                    Err(error) => {
                        let seen = self.seen();
                        self.hold(seen, sub.clone(), Answer::Lease(Err(error)));
                    }
                }
                Ok(())
            }
            Work::Reap(tops) => {
                let executed = {
                    let view = Overlaid {
                        overlay: &self.overlay,
                        base: &self.cache,
                    };
                    let subtrees: Vec<exec::Reap<'_>> = tops
                        .iter()
                        .map(|key| exec::Reap {
                            top: key.1,
                            at: key.0,
                            above: self
                                .detached
                                .subtrees
                                .get(key)
                                .map_or(&[][..], Vec::as_slice),
                        })
                        .collect();
                    exec::execute_reap(&subtrees, self.now, self.next_version, &view)?
                };
                let version = self.next_version;
                let delta = executed.delta;
                let log =
                    LogEntry::from_delta(&delta, self.now, &executed.holders, executed.chains);
                let mut writes = delta.write_set();
                writes.log.push((version, log.encode()));
                self.add(sub, delta, version, Answer::Unavailable, writes, None);
                Ok(())
            }
            Work::Expire(ids) => {
                let delta = {
                    let view = Overlaid {
                        overlay: &self.overlay,
                        base: &self.cache,
                    };
                    exec::execute_expiry(ids, self.now, self.next_version, &view)
                };
                if delta.is_empty() {
                    // A revocation took them first.
                    let seen = self.seen();
                    self.hold(seen, sub.clone(), Answer::Unavailable);
                } else {
                    self.add_change(sub, delta, Answer::Unavailable);
                }
                Ok(())
            }
            Work::Ack(entries) => {
                let delta = {
                    let view = Overlaid {
                        overlay: &self.overlay,
                        base: &self.cache,
                    };
                    exec::execute_ack(entries, self.next_version, &view)?
                };
                if delta.is_empty() {
                    // Another consumer acknowledged them first.
                    let seen = self.seen();
                    self.leases.acknowledged(entries);
                    self.hold(seen, sub.clone(), Answer::Acked(Ok(seen)));
                } else {
                    let answer = Answer::Acked(Ok(self.next_version));
                    self.add_change(sub, delta, answer);
                }
                Ok(())
            }
        }
    }

    /// Adds a change that touches no paths to the forming group.
    fn add_change(&mut self, sub: &Submitted, delta: Delta, answer: Answer) {
        let version = self.next_version;
        let mut writes = delta.write_set();
        let log = LogEntry::from_delta(&delta, self.now, &BTreeMap::new(), BTreeMap::new());
        writes.log.push((version, log.encode()));
        if too_large(&writes) {
            let seen = self.seen();
            self.hold(seen, sub.clone(), Answer::Rejected(BatchError::TooLarge));
            return;
        }
        self.add(sub, delta, version, answer, writes, None);
    }

    fn prepare_key(&mut self, sub: &Submitted, op: &KeyOp, scope: &Scope) -> Result<(), Vec<Read>> {
        if matches!(op, KeyOp::Mint { .. }) && self.cache.keys().len() >= auth::MAX_KEYS {
            let seen = self.seen();
            self.hold(seen, sub.clone(), Answer::Key(Err(KeyError::TooMany)));
            return Ok(());
        }
        let executed = {
            let view = Overlaid {
                overlay: &self.overlay,
                base: &self.cache,
            };
            exec::execute_key(op, scope, self.now, self.next_version, &view)?
        };
        let delta = match executed {
            Ok(delta) => delta,
            Err(error) => {
                let seen = self.seen();
                self.hold(seen, sub.clone(), Answer::Key(Err(error)));
                return Ok(());
            }
        };
        let version = self.next_version;
        self.add_change(sub, delta, Answer::Key(Ok(version)));
        Ok(())
    }

    /// Adds a run batch to the forming group.
    fn add(
        &mut self,
        sub: &Submitted,
        delta: Delta,
        version: Version,
        answer: Answer,
        writes: WriteSet,
        expires: Option<(Millis, u16)>,
    ) {
        let bytes = writes.charged();
        // The group closes before this batch's inodes count against it.
        if !self.forming.is_empty() && self.forming_bytes + bytes > self.group_max_bytes() {
            self.close_group();
        }
        self.next_version += 1;
        self.overlay.add(&delta);
        self.forming_bytes += bytes;
        self.forming.push(Prepared {
            sub: sub.clone(),
            delta: Arc::new(delta),
            version,
            answer,
            writes,
            expires,
        });
    }

    fn prepare_batch(
        &mut self,
        sub: &Submitted,
        batch: &Batch,
        scope: &Scope,
    ) -> Result<(), Vec<Read>> {
        if let Err(error) = batch.validate() {
            self.answer(sub.token, Answer::Rejected(error));
            return Ok(());
        }
        let id = scoped_request(batch.request_id, scope.key);
        let digest = if id != 0 { batch.digest() } else { 0 };
        if id != 0 {
            self.prune_unchanged();
            if let Some((_, recorded)) = self.unchanged.get(&id) {
                let recorded = recorded.clone();
                self.answer_recorded(sub, recorded, digest);
                return Ok(());
            }
            let view = Overlaid {
                overlay: &self.overlay,
                base: &self.cache,
            };
            match view.request(id) {
                Ok(Some(recorded)) => {
                    self.answer_recorded(sub, recorded, digest);
                    return Ok(());
                }
                Ok(None) => {}
                Err(read) => return Err(vec![read]),
            }
        }
        let executed = {
            let view = Overlaid {
                overlay: &self.overlay,
                base: &self.cache,
            };
            exec::execute(
                Input {
                    batch,
                    root: scope.root,
                    now: self.now,
                    version: self.next_version,
                    next_ino: self.next_ino,
                    ahead: false,
                },
                &view,
            )?
        };
        if !executed.committed {
            let seen = self.seen();
            let result = CommitResult {
                version: seen,
                committed: false,
                outcomes: executed.outcomes,
            };
            if id != 0 {
                let at = self.now + self.config.request_ttl;
                let recorded = Recorded {
                    result: result.clone(),
                    digest: Some(digest),
                };
                self.unchanged.insert(id, (at, recorded));
                self.unchanged_expiries.insert((at, id));
            }
            self.hold(seen, sub.clone(), Answer::Commit(result));
            return Ok(());
        }
        let version = self.next_version;
        let result = CommitResult {
            version,
            committed: true,
            outcomes: executed.outcomes,
        };
        let mut delta = executed.delta;
        let log = LogEntry::from_delta(&delta, self.now, &executed.holders, executed.chains);
        let mut writes = delta.write_set();
        let mut expires = None;
        if id != 0 {
            let recorded = Recorded {
                result: result.clone(),
                digest: Some(digest),
            };
            let bytes = recorded.encode();
            delta.request = Some((id, recorded));
            let mut parts = 0;
            for (part, chunk) in bytes.chunks(REQUEST_PART).enumerate() {
                writes.set(keys::request(id, part as u16), chunk.to_vec());
                parts += 1;
            }
            let at = self.now + self.config.request_ttl;
            writes.set(keys::request_expiry(at, id), keys::encode_parts(parts));
            expires = Some((at, parts));
        }
        writes.log.push((version, log.encode()));
        if too_large(&writes) {
            // Its size depends on the changes it ran against, which may not
            // commit.
            let seen = self.seen();
            self.hold(seen, sub.clone(), Answer::Rejected(BatchError::TooLarge));
            return Ok(());
        }
        self.next_ino = executed.next_ino;
        self.add(sub, delta, version, Answer::Commit(result), writes, expires);
        Ok(())
    }

    /// Answers a batch under an id already recorded: a retry of the batch
    /// that earned the record hears its answer, and a different batch is
    /// refused, each once the record is durable.
    fn answer_recorded(&mut self, sub: &Submitted, recorded: Recorded, digest: u128) {
        let version = recorded.result.version;
        let answer = if recorded.answers(digest) {
            Answer::Commit(recorded.result)
        } else {
            Answer::Rejected(BatchError::Reused)
        };
        self.hold(version, sub.clone(), answer);
    }

    /// A group's size cap, below what one storage transaction holds.
    fn group_max_bytes(&self) -> usize {
        self.config.group_max_bytes.min(MAX_GROUP_BYTES)
    }

    /// Answers once the writer has committed `until`, while its lease
    /// runs, since the answer reflects what the writer has seen.
    fn hold(&mut self, until: Version, sub: Submitted, answer: Answer) {
        if until <= self.version && self.is_fresh() {
            self.answer(sub.token, answer);
        } else {
            self.held.push(Held { until, sub, answer });
        }
    }

    fn close_group(&mut self) {
        if self.forming.is_empty() {
            return;
        }
        let id = self.id();
        let batches = std::mem::take(&mut self.forming);
        self.forming_bytes = 0;
        self.groups.push_back(Group {
            id,
            batches,
            next_ino: self.next_ino,
            expiring: Vec::new(),
            leases: BTreeMap::new(),
            release: None,
        });
    }

    fn send(&mut self) {
        self.send_due = false;
        if self.phase != Phase::Ready {
            return;
        }
        if self.releasing.is_some() {
            if self.in_flight.is_none() {
                self.send_release();
            }
            return;
        }
        // Free part of the detached subtrees when there is nothing else
        // to do, and now and then however busy the writer is. One commit
        // takes them oldest first, as many as its budget frees; each frees
        // an inode at least.
        let idle = self.in_flight.is_none()
            && self.queue.is_empty()
            && self.forming.is_empty()
            && self.groups.is_empty();
        if !self.reaping
            && (idle || self.now >= self.reaped_at + REAP_EVERY)
            && !self.detached.subtrees.is_empty()
        {
            self.reaping = true;
            self.reaped_at = self.now;
            let tops = self
                .detached
                .subtrees
                .keys()
                .take(exec::REAP_BUDGET)
                .copied()
                .collect();
            self.enqueue(INTERNAL, Caller::Trusted(ROOT), Work::Reap(tops));
            return self.send();
        }
        // Delete expired keys once a second, in commits of a bounded
        // size, and at once while a backlog fills each commit.
        if !self.expiring_keys
            && (self.keys_left || self.now >= self.keys_expired_at + EXPIRE_KEYS_EVERY)
        {
            let ids = self.cache.keys().expired(self.now, EXPIRE_KEYS_BUDGET);
            self.keys_left = false;
            if !ids.is_empty() {
                self.expiring_keys = true;
                self.keys_expired_at = self.now;
                self.keys_left = ids.len() == EXPIRE_KEYS_BUDGET;
                self.enqueue(EXPIRY, Caller::Trusted(ROOT), Work::Expire(ids));
                return self.send();
            }
        }
        if self.in_flight.is_some() {
            return;
        }
        if self.groups.is_empty() {
            self.close_group();
        }
        if self.groups.is_empty() {
            self.send_chore();
            return;
        }
        let expiring = self.take_expired();
        let group = self.groups.front().expect("checked");
        let last = group.batches.last().expect("groups hold batches").version;
        let head = self.head.next(last, group.next_ino);
        let group = self.groups.front_mut().expect("checked");
        // Deletions come first, so a record the group writes again stays.
        let mut writes = WriteSet::default();
        for (at, id) in &expiring {
            writes.clear(keys::request_expiry(*at, *id));
            // Each part by its key, so a group's deletions read nothing and
            // leave no range for later reads to consult.
            let parts = self.long_records.get(id).copied().unwrap_or(1);
            for part in 0..parts {
                writes.clear(keys::request(*id, part));
            }
        }
        group.expiring = expiring;
        // A group goes out once: one that fails runs its batches again.
        for prepared in &mut group.batches {
            writes.extend(std::mem::take(&mut prepared.writes));
        }
        // Heartbeats and release reach storage with the next group.
        group.leases = self.leases.take_writes(LEASE_BYTES);
        for (lease, stored) in &group.leases {
            match stored {
                Some(stored) => writes.set(keys::lease(*lease), stored.encode()),
                None => writes.clear(keys::lease(*lease)),
            }
        }
        if self.leases.release_due() {
            let release = self.leases.release();
            writes.set(keys::release(), release.encode());
            group.release = Some(release);
        }
        if !self.secret_stored {
            writes.set(keys::handle_secret(), self.handle_secret.to_vec());
        }
        writes.set(keys::head(), head.encode());
        let id = group.id;
        self.send_commit(id, head, writes);
    }

    /// Sends a transaction that expects the committed head and writes
    /// `head`.
    fn send_commit(&mut self, id: u64, head: Head, writes: WriteSet) {
        let expect = Some(self.head.encode());
        self.in_flight = Some(id);
        self.sending = Some(Sending {
            head,
            expect: expect.clone(),
            at: self.now,
        });
        self.actions.push(Action::Commit { id, expect, writes });
    }

    /// Commits the head marked released, and nothing else.
    fn send_release(&mut self) {
        let head = Head {
            released: true,
            ..self.head.next(self.version, self.head.next_ino)
        };
        let mut writes = WriteSet::default();
        writes.set(keys::head(), head.encode());
        let id = self.id();
        self.releasing = Some(Some(id));
        self.send_commit(id, head, writes);
    }

    /// Commits what no group carries: a renewal of the writer's lease every
    /// `renew_every`, and what heartbeats and release left for storage, at
    /// most every `CHORE_EVERY` or at once for the release record a claim
    /// waits on.
    fn send_chore(&mut self) {
        let due = self.now >= self.chored_at + CHORE_EVERY;
        let waiting = self.claiming.values().any(|waiting| !waiting.scanning);
        let release =
            (self.leases.release_due() && (due || waiting)).then(|| self.leases.release());
        let beats = self.leases.writes_due() && due;
        let renew = self
            .renewed_at
            .is_none_or(|at| self.now >= at + self.config.renew_every);
        if release.is_none() && !beats && !renew {
            return;
        }
        self.chored_at = self.now;
        let leases = self.leases.take_writes(MAX_GROUP_BYTES);
        let mut writes = WriteSet::default();
        for (lease, stored) in &leases {
            match stored {
                Some(stored) => writes.set(keys::lease(*lease), stored.encode()),
                None => writes.clear(keys::lease(*lease)),
            }
        }
        if let Some(release) = release {
            writes.set(keys::release(), release.encode());
        }
        let head = self.head.next(self.version, self.head.next_ino);
        writes.set(keys::head(), head.encode());
        let id = self.id();
        self.chore = Some(Chore {
            id,
            leases,
            release,
        });
        self.send_commit(id, head, writes);
    }

    fn chore_done(&mut self, outcome: CommitOutcome) {
        let chore = self.chore.take().expect("a chore was in flight");
        self.in_flight = None;
        let sending = self.sending.take().expect("a chore was in flight");
        match outcome {
            CommitOutcome::Committed => {
                self.head = sending.head;
                if let Some(release) = chore.release {
                    self.leases.stored(release);
                    self.scan_waiting();
                }
                self.renewed(sending.at);
            }
            // The records are idempotent, so they go again with the next
            // commit.
            CommitOutcome::Failed | CommitOutcome::Unknown => {
                self.leases.restore_writes(chore.leases);
            }
            CommitOutcome::Fenced => return self.stop(),
        }
        self.pump();
    }

    pub fn commit_done(&mut self, id: u64, outcome: CommitOutcome) {
        if self.in_flight != Some(id) {
            return;
        }
        if outcome == CommitOutcome::Unknown {
            return self.actions.push(Action::Resolve { id });
        }
        if self.phase == Phase::Claiming {
            return self.claimed(outcome);
        }
        if self.releasing == Some(Some(id)) {
            return self.stop();
        }
        if self.chore.as_ref().is_some_and(|chore| chore.id == id) {
            return self.chore_done(outcome);
        }
        match outcome {
            CommitOutcome::Committed => self.committed(),
            CommitOutcome::Failed | CommitOutcome::Unknown => self.failed(),
            CommitOutcome::Fenced => self.stop(),
        }
    }

    /// The head record a resolving transaction found, or None if absent:
    /// the head the transaction in doubt writes means it applied, and the
    /// head it expects means it never will.
    pub fn resolve_done(&mut self, id: u64, head: Option<Vec<u8>>) {
        if self.in_flight != Some(id) {
            return;
        }
        let Some(sending) = &self.sending else {
            return;
        };
        let outcome = if head.as_deref() == Some(&sending.head.encode()[..]) {
            CommitOutcome::Committed
        } else if head == sending.expect {
            CommitOutcome::Failed
        } else {
            CommitOutcome::Fenced
        };
        self.commit_done(id, outcome);
    }

    /// After a commit applies: the lease runs from when it went out, and
    /// what waited for it goes ahead.
    fn renewed(&mut self, at: Millis) {
        self.renewed_at = Some(self.renewed_at.map_or(at, |renewed| renewed.max(at)));
        if !self.is_fresh() {
            return;
        }
        for id in std::mem::take(&mut self.stale) {
            self.run_query(id);
        }
        let version = self.version;
        let (ready, held): (Vec<Held>, Vec<Held>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|held| held.until <= version);
        self.held = held;
        for held in ready {
            self.answer(held.sub.token, held.answer);
        }
    }

    fn prune_unchanged(&mut self) {
        while let Some(&(at, id)) = self.unchanged_expiries.first()
            && at <= self.now
        {
            self.unchanged_expiries.pop_first();
            if self
                .unchanged
                .get(&id)
                .is_some_and(|(expires, _)| *expires == at)
            {
                self.unchanged.remove(&id);
            }
        }
    }

    /// Request records past their lifetime that no read is fetching, up
    /// to a group's share. The cache forgets them now, so a retry runs
    /// again rather than reading a record about to go.
    fn take_expired(&mut self) -> Vec<(Millis, u128)> {
        if self.now < self.expiring_from
            || self.expiries.first().is_none_or(|(at, _)| *at > self.now)
        {
            return Vec::new();
        }
        let fetching: BTreeSet<u128> = self
            .reads
            .values()
            .flat_map(|outstanding| &outstanding.reads)
            .filter_map(|read| match read {
                Read::Request(id) => Some(*id),
                _ => None,
            })
            .collect();
        let expired: Vec<(Millis, u128)> = self
            .expiries
            .iter()
            .take_while(|(at, _)| *at <= self.now)
            .filter(|(_, id)| !fetching.contains(id))
            .take(EXPIRE_PER_GROUP)
            .copied()
            .collect();
        for record in &expired {
            self.expiries.remove(record);
            self.cache.expire_request(record.1);
        }
        expired
    }

    fn committed(&mut self) {
        let group = self.groups.pop_front().expect("a group was in flight");
        self.in_flight = None;
        self.secret_stored = true;
        if let Some(release) = group.release {
            self.leases.stored(release);
            self.scan_waiting();
        }
        let keep = !self.reads.is_empty();
        // Ahead of the batches: one of them may write an expired id again.
        for (_, id) in &group.expiring {
            self.long_records.remove(id);
        }
        for prepared in group.batches {
            if let (Some((at, parts)), Some((id, _))) = (prepared.expires, &prepared.delta.request)
            {
                self.expiries.insert((at, *id));
                if parts > 1 {
                    self.long_records.insert(*id, parts);
                }
            }
            self.cache.apply(&prepared.delta);
            self.version = prepared.version;
            if let Work::Ack(entries) = &*prepared.sub.work {
                self.leases.acknowledged(entries);
            }
            for ino in &prepared.delta.detached {
                let above = prepared.delta.above.get(ino).cloned().unwrap_or_default();
                self.detached.insert(prepared.version, *ino, above);
            }
            for record in &prepared.delta.undetached {
                self.detached.remove(record);
            }
            if keep {
                self.acked.push_back(prepared.delta);
            }
            self.answer(prepared.sub.token, prepared.answer);
        }
        self.leases.advance(self.version);
        let sending = self.sending.take().expect("a group was in flight");
        self.head = sending.head;
        self.overlay.prune(self.version);
        self.renewed(sending.at);
        self.prune_acked();
        self.send();
    }

    /// Drops every uncommitted change and runs those batches again, in
    /// arrival order, ahead of the queue.
    fn failed(&mut self) {
        self.in_flight = None;
        self.sending = None;
        let mut requeued: Vec<Submitted> = Vec::new();
        for group in std::mem::take(&mut self.groups) {
            // Their records stay in storage, so a later group deletes them.
            self.expiries.extend(group.expiring);
            self.leases.restore_writes(group.leases);
            requeued.extend(group.batches.into_iter().map(|prepared| prepared.sub));
        }
        requeued.extend(std::mem::take(&mut self.forming).into_iter().map(|p| p.sub));
        self.forming_bytes = 0;
        requeued.extend(
            std::mem::take(&mut self.held)
                .into_iter()
                .map(|held| held.sub),
        );
        requeued.sort_by_key(|sub| sub.seq);
        for sub in requeued.into_iter().rev() {
            self.queue.push_front(sub);
        }
        self.overlay.clear();
        // Answers run against the dropped changes go, as those batches run
        // again.
        let version = self.version;
        self.unchanged
            .retain(|_, (_, recorded)| recorded.result.version <= version);
        self.next_version = self.version + 1;
        self.next_ino = self.head.next_ino;
        self.blocked = None;
        self.blocked_since = None;
        self.pump();
    }

    fn stop(&mut self) {
        self.phase = Phase::Stopped;
        let mut tokens: Vec<Token> = Vec::new();
        for group in std::mem::take(&mut self.groups) {
            tokens.extend(group.batches.iter().map(|prepared| prepared.sub.token));
        }
        tokens.extend(
            std::mem::take(&mut self.forming)
                .iter()
                .map(|p| p.sub.token),
        );
        tokens.extend(
            std::mem::take(&mut self.held)
                .iter()
                .map(|held| held.sub.token),
        );
        tokens.extend(std::mem::take(&mut self.queue).iter().map(|sub| sub.token));
        tokens.extend(
            std::mem::take(&mut self.queries)
                .values()
                .map(|query| query.token),
        );
        tokens.extend(
            std::mem::take(&mut self.claiming)
                .values()
                .map(|claim| claim.token),
        );
        tokens.extend(
            std::mem::take(&mut self.listing)
                .values()
                .map(|list| list.token),
        );
        for token in tokens {
            self.answer(token, Answer::Unavailable);
        }
        self.stale.clear();
        self.in_flight = None;
        self.sending = None;
        self.overlay.clear();
    }

    fn prune_acked(&mut self) {
        let oldest = self.reads.values().map(|read| read.issued_at).min();
        match oldest {
            None => self.acked.clear(),
            Some(oldest) => {
                while self
                    .acked
                    .front()
                    .is_some_and(|delta| delta.version <= oldest)
                {
                    self.acked.pop_front();
                }
            }
        }
    }

    /// Every delta a read might straddle, in version order.
    fn window(&self) -> Vec<&Delta> {
        let mut window: Vec<&Delta> = self.acked.iter().map(|delta| delta.as_ref()).collect();
        for group in &self.groups {
            window.extend(group.batches.iter().map(|prepared| prepared.delta.as_ref()));
        }
        window.extend(self.forming.iter().map(|prepared| prepared.delta.as_ref()));
        window
    }

    /// Results of a snapshot read, in the order the action listed them.
    pub fn read_done(&mut self, id: u64, results: Vec<KvResult>) {
        let Some(outstanding) = self.reads.remove(&id) else {
            return;
        };
        if let Purpose::Claim(claim) = outstanding.purpose {
            if self.claim_read(claim, results).is_err() {
                self.stop();
            }
            return;
        }
        if let Purpose::List(list) = outstanding.purpose {
            if self.list_read(list, results).is_err() {
                self.stop();
            }
            return;
        }
        if let Err(error) = self.absorb(&outstanding, results) {
            let _ = error;
            self.stop();
            return;
        }
        self.prune_acked();
        match outstanding.purpose {
            Purpose::Startup => {}
            Purpose::Batch => {
                if self.blocked == Some(id) {
                    self.blocked = None;
                }
                self.pump();
            }
            Purpose::Query(query) => self.run_query(query),
            Purpose::Claim(_) | Purpose::List(_) => {}
            Purpose::Preload(_) => match self.preload.take() {
                Some(cursor) => self.preload_page(cursor),
                None => self.preload_dirs.clear(),
            },
        }
    }

    /// Reads the preload's next page while the writer serves.
    fn preload_page(&mut self, cursor: Cursor) {
        if self.phase != Phase::Ready || self.releasing.is_some() {
            self.preload_dirs.clear();
            return;
        }
        let id = self.id();
        let read = cursor.read();
        self.reads.insert(
            id,
            Outstanding {
                issued_at: self.version,
                reads: Vec::new(),
                purpose: Purpose::Preload(cursor),
            },
        );
        self.actions.push(Action::Read {
            id,
            reads: vec![KvRead::Get(keys::head()), read],
        });
    }

    fn absorb(
        &mut self,
        outstanding: &Outstanding,
        results: Vec<KvResult>,
    ) -> Result<(), DecodeError> {
        let mut results = results.into_iter();
        let (raw_head, head) = match results.next() {
            Some(KvResult::Get(Some(bytes))) => {
                let head = Head::decode(&bytes)?;
                (Some(bytes), Some(head))
            }
            Some(KvResult::Get(None)) => (None, None),
            _ => return Err(DecodeError),
        };
        if let Purpose::Startup = outstanding.purpose {
            if let Some((cache, version)) = self.warm.take()
                && head.as_ref().is_some_and(|head| head.version == version)
            {
                self.cache = cache;
            }
            let (
                Some(KvResult::Scan(ids)),
                Some(KvResult::Scan(records)),
                Some(KvResult::Get(secret)),
                Some(KvResult::Scan(stored)),
                Some(KvResult::Scan(retired)),
                Some(KvResult::Scan(detached)),
                Some(KvResult::Get(release)),
                Some(KvResult::Get(indexed_from)),
                Some(KvResult::Scan(queue_start)),
            ) = (
                results.next(),
                results.next(),
                results.next(),
                results.next(),
                results.next(),
                results.next(),
                results.next(),
                results.next(),
                results.next(),
            )
            else {
                return Err(DecodeError);
            };
            // A store without the record gets it with this writer's first
            // commit, from the version it starts at: an earlier writer
            // freed what the queue holds, and kept none of it under
            // directories.
            self.indexed_from = indexed_from
                .map(|bytes| decode_indexed_from(&bytes))
                .transpose()?;
            let from = self
                .indexed_from
                .unwrap_or_else(|| head.as_ref().map_or(0, |head| head.version));
            self.queue_indexed = match queue_start.first() {
                Some((key, value)) => free_entry(key, value)?.version > from,
                None => true,
            };
            let prefix = keys::leases().start.len();
            let mut leases = Vec::with_capacity(stored.len());
            for (key, value) in stored {
                let id = key.get(prefix..).ok_or(DecodeError)?;
                let id = u64::from_be_bytes(id.try_into().map_err(|_| DecodeError)?);
                leases.push((id, StoredLease::decode(&value)?));
            }
            let committed = head.as_ref().map_or(0, |head| head.version);
            let release = release.map(|bytes| Release::decode(&bytes)).transpose()?;
            self.leases.load(leases, release, self.now, committed);
            for (key, value) in detached {
                match keys::parse(&key) {
                    Some(Parsed::Detached(version, ino)) => {
                        let above = crate::delta::decode_ancestors(&value)?;
                        self.detached.insert(version, ino, above);
                    }
                    _ => return Err(DecodeError),
                }
            }
            let prefix = keys::key_records().start.len();
            let mut keys = Keys::default();
            for (key, value) in records {
                let id = key.get(prefix..).ok_or(DecodeError)?;
                let id = u64::from_be_bytes(id.try_into().map_err(|_| DecodeError)?);
                keys.put(id, Some(KeyRecord::decode(&value)?));
            }
            for (key, _) in retired {
                let id = key.get(prefix..).ok_or(DecodeError)?;
                keys.retire(u64::from_be_bytes(id.try_into().map_err(|_| DecodeError)?));
            }
            self.cache.load_keys(keys);
            if let Some(secret) = secret {
                self.handle_secret = secret.as_slice().try_into().map_err(|_| DecodeError)?;
                self.secret_stored = true;
            }
            return self.started(raw_head, head, ids);
        }
        let read_at = head.map_or(0, |head| head.version);
        let to = self.version;
        let facts: Vec<_> = match &outstanding.purpose {
            Purpose::Preload(cursor) => {
                let Some(KvResult::Scan(pairs)) = results.next() else {
                    return Err(DecodeError);
                };
                let page = cursor.page(pairs, &self.preload_dirs)?;
                self.preload_dirs.extend(page.dirs);
                self.preload = page.next;
                page.facts
            }
            _ => outstanding
                .reads
                .iter()
                .zip(results)
                .map(|(read, result)| read.fact(result))
                .collect::<Result<_, _>>()?,
        };
        let window = self.window();
        // Rebasing needs every delta between the two versions. A read past
        // every change this writer made saw another writer's commit.
        let (low, high) = (read_at.min(to), read_at.max(to));
        let covered = window
            .iter()
            .filter(|delta| delta.version > low && delta.version <= high)
            .count();
        if covered as u64 != high - low {
            return Err(DecodeError);
        }
        let rebased: Vec<_> = facts
            .into_iter()
            .filter_map(|fact| rebase(fact, read_at, to, &window))
            .collect();
        let preload = matches!(outstanding.purpose, Purpose::Preload(_));
        for fact in rebased {
            // The preload stops at the first fact the budget has no room
            // for, so it never evicts what it read before.
            if preload && !self.cache.has_room(&fact) {
                self.preload = None;
                break;
            }
            self.cache.fill(fact);
        }
        Ok(())
    }

    fn started(
        &mut self,
        stored: Option<Vec<u8>>,
        head: Option<Head>,
        expiries: Pairs,
    ) -> Result<(), DecodeError> {
        let prefix = keys::request_expiry_range().start.len();
        let mut ids = Vec::with_capacity(expiries.len());
        for (key, value) in expiries {
            let at = key.get(prefix..prefix + 8).ok_or(DecodeError)?;
            let id = key.get(prefix + 8..prefix + 24).ok_or(DecodeError)?;
            let at = u64::from_be_bytes(at.try_into().map_err(|_| DecodeError)?);
            let id = u128::from_be_bytes(id.try_into().map_err(|_| DecodeError)?);
            let parts = keys::decode_parts(&value).ok_or(DecodeError)?;
            if parts > 1 {
                self.long_records.insert(id, parts);
            }
            self.expiries.insert((at, id));
            ids.push(id);
        }
        self.expiring_from = self.now + self.config.request_ttl;
        self.cache.load_request_ids(ids);
        // A standby's writer takes over only from the head it saw go
        // unchanged; any other means some writer still runs.
        if let Takeover::From(seen) = &self.takeover
            && *seen != stored
        {
            self.stop();
            return Ok(());
        }
        match head {
            Some(head) => {
                self.version = head.version;
                self.next_version = head.version + 1;
                self.next_ino = head.next_ino;
                let claim = Head {
                    writer: self.writer_id,
                    epoch: head.epoch + 1,
                    beat: 0,
                    address: self.config.address.clone(),
                    released: false,
                    ..head.clone()
                };
                let mut writes = WriteSet::default();
                if self.indexed_from.is_none() {
                    writes.set(keys::free_index(), encode_indexed_from(head.version));
                }
                self.head = head;
                writes.set(keys::head(), claim.encode());
                self.send_claim(claim, stored, writes);
            }
            None => self.start_bootstrap(),
        }
        Ok(())
    }

    /// Commits the head that names this writer, which fences every earlier
    /// one.
    fn send_claim(&mut self, head: Head, expect: Option<Vec<u8>>, writes: WriteSet) {
        self.phase = Phase::Claiming;
        let id = self.id();
        self.in_flight = Some(id);
        self.sending = Some(Sending {
            head,
            expect: expect.clone(),
            at: self.now,
        });
        self.actions.push(Action::Commit { id, expect, writes });
    }

    /// Commits version 1: the root directory.
    fn start_bootstrap(&mut self) {
        let mut delta = Delta::new(1);
        delta.created.insert(ROOT);
        delta.set_inode(ROOT, None, Inode::dir(1, self.now, ROOT));
        let mut writes = delta.write_set();
        writes.log.push((
            1,
            LogEntry::from_delta(
                &delta,
                self.now,
                &BTreeMap::new(),
                BTreeMap::from([(
                    ROOT,
                    Chains {
                        before: Vec::new(),
                        after: vec![ROOT],
                    },
                )]),
            )
            .encode(),
        ));
        let head = Head {
            version: 1,
            next_ino: ROOT + 1,
            writer: self.writer_id,
            epoch: 1,
            beat: 0,
            address: self.config.address.clone(),
            released: false,
        };
        writes.set(keys::head(), head.encode());
        writes.set(keys::handle_secret(), self.handle_secret.to_vec());
        // A new store keeps every entry under its directories.
        writes.set(keys::free_index(), encode_indexed_from(0));
        self.bootstrap = Some(Arc::new(delta));
        self.send_claim(head, None, writes);
    }

    fn claimed(&mut self, outcome: CommitOutcome) {
        self.in_flight = None;
        let sending = self.sending.take().expect("a claim was in flight");
        let bootstrap = self.bootstrap.take();
        match outcome {
            CommitOutcome::Committed => {
                // The commit stored the free queue's index record in a
                // store that lacked it: a new store's from the start, and
                // an older one's from the version this writer began at.
                let from = if bootstrap.is_some() { 0 } else { self.version };
                self.indexed_from.get_or_insert(from);
                if let Some(delta) = bootstrap {
                    self.cache.apply(&delta);
                    self.secret_stored = true;
                    self.version = 1;
                    self.next_version = 2;
                    self.next_ino = ROOT + 1;
                }
                self.head = sending.head;
                self.renewed_at = Some(sending.at);
                self.ready();
            }
            // Nothing applied: start over from what storage holds. A
            // standby's writer that lost the race stops.
            CommitOutcome::Failed | CommitOutcome::Unknown => self.restart(),
            CommitOutcome::Fenced if self.takeover == Takeover::Now => self.restart(),
            CommitOutcome::Fenced => self.stop(),
        }
    }

    /// Reads the store again from the start, with nothing cached.
    fn restart(&mut self) {
        self.phase = Phase::Starting;
        self.cache = Cache::with_budget(self.config.cache);
        self.detached = Detached::default();
        self.long_records.clear();
        self.expiries.clear();
        let id = self.id();
        self.reads.insert(
            id,
            Outstanding {
                issued_at: 0,
                reads: Vec::new(),
                purpose: Purpose::Startup,
            },
        );
        self.actions.push(Action::Read {
            id,
            reads: startup_reads(),
        });
    }

    fn ready(&mut self) {
        self.phase = Phase::Ready;
        if self.config.preload {
            self.preload_dirs = BTreeSet::from([ROOT]);
            self.preload_page(Cursor::start());
        }
        let waiting: Vec<u64> = self.queries.keys().copied().collect();
        for id in waiting {
            self.run_query(id);
        }
        self.pump();
    }

    /// Whether a page of the preload is being read.
    pub fn is_preloading(&self) -> bool {
        self.reads
            .values()
            .any(|read| matches!(read.purpose, Purpose::Preload(_)))
    }

    /// Batches and answers the writer holds, for tests and metrics.
    pub fn pending(&self) -> usize {
        self.queue.len()
            + self.forming.len()
            + self
                .groups
                .iter()
                .map(|group| group.batches.len())
                .sum::<usize>()
            + self.held.len()
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Forgets everything cached that no read in progress needs, so later
    /// lookups read storage again.
    pub fn evict_all(&mut self) {
        let spare = self.spare();
        self.cache.forget_tree(spare);
    }
}

/// The id a request is recorded under: the client's id, bound to the key
/// that sent it, so a retry finds only its own key's record. A trusted
/// caller's ids stand as they are, and zero stays zero.
pub fn scoped_request(id: u128, key: Option<KeyId>) -> u128 {
    match key {
        Some(key) if id != 0 => {
            // SplitMix64's finalizer, twice, so every key masks every bit.
            let mix = |mut z: u64| {
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                z ^ (z >> 31)
            };
            let high = mix(key);
            let mask = u128::from(high) << 64 | u128::from(mix(high ^ key));
            match id ^ mask {
                0 => id,
                scoped => scoped,
            }
        }
        _ => id,
    }
}

/// Whether a batch writes more than a batch may: more than 8 MiB of keys
/// and values, or more than a group holds as FoundationDB charges it,
/// which no group could commit. Small keys reach the second first: a batch
/// that frees thousands of files deep in the tree writes little, in keys
/// under each directory above them, and is charged several times as much.
fn too_large(writes: &WriteSet) -> bool {
    writes.bytes() > MAX_BATCH_BYTES || writes.charged() > MAX_GROUP_BYTES
}

/// The entry a key in a directory's part of the free queue names: the
/// version that freed it, and what it frees.
fn part_key(key: &[u8]) -> Result<(Version, Freed), DecodeError> {
    match keys::parse(key) {
        Some(Parsed::FreeUnder(_, version, ino, value)) => Ok((version, Freed::named(ino, value))),
        _ => Err(DecodeError),
    }
}

/// A free-queue entry from its key and the ancestors it stores.
fn free_entry(key: &[u8], value: &[u8]) -> Result<FreeEntry, DecodeError> {
    let (version, freed) = match keys::parse(key) {
        Some(Parsed::Free(version, ino, value)) => (version, Freed::named(ino, value)),
        _ => return Err(DecodeError),
    };
    Ok(FreeEntry {
        version,
        freed,
        ancestors: crate::delta::decode_ancestors(value)?,
    })
}

/// What a starting writer reads: the head, the request records' expiries,
/// every key, the handle secret, every lease, revoked keys' ids, the
/// detach records, the release record, the version the free queue keeps
/// its parts under directories from, and the queue's first entry.
fn startup_reads() -> Vec<KvRead> {
    vec![
        KvRead::Get(keys::head()),
        KvRead::Scan {
            range: keys::request_expiry_range(),
            limit: usize::MAX,
        },
        KvRead::Scan {
            range: keys::key_records(),
            limit: usize::MAX,
        },
        KvRead::Get(keys::handle_secret()),
        KvRead::Scan {
            range: keys::leases(),
            limit: usize::MAX,
        },
        KvRead::Scan {
            range: keys::retired_keys(),
            limit: usize::MAX,
        },
        KvRead::Scan {
            range: keys::detached_range(),
            limit: usize::MAX,
        },
        KvRead::Get(keys::release()),
        KvRead::Get(keys::free_index()),
        KvRead::Scan {
            range: keys::free_range(),
            limit: 1,
        },
    ]
}

/// The free-queue index record's value: the version every entry freed
/// after has its keys under the directories above it.
pub fn encode_indexed_from(version: Version) -> Vec<u8> {
    let mut enc = Enc::with_capacity(10);
    enc.varint(version);
    enc.finish()
}

pub fn decode_indexed_from(bytes: &[u8]) -> Result<Version, DecodeError> {
    let mut dec = Dec::new(bytes);
    let version = dec.varint()?;
    dec.end()?;
    Ok(version)
}
