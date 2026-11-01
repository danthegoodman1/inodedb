//! Deterministic simulator for InodeDB.
//!
//! One thread runs clients, writers and standbys, the memory backend and a
//! network. Storage reads and commits take time, so reads straddle commits;
//! messages take time, so groups form. Faults come from the seed too:
//! commits that fail or end unknown, slow storage, lost and duplicated
//! messages, clients cut off for a while, writer crashes, and writers whose
//! storage stalls until a standby takes over while they still run. Each
//! process's clock runs at its own rate. Clients retry with the same
//! request id until they hear back, and some reach a writer a takeover has
//! fenced. The model follows what storage makes durable,
//! and every answer is checked against the model at the version the answer
//! reports, never against the core's own state. `fsck` runs after every
//! commit. Subscribers hold leases and snapshots, and a consumer claims
//! and acknowledges freed records while they read. The seed determines the
//! whole run, so a seed replays its run exactly.

pub mod generate;
pub mod harness;
pub mod model;
pub mod prng;

use generate::Shape;
use inodedb_core::auth::{Caller, Denied, Grant, KeyError, KeyId, KeyOp};
use inodedb_core::cache::{Budget, Cache};
use inodedb_core::feed::{Change, Subscriber, route};
use inodedb_core::fsck;
use inodedb_core::keys::{self, KvRead, KvResult, Parsed, WriteSet};
use inodedb_core::lease::{ClaimId, FreeEntry, LeaseError, LeaseId, LeaseRecord};
use inodedb_core::log::LogEntry;
use inodedb_core::ops::{Batch, BatchError, CommitResult};
use inodedb_core::query::{self as core_query, Query, QueryResult, Target};
use inodedb_core::snapshot::Miss;
use inodedb_core::standby::Follower;
use inodedb_core::types::{Error, Ino, Kind, Life, MAX_OPS, Millis, ROOT, Version};
use inodedb_core::view::Fact;
use inodedb_core::writer::{Action, Answer, CommitOutcome, Config, Head, Takeover, Writer};
use inodedb_storage::Memory;
use model::{Model, ModelAnswer, ModelOutcome, NodeId};
use prng::Prng;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

/// A request lifetime no run outlasts.
pub const FOREVER: Millis = 1 << 40;

/// A run's configuration, drawn from its seed.
#[derive(Clone, Debug)]
pub struct Options {
    pub clients: usize,
    pub requests: usize,
    pub in_flight: usize,
    pub names: usize,
    pub depth: usize,
    pub storage_delay: u64,
    pub network_delay: u64,
    pub think: u64,
    pub group_max_bytes: usize,
    pub query_percent: u64,
    pub cond_percent: u64,
    pub atomic_percent: u64,
    pub when_percent: u64,
    /// Chance per event that the writer forgets its cache.
    pub evict_percent: u64,
    /// Chance a commit fails, or ends unknown having applied or not.
    pub commit_fault_percent: u64,
    /// Chance a storage call stalls.
    pub slow_percent: u64,
    /// Chance a request or a reply is lost.
    pub loss_percent: u64,
    /// Chance a request arrives twice.
    pub dup_percent: u64,
    /// Chance per request that its client is cut off for a while.
    pub partition_percent: u64,
    /// Ticks a client waits for an answer before it sends again; it waits
    /// twice as long after each try.
    pub retry_after: u64,
    /// Times the writer crashes and a new one starts over the storage.
    pub crashes: usize,
    /// The most ticks between crashes.
    pub crash_every: u64,
    /// How long request records last.
    pub request_ttl: Millis,
    /// What the writer's cache holds.
    pub cache: Budget,
    /// The most ticks a subscriber's changes take to arrive.
    pub feed_delay: u64,
    /// Chance a subscriber's feed is cut, so it starts over empty.
    pub feed_cut_percent: u64,
    /// Chance each client works under a key rooted at a directory it
    /// picks, rather than at the tree's root.
    pub keyed_percent: u64,
    /// How long a lease lasts past its last heartbeat.
    pub lease_ttl: Millis,
    /// How long a claim lasts unacknowledged.
    pub claim_ttl: Millis,
    /// Chance per delivery that a subscriber opens a snapshot.
    pub snapshot_percent: u64,
    /// The most ticks a snapshot stays open.
    pub snapshot_ticks: u64,
    /// Chance per delivery that a subscriber's heartbeats stop for longer
    /// than its lease lasts.
    pub lease_gap_percent: u64,
    /// Ticks between a consumer's claims of freed records; FOREVER runs no
    /// consumer.
    pub consume_every: u64,
    /// How long a writer answers reads after its last commit that applied.
    pub writer_lease: Millis,
    /// Standbys that watch the head record and take over once it goes
    /// unchanged.
    pub standbys: usize,
    /// Times the writer's storage calls stall, long enough at most for a
    /// standby to take over while the writer still runs.
    pub stalls: usize,
    /// The most ticks between stalls.
    pub stall_every: u64,
    /// The most each process's clock runs fast or slow, in parts per
    /// million.
    pub drift_ppm: u64,
    /// Clients that only read, and so keep reading through a writer's
    /// stall.
    pub reader_percent: u64,
}

impl Options {
    /// Options for a seed. Faults draw from their own stream, so the rest
    /// match what the seed drew before faults existed.
    pub fn swarm(seed: u64) -> Options {
        let prng = &mut Prng::stream(seed, "options");
        let options = Options {
            clients: prng.range(1..=6) as usize,
            requests: prng.range(20..=200) as usize,
            in_flight: prng.range(1..=4) as usize,
            names: prng.range(2..=4) as usize,
            depth: prng.range(1..=3) as usize,
            storage_delay: prng.range(0..=6),
            network_delay: prng.range(0..=4),
            think: prng.range(0..=4),
            group_max_bytes: if prng.percent(30) {
                prng.range(200..=4_000) as usize
            } else {
                9_000_000
            },
            query_percent: prng.range(0..=40),
            cond_percent: prng.range(0..=40),
            atomic_percent: prng.range(0..=30),
            when_percent: prng.range(0..=20),
            evict_percent: *prng.pick(&[0, 1, 5, 20]),
            ..Options::calm()
        };
        let prng = &mut Prng::stream(seed, "fault options");
        let options = Options {
            commit_fault_percent: *prng.pick(&[0, 0, 2, 10, 30]),
            slow_percent: *prng.pick(&[0, 1, 5]),
            loss_percent: *prng.pick(&[0, 0, 1, 5, 15]),
            dup_percent: *prng.pick(&[0, 0, 2, 10]),
            partition_percent: *prng.pick(&[0, 0, 1, 3]),
            retry_after: prng.range(5..=200),
            crashes: *prng.pick(&[0, 0, 1, 2, 5]),
            crash_every: prng.range(20..=2_000),
            request_ttl: if prng.percent(30) {
                prng.range(50..=3_000)
            } else {
                FOREVER
            },
            cache: if prng.percent(30) {
                Budget {
                    dirs: prng.range(0..=6) as usize,
                    inodes: prng.range(0..=12) as usize,
                    value_bytes: prng.range(0..=200) as usize,
                    undo_bytes: prng.range(0..=4_000) as usize,
                }
            } else {
                Budget::default()
            },
            feed_delay: prng.range(0..=50),
            feed_cut_percent: *prng.pick(&[0, 0, 2, 10]),
            keyed_percent: *Prng::stream(seed, "key options").pick(&[0, 0, 50, 100]),
            ..options
        };
        let prng = &mut Prng::stream(seed, "lease options");
        let options = Options {
            lease_ttl: prng.range(50..=3_000),
            claim_ttl: prng.range(20..=2_000),
            snapshot_percent: *prng.pick(&[0, 10, 30, 60]),
            snapshot_ticks: prng.range(10..=3_000),
            lease_gap_percent: *prng.pick(&[0, 0, 2, 10]),
            consume_every: if prng.percent(70) {
                prng.range(5..=400)
            } else {
                FOREVER
            },
            ..options
        };
        let prng = &mut Prng::stream(seed, "takeover options");
        Options {
            writer_lease: prng.range(50..=2_000),
            standbys: *prng.pick(&[0, 1, 2, 2]),
            stalls: *prng.pick(&[0, 1, 3, 3]),
            stall_every: prng.range(100..=5_000),
            // Clocks drift apart by less than the margin, a fifth of the
            // lease, over a lease.
            drift_ppm: *prng.pick(&[0, 10_000, 90_000, 90_000]),
            reader_percent: *prng.pick(&[0, 30, 60, 60]),
            ..options
        }
    }

    /// One client, small batches, and no faults.
    pub fn calm() -> Options {
        Options {
            clients: 1,
            requests: 50,
            in_flight: 1,
            names: 3,
            depth: 2,
            storage_delay: 2,
            network_delay: 1,
            think: 1,
            group_max_bytes: 9_000_000,
            query_percent: 20,
            cond_percent: 20,
            atomic_percent: 10,
            when_percent: 5,
            evict_percent: 0,
            commit_fault_percent: 0,
            slow_percent: 0,
            loss_percent: 0,
            dup_percent: 0,
            partition_percent: 0,
            retry_after: 1_000,
            crashes: 0,
            crash_every: 1_000,
            request_ttl: FOREVER,
            cache: Budget::default(),
            feed_delay: 5,
            feed_cut_percent: 0,
            keyed_percent: 0,
            lease_ttl: 30_000,
            claim_ttl: 60_000,
            snapshot_percent: 0,
            snapshot_ticks: 100,
            lease_gap_percent: 0,
            consume_every: FOREVER,
            writer_lease: 5_000,
            standbys: 0,
            stalls: 0,
            stall_every: 1_000,
            drift_ppm: 0,
            reader_percent: 0,
        }
    }
}

/// A bijection between the core's inodes and the model's nodes, learned
/// from answers.
#[derive(Default, Debug)]
pub struct Mapping {
    ino_to_node: BTreeMap<Ino, NodeId>,
    node_to_ino: BTreeMap<NodeId, Ino>,
}

impl Mapping {
    pub fn node(&self, ino: Ino) -> Option<NodeId> {
        self.ino_to_node.get(&ino).copied()
    }

    pub fn ino(&self, node: NodeId) -> Option<Ino> {
        self.node_to_ino.get(&node).copied()
    }

    /// One of the inodes answers have named, drawn from `prng`.
    fn any(&self, prng: &mut Prng) -> Option<Ino> {
        let count = self.ino_to_node.len() as u64;
        (count > 0).then(|| {
            *self
                .ino_to_node
                .keys()
                .nth(prng.below(count) as usize)
                .expect("in range")
        })
    }

    pub fn bind(&mut self, ino: Ino, node: NodeId) -> Result<(), String> {
        match (self.ino_to_node.get(&ino), self.node_to_ino.get(&node)) {
            (Some(known), _) if *known != node => {
                Err(format!("inode {ino} is node {known}, not {node}"))
            }
            (_, Some(known)) if *known != ino => {
                Err(format!("node {node} is inode {known}, not {ino}"))
            }
            _ => {
                self.ino_to_node.insert(ino, node);
                self.node_to_ino.insert(node, ino);
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Kindof {
    Batch(Batch),
    Query(Query),
    Key(KeyOp),
}

/// The key a client works under, once the writer has minted it.
#[derive(Clone, Debug)]
struct ClientKey {
    id: KeyId,
    minted: bool,
}

/// The key a caller works under; None for a trusted one.
fn caller_key(caller: Caller) -> Option<KeyId> {
    match caller {
        Caller::Key { id, .. } => Some(id),
        Caller::Trusted(_) => None,
    }
}

/// The id a batch's request is recorded under, for its caller.
fn recorded(batch: &Batch, caller: Caller) -> u128 {
    inodedb_core::writer::scoped_request(batch.request_id, caller_key(caller))
}

/// The hash the simulator gives a key's secret.
fn key_hash(id: KeyId) -> [u8; 32] {
    let mut hash = [0; 32];
    for chunk in hash.chunks_mut(8) {
        chunk.copy_from_slice(&id.to_be_bytes());
    }
    hash
}

/// Each minting of a key: the version that minted it and its root node.
/// A late copy of a mint may mint an id again once revocation has freed
/// it.
type Incarnations = Vec<(Version, NodeId)>;

/// A key's root node as of `version`.
fn key_node(incarnations: &Incarnations, version: Version) -> Option<NodeId> {
    incarnations
        .iter()
        .rev()
        .find(|(minted, _)| *minted <= version)
        .map(|(_, node)| *node)
}

/// The keys whose roots the model's tree holds at its version.
fn live_keys(keys: &BTreeMap<KeyId, Incarnations>, model: &Model) -> BTreeSet<KeyId> {
    keys.iter()
        .filter(|(_, incarnations)| {
            key_node(incarnations, model.version).is_some_and(|node| model.path_of(node).is_some())
        })
        .map(|(id, _)| *id)
        .collect()
}

/// A request a client waits on.
#[derive(Debug)]
struct Request {
    client: usize,
    caller: Caller,
    what: Kindof,
    /// The highest version acknowledged when the request first reached the
    /// writer.
    floor: Option<Version>,
    /// The try the client listens to; it ignores replies to earlier ones.
    attempt: u32,
    /// Ticks before the client tries again.
    wait: u64,
    /// When the client first sent it; it gives up after `give_up` ticks.
    sent: u64,
}

/// One copy of a request on its way to, or held by, the writer.
#[derive(Debug)]
struct Attempt {
    request: u64,
    attempt: u32,
    client: usize,
    caller: Caller,
    what: Kindof,
    /// The writer that holds it, once delivered.
    generation: Option<u64>,
    /// The highest version acknowledged when this copy arrived.
    floor: Version,
    /// The request's floor when this copy arrived.
    first_floor: Version,
    /// The version a query asked to read at least.
    at_least: Version,
    /// A standby sent it on to the writer.
    to_writer: bool,
}

#[derive(Debug)]
enum Event {
    Submit {
        client: usize,
    },
    Deliver {
        token: u64,
    },
    Timeout {
        request: u64,
        attempt: u32,
    },
    StorageRead {
        generation: u64,
        id: u64,
        reads: Vec<KvRead>,
    },
    ReadDone {
        generation: u64,
        id: u64,
        results: Vec<KvResult>,
    },
    StorageCommit {
        generation: u64,
        id: u64,
        expect: Option<Vec<u8>>,
        writes: WriteSet,
    },
    CommitDone {
        generation: u64,
        id: u64,
        outcome: CommitOutcome,
    },
    StorageResolve {
        generation: u64,
        id: u64,
    },
    ResolveDone {
        generation: u64,
        id: u64,
        head: Option<Vec<u8>>,
    },
    Reply {
        request: u64,
        attempt: u32,
        result: Option<CommitResult>,
    },
    Crash,
    Feed {
        watcher: usize,
    },
    /// The consumer claims released records, or acknowledges a claim.
    Consume,
    /// Every writer acts on the time alone, as a server's timer has it.
    Tick,
    /// A standby reads the head record.
    Poll {
        standby: usize,
    },
    Polled {
        standby: usize,
        head: Option<Vec<u8>>,
    },
    /// The newest writer's storage calls stall.
    Stall,
    /// A standby's follower reads storage.
    StandbyRead {
        standby: usize,
        era: u64,
        id: u64,
        reads: Vec<KvRead>,
    },
    StandbyReadDone {
        standby: usize,
        era: u64,
        id: u64,
        results: Vec<KvResult>,
    },
    /// A standby's follower reads the log's newest entries.
    Follow {
        standby: usize,
    },
}

/// A process's clock, which runs at its own rate.
#[derive(Clone, Copy, Debug)]
struct Clock {
    /// Parts per million faster than the simulator's time; negative runs
    /// slower.
    ppm: i64,
}

impl Clock {
    /// A clock as fast or as slow as `drift` allows, or exact: the
    /// extremes test the margin hardest.
    fn drawn(prng: &mut Prng, drift: u64) -> Clock {
        let drift = drift as i64;
        Clock {
            ppm: *prng.pick(&[-drift, -drift, 0, drift, drift]),
        }
    }

    fn at(&self, time: u64) -> Millis {
        let time = time as i64;
        (time + time * self.ppm / 1_000_000) as Millis
    }
}

/// A writer's process.
struct Proc {
    writer: Writer,
    clock: Clock,
    /// Storage calls it makes wait until this tick.
    stalled_until: u64,
}

/// A standby: while no writer of its own runs, a follower of the log that
/// answers reads at a version, watches the head record, and hands its cache
/// and waiting reads to the writer it starts once the head goes unchanged.
struct Standby {
    follower: Option<Follower>,
    /// Counts the followers it has run, so storage results for an earlier
    /// one go unused.
    era: u64,
    clock: Clock,
    /// The writer it started, while that runs.
    writer: Option<u64>,
    /// Whether a read of the log is on its way.
    following: bool,
}

/// A subscriber to a subtree, as a client's cache would hold one.
struct Watcher {
    subscriber: Subscriber,
    /// The model's node for the subtree's root.
    node: NodeId,
    /// Changes on their way.
    queue: VecDeque<Change>,
    feed_scheduled: bool,
    /// Reads the subscriber hasn't filled yet, with their versions.
    reads: Vec<(Version, Fact)>,
    lease: Held,
    /// Its heartbeats stop until this tick.
    stalled_until: u64,
    /// Open snapshots' versions, each with the tick it closes at.
    snapshots: Vec<(Version, u64)>,
}

impl Watcher {
    fn new(subscriber: Subscriber, node: NodeId) -> Watcher {
        Watcher {
            subscriber,
            node,
            queue: VecDeque::new(),
            feed_scheduled: false,
            reads: Vec::new(),
            lease: Held::None,
            stalled_until: 0,
            snapshots: Vec::new(),
        }
    }

    /// Starts over at `head` with nothing cached and no snapshots.
    fn reset(&mut self, head: Version) {
        self.subscriber.reset(head);
        self.queue.clear();
        self.reads.clear();
        self.snapshots.clear();
    }
}

/// A subscriber's lease, as its answers report it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Held {
    None,
    /// Asked for under a token, with no answer yet.
    Opening(u64),
    Live {
        id: LeaseId,
        /// The oldest version it last reported, which it reads nothing
        /// older than.
        oldest: Version,
        position: Version,
        /// When the writer lets it run out, short of another heartbeat.
        expires: Millis,
    },
}

/// A lease or free-queue call awaiting the writer's answer.
#[derive(Clone, Debug)]
enum LeaseCall {
    /// A lease's lifetime counts from when the writer runs its opening,
    /// which is no sooner than when it was asked.
    Open {
        watcher: usize,
        sent: Millis,
    },
    Beat {
        watcher: usize,
        oldest: Version,
        position: Version,
    },
    Claim,
    Ack {
        entries: Vec<FreeEntry>,
    },
}

struct Client {
    remaining: usize,
    outstanding: usize,
    submit_scheduled: bool,
    /// The client hears nothing and reaches no one until this tick.
    cut_until: u64,
    /// The key it works under; None works at the tree's root.
    key: Option<ClientKey>,
    /// The writer it talks to: the newest when it last looked, until that
    /// one fails it.
    writer: Option<u64>,
    /// It only reads.
    reads: bool,
}

/// A durable commit of a request id.
#[derive(Clone, Debug)]
struct Committed {
    version: Version,
    outcomes: Vec<ModelOutcome>,
    /// When the writer ran the batch, which starts its record's lifetime.
    time: Millis,
}

pub struct Simulator {
    seed: u64,
    options: Options,
    config: Config,
    trace: bool,
    now: u64,
    seq: u64,
    events: BTreeMap<(u64, u64), Event>,
    delays: Prng,
    requests_prng: Prng,
    faults: Prng,
    floors: Prng,
    /// Which reads go to a standby, and to which.
    standby_reads: Prng,
    /// Which stats go by inode.
    handles: Prng,
    standby_delays: Prng,
    storage: Memory,
    /// Every writer still running, by generation, which is also its id.
    procs: BTreeMap<u64, Proc>,
    /// The writer clients reach: the one the newest committed head names,
    /// once it is ready.
    current: u64,
    /// The last generation started.
    generation: u64,
    /// The writer the newest committed head names.
    holder: u64,
    /// When each writer that held the store first had a successor's commit
    /// land.
    superseded: BTreeMap<u64, u64>,
    standbys: Vec<Standby>,
    takeover_prng: Prng,
    stalls_left: usize,
    crashes_left: usize,
    clients: Vec<Client>,
    requests: BTreeMap<u64, Request>,
    attempts: BTreeMap<u64, Attempt>,
    next_request: u64,
    next_token: u64,
    /// Every batch sent, by request id.
    batches: BTreeMap<u128, Batch>,
    /// The key each keyed batch was sent under, by request id.
    callers: BTreeMap<u128, KeyId>,
    /// The root path each mint asked for, by key id.
    mints: BTreeMap<KeyId, Vec<inodedb_core::types::Name>>,
    /// Every minting of each key.
    keys: BTreeMap<KeyId, Incarnations>,
    /// Keys some commit revoked.
    revoked: BTreeSet<KeyId>,
    keys_prng: Prng,
    model: Option<Model>,
    history: BTreeMap<Version, Model>,
    /// Each request id's durable commits.
    commits: BTreeMap<u128, Vec<Committed>>,
    /// The first answer each writer gave a request id.
    answers: BTreeMap<(u64, u128), CommitResult>,
    /// The answer each client took for its batch.
    accepted: BTreeMap<u128, CommitResult>,
    mapping: Mapping,
    acked: Version,
    versions_seen: Vec<Version>,
    watchers: Vec<Watcher>,
    watch_prng: Prng,
    /// Lease and free-queue calls awaiting answers, each with the writer
    /// asked.
    lease_calls: BTreeMap<u64, (u64, LeaseCall)>,
    /// The consumer's claim awaiting acknowledgement, and whether a call
    /// of its is in flight.
    claim: Option<(ClaimId, Vec<FreeEntry>)>,
    consuming: bool,
    lease_prng: Prng,
    summary: Summary,
}

#[derive(Debug, Default)]
pub struct Summary {
    pub commits: u64,
    pub transactions: u64,
    pub batches: u64,
    pub skipped_batches: u64,
    pub queries: u64,
    pub reads: u64,
    pub rejected: u64,
    pub retries: u64,
    pub faults: u64,
    pub crashes: u64,
    pub given_up: u64,
    pub cache_checks: u64,
    pub snapshot_checks: u64,
    pub snapshots_too_old: u64,
    pub leases_expired: u64,
    pub acked: u64,
    pub keys: u64,
    pub denied: u64,
    pub takeovers: u64,
    pub stalls: u64,
    pub ticks: u64,
    /// Queries a standby answered.
    pub standby_queries: u64,
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} commits in {} transactions, {} batches ({} changed nothing, {} rejected), {} queries ({} from standbys), {} storage reads, {} retries ({} given up), {} storage faults, {} crashes, {} cache checks, {} snapshot checks ({} too old), {} leases expired, {} records acknowledged, {} keys ({} requests denied), {} takeovers, {} stalls, {} ticks",
            self.commits,
            self.transactions,
            self.batches,
            self.skipped_batches,
            self.rejected,
            self.queries,
            self.standby_queries,
            self.reads,
            self.retries,
            self.given_up,
            self.faults,
            self.crashes,
            self.cache_checks,
            self.snapshot_checks,
            self.snapshots_too_old,
            self.leases_expired,
            self.acked,
            self.keys,
            self.denied,
            self.takeovers,
            self.stalls,
            self.ticks
        )
    }
}

#[derive(Debug)]
pub struct Failure {
    pub seed: u64,
    pub tick: u64,
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed at tick {}: {}",
            self.seed, self.tick, self.message
        )
    }
}

impl Simulator {
    pub fn from_seed(seed: u64) -> Simulator {
        Simulator::new(seed, Options::swarm(seed))
    }

    pub fn new(seed: u64, options: Options) -> Simulator {
        let config = Config {
            group_max_bytes: options.group_max_bytes,
            request_ttl: options.request_ttl,
            cache: options.cache,
            lease_ttl: options.lease_ttl,
            claim_ttl: options.claim_ttl,
            writer_lease: options.writer_lease,
            renew_every: (options.writer_lease / 5).max(1),
            takeover_margin: (options.writer_lease / 5).max(1),
            ..Config::default()
        };
        let mut takeover_prng = Prng::stream(seed, "takeovers");
        let mut clock = || Clock::drawn(&mut takeover_prng, options.drift_ppm);
        let first = clock();
        let standbys = (0..options.standbys)
            .map(|_| {
                let clock = clock();
                Standby {
                    follower: Some(Follower::new(config.clone(), clock.at(0))),
                    era: 0,
                    clock,
                    writer: None,
                    following: false,
                }
            })
            .collect();
        let mut keys_prng = Prng::stream(seed, "keys");
        let mut readers = Prng::stream(seed, "readers");
        let clients = (0..options.clients)
            .map(|_| Client {
                reads: readers.percent(options.reader_percent),
                remaining: options.requests,
                outstanding: 0,
                submit_scheduled: false,
                cut_until: 0,
                key: keys_prng.percent(options.keyed_percent).then(|| ClientKey {
                    id: keys_prng.next_u64(),
                    minted: false,
                }),
                writer: None,
            })
            .collect();
        Simulator {
            seed,
            trace: false,
            now: 0,
            seq: 0,
            events: BTreeMap::new(),
            delays: Prng::stream(seed, "delays"),
            requests_prng: Prng::stream(seed, "requests"),
            faults: Prng::stream(seed, "faults"),
            floors: Prng::stream(seed, "floors"),
            standby_reads: Prng::stream(seed, "standby reads"),
            handles: Prng::stream(seed, "handles"),
            standby_delays: Prng::stream(seed, "standby delays"),
            storage: Memory::new(),
            procs: BTreeMap::from([(
                1,
                Proc {
                    // With standbys, the first writer is one of them, and an
                    // empty store is all it may take over.
                    writer: Writer::new(config.clone(), 1, 0, [1; 32]).after(
                        if options.standbys == 0 {
                            Takeover::Now
                        } else {
                            Takeover::From(None)
                        },
                    ),
                    clock: first,
                    stalled_until: 0,
                },
            )]),
            current: 1,
            generation: 1,
            holder: 0,
            superseded: BTreeMap::new(),
            standbys,
            takeover_prng,
            stalls_left: options.stalls,
            crashes_left: options.crashes,
            config,
            clients,
            requests: BTreeMap::new(),
            attempts: BTreeMap::new(),
            next_request: 1,
            next_token: 1,
            batches: BTreeMap::new(),
            callers: BTreeMap::new(),
            mints: BTreeMap::new(),
            keys: BTreeMap::new(),
            revoked: BTreeSet::new(),
            keys_prng,
            model: None,
            history: BTreeMap::new(),
            commits: BTreeMap::new(),
            answers: BTreeMap::new(),
            accepted: BTreeMap::new(),
            mapping: Mapping::default(),
            acked: 0,
            versions_seen: Vec::new(),
            watchers: vec![Watcher::new(
                Subscriber::with_budget(ROOT, 0, options.cache),
                model::MODEL_ROOT,
            )],
            watch_prng: Prng::stream(seed, "watch"),
            lease_calls: BTreeMap::new(),
            claim: None,
            consuming: false,
            lease_prng: Prng::stream(seed, "leases"),
            summary: Summary::default(),
            options,
        }
    }

    pub fn trace(mut self) -> Simulator {
        self.trace = true;
        self
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    fn fail(&self, message: impl Into<String>) -> Failure {
        Failure {
            seed: self.seed,
            tick: self.now,
            message: message.into(),
        }
    }

    fn schedule(&mut self, delay: u64, event: Event) {
        let seq = self.seq;
        self.seq += 1;
        self.events.insert((self.now + delay, seq), event);
    }

    fn storage_delay(&mut self) -> u64 {
        let max = self.options.storage_delay;
        let delay = self.delays.range(0..=max);
        if self.faults.percent(self.options.slow_percent) {
            delay + self.faults.range(20..=300)
        } else {
            delay
        }
    }

    fn network_delay(&mut self) -> u64 {
        let max = self.options.network_delay;
        self.delays.range(0..=max)
    }

    /// A delay for a standby's storage call (`storage` true) or message,
    /// from a stream of its own.
    fn standby_delay(&mut self, storage: bool) -> u64 {
        let max = if storage {
            self.options.storage_delay
        } else {
            self.options.network_delay
        };
        self.standby_delays.range(0..=max)
    }

    /// Whether the record of a commit at `time` still answers retries at
    /// `later`.
    fn live(&self, time: Millis, later: Millis) -> bool {
        later < time.saturating_add(self.options.request_ttl)
    }

    fn unfinished(&self) -> bool {
        !self.requests.is_empty() || self.clients.iter().any(|client| client.remaining > 0)
    }

    pub fn run(mut self) -> Result<Summary, Failure> {
        self.mapping
            .bind(ROOT, model::MODEL_ROOT)
            .expect("an empty mapping binds");
        self.actions()?;
        for client in 0..self.clients.len() {
            let delay = self.delays.range(0..=self.options.think);
            self.clients[client].submit_scheduled = true;
            self.schedule(delay, Event::Submit { client });
        }
        if self.crashes_left > 0 {
            let delay = self.faults.range(1..=self.options.crash_every);
            self.schedule(delay, Event::Crash);
        }
        if self.options.consume_every != FOREVER {
            self.schedule(self.options.consume_every, Event::Consume);
        }
        self.schedule(self.tick_every(), Event::Tick);
        for standby in 0..self.standbys.len() {
            self.schedule(0, Event::Poll { standby });
        }
        if self.stalls_left > 0 {
            let delay = self.takeover_prng.range(1..=self.options.stall_every);
            self.schedule(delay, Event::Stall);
        }
        while let Some(((time, _), event)) = self.events.pop_first() {
            self.now = time;
            for proc in self.procs.values_mut() {
                proc.writer.set_now(proc.clock.at(time));
            }
            if self.trace {
                eprintln!("{time}: {event:?}");
            }
            self.handle(event)?;
            if self.delays.percent(self.options.evict_percent) {
                self.writer_mut().evict_all();
            }
            self.actions()?;
            self.settle();
            if self.now > 10_000_000 {
                return Err(self.fail("the run never settled"));
            }
        }
        if !self.requests.is_empty() {
            return Err(self.fail(format!(
                "{} requests never got an answer",
                self.requests.len()
            )));
        }
        if !self.attempts.is_empty() {
            return Err(self.fail(format!(
                "the writer never answered {} copies of requests",
                self.attempts.len()
            )));
        }
        if self.writer().pending() != 0 {
            return Err(self.fail(format!(
                "the writer still holds {} batches",
                self.writer().pending()
            )));
        }
        let dump = self.storage.dump();
        let log = self.storage.log_now(0, usize::MAX);
        let problems = fsck::check_with_log(
            dump.iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
            log.iter()
                .map(|(version, bytes)| (*version, bytes.as_slice())),
        );
        if !problems.is_empty() {
            return Err(self.fail(format!("fsck: {}", problems.join("; "))));
        }
        self.summary.ticks = self.now;
        Ok(self.summary)
    }

    fn handle(&mut self, event: Event) -> Result<(), Failure> {
        match event {
            Event::Submit { client } => self.submit(client),
            Event::Deliver { token } => self.deliver(token),
            Event::Timeout { request, attempt } => self.timeout(request, attempt),
            Event::StorageRead {
                generation,
                id,
                reads,
            } => {
                let results = self.storage.read_now(&reads);
                self.summary.reads += 1;
                let delay = self.storage_delay();
                self.schedule(
                    delay,
                    Event::ReadDone {
                        generation,
                        id,
                        results,
                    },
                );
            }
            Event::ReadDone {
                generation,
                id,
                results,
            } => {
                if let Some(proc) = self.procs.get_mut(&generation) {
                    proc.writer.read_done(id, results);
                }
            }
            Event::StorageCommit {
                generation,
                id,
                expect,
                writes,
            } => {
                let before = self.storage.head_now();
                let outcome = self.store(expect.as_deref(), &writes)?;
                self.landed(generation, before)?;
                let delay = self.storage_delay();
                self.schedule(
                    delay,
                    Event::CommitDone {
                        generation,
                        id,
                        outcome,
                    },
                );
            }
            Event::CommitDone {
                generation,
                id,
                outcome,
            } => {
                if let Some(proc) = self.procs.get_mut(&generation) {
                    proc.writer.commit_done(id, outcome);
                }
            }
            Event::StorageResolve { generation, id } => {
                let head = self.storage.head_now();
                let delay = self.storage_delay();
                self.schedule(
                    delay,
                    Event::ResolveDone {
                        generation,
                        id,
                        head,
                    },
                );
            }
            Event::ResolveDone {
                generation,
                id,
                head,
            } => {
                if let Some(proc) = self.procs.get_mut(&generation) {
                    proc.writer.resolve_done(id, head);
                }
            }
            Event::Reply {
                request,
                attempt,
                result,
            } => self.reply(request, attempt, result)?,
            Event::Crash => self.crash()?,
            Event::Feed { watcher } => self.feed(watcher)?,
            Event::Consume => self.consume()?,
            Event::Tick => {
                for proc in self.procs.values_mut() {
                    proc.writer.tick();
                }
                // Writers tick while anything waits on them.
                if self.unfinished() || self.procs.len() > 1 || !self.attempts.is_empty() {
                    self.schedule(self.tick_every(), Event::Tick);
                }
            }
            Event::Poll { standby } => {
                let head = self.storage.head_now();
                let delay = self.storage_delay();
                self.schedule(delay, Event::Polled { standby, head });
            }
            Event::Polled { standby, head } => self.polled(standby, head)?,
            Event::StandbyRead {
                standby,
                era,
                id,
                reads,
            } => {
                let results = self.storage.read_now(&reads);
                self.summary.reads += 1;
                let delay = self.standby_delay(true);
                self.schedule(
                    delay,
                    Event::StandbyReadDone {
                        standby,
                        era,
                        id,
                        results,
                    },
                );
            }
            Event::StandbyReadDone {
                standby,
                era,
                id,
                results,
            } => {
                let standby = &mut self.standbys[standby];
                if standby.era == era
                    && let Some(follower) = &mut standby.follower
                    && follower.read_done(id, results).is_err()
                {
                    return Err(self.fail("a standby can't decode what storage holds"));
                }
            }
            Event::Follow { standby } => self.follow(standby)?,
            Event::Stall => {
                self.summary.stalls += 1;
                let wait = self.config.takeover_wait();
                let until = self.now + self.takeover_prng.range(wait / 2..=3 * wait);
                let current = self.current;
                if let Some(proc) = self.procs.get_mut(&current) {
                    proc.stalled_until = proc.stalled_until.max(until);
                }
                self.stalls_left -= 1;
                if self.stalls_left > 0 && self.unfinished() {
                    let delay = self.takeover_prng.range(1..=self.options.stall_every);
                    self.schedule(delay, Event::Stall);
                }
            }
        }
        Ok(())
    }

    fn writer(&self) -> &Writer {
        &self.procs[&self.current].writer
    }

    fn writer_mut(&mut self) -> &mut Writer {
        &mut self
            .procs
            .get_mut(&self.current)
            .expect("the current writer runs")
            .writer
    }

    /// How often writers act on the time alone: four times a renewal.
    fn tick_every(&self) -> u64 {
        (self.config.renew_every / 4).max(1)
    }

    /// Tracks which writer holds the store after a commit by `generation`
    /// that found `before` as the head. Property 4: a writer whose
    /// successor's commit has landed commits nothing more.
    fn landed(&mut self, generation: u64, before: Option<Vec<u8>>) -> Result<(), Failure> {
        let after = self.storage.head_now();
        if after == before {
            return Ok(());
        }
        if let Some(at) = self.superseded.get(&generation) {
            return Err(self.fail(format!(
                "writer {generation} committed after its successor's commit at {at}"
            )));
        }
        let head = after
            .as_deref()
            .map(Head::decode)
            .transpose()
            .map_err(|_| self.fail("the head doesn't decode"))?
            .ok_or_else(|| self.fail("a commit removed the head"))?;
        if head.writer != generation {
            return Err(self.fail(format!(
                "writer {generation} committed a head naming writer {}",
                head.writer
            )));
        }
        if head.writer != self.holder {
            if self.holder != 0 {
                self.superseded.insert(self.holder, self.now);
                self.summary.takeovers += 1;
            }
            self.holder = head.writer;
        }
        Ok(())
    }

    /// Clients turn to the writer that holds the store once it is ready,
    /// and writers that stopped go.
    fn settle(&mut self) {
        if self.holder != self.current
            && self
                .procs
                .get(&self.holder)
                .is_some_and(|proc| proc.writer.is_ready())
        {
            self.current = self.holder;
        }
        let current = self.current;
        self.procs
            .retain(|generation, proc| *generation == current || !proc.writer.is_stopped());
        for standby in &mut self.standbys {
            if standby
                .writer
                .is_some_and(|generation| !self.procs.contains_key(&generation))
            {
                // A fenced writer's server follows the log again.
                standby.writer = None;
                standby.era += 1;
                standby.following = false;
                standby.follower = Some(Follower::new(
                    self.config.clone(),
                    standby.clock.at(self.now),
                ));
            }
        }
    }

    /// A standby's read of the head returned; once the head has gone
    /// unchanged long enough, it starts a writer that takes over from it
    /// with its follower's cache and waiting reads.
    fn polled(&mut self, index: usize, head: Option<Vec<u8>>) -> Result<(), Failure> {
        // A server polls every 100 ms of a 5-second lease.
        if self.unfinished() {
            let every = (self.config.writer_lease / 50).max(1);
            self.schedule(every, Event::Poll { standby: index });
        }
        let standby = &mut self.standbys[index];
        let now = standby.clock.at(self.now);
        let Some(follower) = standby.follower.as_mut() else {
            return Ok(());
        };
        follower.set_now(now);
        let takeover = match follower.observe(head) {
            Ok(Some(takeover)) => takeover,
            Ok(None) => return self.follow(index),
            Err(_) => return Err(self.fail("a standby can't decode the head")),
        };
        let follower = standby.follower.take().expect("checked");
        standby.era += 1;
        self.generation += 1;
        let generation = self.generation;
        standby.writer = Some(generation);
        let clock = standby.clock;
        let secret = follower.handle_secret().unwrap_or([generation as u8; 32]);
        let handover = follower.hand_over();
        let mut writer = Writer::new(self.config.clone(), generation, now, secret)
            .after(takeover)
            .warm(handover.cache, handover.version);
        for read in handover.waiting {
            writer.query(read.token, read.caller, read.query, read.at_least);
            if let Some(attempt) = self.attempts.get_mut(&read.token) {
                attempt.generation = Some(generation);
            }
        }
        self.procs.insert(
            generation,
            Proc {
                writer,
                clock,
                stalled_until: 0,
            },
        );
        Ok(())
    }

    /// Applies the log's entries past a standby's follower, as its server's
    /// read of the log returns them.
    fn follow(&mut self, index: usize) -> Result<(), Failure> {
        self.standbys[index].following = false;
        let from = match &self.standbys[index].follower {
            Some(follower) if follower.is_started() => follower.version() + 1,
            _ => return Ok(()),
        };
        for (version, bytes) in self.storage.log_now(from, usize::MAX) {
            let entry =
                LogEntry::decode(&bytes).map_err(|_| self.fail("a log entry doesn't decode"))?;
            if entry.version != version {
                return Err(self.fail("a log entry's version disagrees with its key"));
            }
            self.standbys[index]
                .follower
                .as_mut()
                .expect("checked")
                .apply(&entry);
        }
        Ok(())
    }

    /// Queues a commit's routed changes for every subscriber, and starts a
    /// subscriber for each directory the root gains, up to six.
    fn publish(&mut self, entry: &LogEntry) {
        if self.trace {
            eprintln!("{}: durable {entry:?}", self.now);
        }
        for index in 0..self.standbys.len() {
            let standby = &mut self.standbys[index];
            if standby.follower.is_some() && !standby.following {
                standby.following = true;
                let delay = self.standby_delay(false);
                self.schedule(delay, Event::Follow { standby: index });
            }
        }
        for effect in &entry.entries {
            if self.watchers.len() < 6
                && effect.dir == ROOT
                && let Some(after) = effect.after
                && after.kind == Kind::Dir
                && effect
                    .before
                    .is_none_or(|before| before.child != after.child)
                && let Some(node) = self
                    .model
                    .as_ref()
                    .and_then(|model| model.node_at(std::slice::from_ref(&effect.name)))
            {
                self.watchers.push(Watcher::new(
                    Subscriber::with_budget(after.child, entry.version, self.options.cache),
                    node,
                ));
            }
        }
        for index in 0..self.watchers.len() {
            let watcher = &mut self.watchers[index];
            let change = route(entry, watcher.subscriber.root(), ROOT).unwrap_or(Change {
                version: entry.version,
                time: entry.time,
                ..Change::default()
            });
            watcher.queue.push_back(change);
            if !watcher.feed_scheduled {
                watcher.feed_scheduled = true;
                let delay = self.watch_prng.range(0..=self.options.feed_delay);
                self.schedule(delay, Event::Feed { watcher: index });
            }
        }
    }

    /// Delivers a subscriber's queued changes, or cuts its feed, then reads
    /// through its cache and checks what it answers against the model.
    fn feed(&mut self, index: usize) -> Result<(), Failure> {
        let head = self.model.as_ref().map_or(0, |model| model.version);
        let watcher = &mut self.watchers[index];
        watcher.feed_scheduled = false;
        if self.watch_prng.percent(self.options.feed_cut_percent) {
            watcher.reset(head);
            return Ok(());
        }
        while let Some(change) = watcher.queue.pop_front() {
            watcher.subscriber.apply(&change);
        }
        for (at, fact) in std::mem::take(&mut watcher.reads) {
            watcher.subscriber.fill(fact, at);
        }
        self.lease(index)?;
        self.open_snapshots(index);
        let shape = Shape {
            names: self.options.names,
            depth: self.options.depth,
            cond_percent: 0,
            atomic_percent: 0,
            when_percent: 0,
        };
        for _ in 0..3 {
            let path = generate::path(&mut self.watch_prng, &shape);
            let query = match self.watch_prng.below(3) {
                0 => Query::Stat(Target::Path(path)),
                1 => Query::Read {
                    target: Target::Path(path),
                    version: None,
                },
                _ => Query::List {
                    target: Target::Path(path),
                    after: None,
                    prefix: None,
                    limit: usize::MAX,
                    budget: usize::MAX,
                    attrs: self.watch_prng.percent(50),
                    values: self.watch_prng.percent(30),
                },
            };
            self.probe(index, &query, head)?;
        }
        self.probe_snapshot(index, head)?;
        self.check_positions()
    }

    /// Renews a subscriber's lease, or opens one. A lease that ran out
    /// starts the subscriber over, as a client does on `LeaseExpired`.
    fn lease(&mut self, index: usize) -> Result<(), Failure> {
        let now = self.now;
        if self.lease_prng.percent(self.options.lease_gap_percent) {
            self.watchers[index].stalled_until = now + 2 * self.options.lease_ttl;
        }
        let watcher = &self.watchers[index];
        if watcher.stalled_until > now {
            return Ok(());
        }
        let token = self.next_token;
        match watcher.lease {
            Held::Opening(_) => return Ok(()),
            Held::None => {
                self.watchers[index].lease = Held::Opening(token);
                self.lease_calls.insert(
                    token,
                    (
                        self.current,
                        LeaseCall::Open {
                            watcher: index,
                            sent: now,
                        },
                    ),
                );
                self.next_token += 1;
                self.writer_mut()
                    .open_lease(token, Caller::Trusted(ROOT), None);
            }
            Held::Live {
                id, oldest: held, ..
            } => {
                let oldest = watcher.subscriber.oldest().max(held);
                let position = watcher.subscriber.version();
                self.lease_calls.insert(
                    token,
                    (
                        self.current,
                        LeaseCall::Beat {
                            watcher: index,
                            oldest,
                            position,
                        },
                    ),
                );
                self.next_token += 1;
                let record = LeaseRecord { oldest, position };
                self.writer_mut()
                    .beat_lease(token, Caller::Trusted(ROOT), id, record);
            }
        }
        self.actions()
    }

    /// How long a writer's `span` surely lasts on the simulator's clock,
    /// whatever the writer's clock rate.
    fn drifted(&self, span: Millis) -> Millis {
        span - span * self.options.drift_ppm / 1_000_000 - 1
    }

    /// Whether a subscriber's lease surely still holds: the writer lets a
    /// lease run out no sooner than its last heartbeat said.
    fn leased(&self, index: usize) -> Option<Version> {
        match self.watchers[index].lease {
            Held::Live {
                oldest, expires, ..
            } if expires > self.now => Some(oldest),
            _ => None,
        }
    }

    /// Closes a subscriber's snapshots whose time is up, and perhaps opens
    /// one, under a lease that holds the snapshot's version.
    fn open_snapshots(&mut self, index: usize) {
        let now = self.now;
        let leased = self.leased(index);
        let watcher = &mut self.watchers[index];
        let (closing, open): (Vec<_>, Vec<_>) = std::mem::take(&mut watcher.snapshots)
            .into_iter()
            .partition(|(_, until)| *until <= now);
        for (at, _) in closing {
            watcher.subscriber.close_snapshot(at);
        }
        watcher.snapshots = open;
        if leased.is_some_and(|oldest| watcher.subscriber.version() >= oldest)
            && self.lease_prng.percent(self.options.snapshot_percent)
        {
            let at = watcher.subscriber.open_snapshot();
            let until = now + self.lease_prng.range(1..=self.options.snapshot_ticks);
            watcher.snapshots.push((at, until));
        }
    }

    /// Reads through one of a subscriber's snapshots, as a client would,
    /// and checks the answer against the model at the snapshot's version.
    /// Misses read storage, which the subscriber has caught up with.
    fn probe_snapshot(&mut self, index: usize, head: Version) -> Result<(), Failure> {
        let watcher = &self.watchers[index];
        if watcher.snapshots.is_empty()
            || self.leased(index).is_none()
            || watcher.subscriber.version() != head
        {
            return Ok(());
        }
        let pick = self.lease_prng.below(watcher.snapshots.len() as u64) as usize;
        let at = watcher.snapshots[pick].0;
        let shape = Shape {
            names: self.options.names,
            depth: self.options.depth,
            cond_percent: 0,
            atomic_percent: 0,
            when_percent: 0,
        };
        let path = generate::path(&mut self.lease_prng, &shape);
        let query = match self.lease_prng.below(3) {
            0 => Query::Stat(Target::Path(path)),
            1 => Query::Read {
                target: Target::Path(path),
                version: None,
            },
            _ => Query::List {
                target: Target::Path(path),
                after: None,
                prefix: None,
                limit: usize::MAX,
                budget: usize::MAX,
                attrs: self.lease_prng.percent(50),
                values: self.lease_prng.percent(30),
            },
        };
        let mut fetched = Cache::new();
        for _ in 0..64 {
            let watcher = &self.watchers[index];
            let (root, node) = (watcher.subscriber.root(), watcher.node);
            match watcher.subscriber.run_at(at, root, &query, &fetched) {
                Ok(result) => {
                    let Some(model) = self.history.get(&at).cloned() else {
                        return Ok(());
                    };
                    let Some(path) = model.path_of(node) else {
                        return Ok(());
                    };
                    self.summary.snapshot_checks += 1;
                    let expected = model.query(&path, &query);
                    return compare_answer(&mut self.mapping, &model, &query, &expected, &result)
                        .map_err(|why| {
                            self.fail(format!(
                                "subscriber {index} to {path:?}, inode {root}, through its snapshot at {at} answered {query:?}: {why}; got {result:?}, expected {expected:?}"
                            ))
                        });
                }
                Err(Miss::TooOld) => {
                    self.summary.snapshots_too_old += 1;
                    return Ok(());
                }
                Err(Miss::Reads(reads)) => {
                    for read in reads {
                        let result = self
                            .storage
                            .read_now(&[read.kv()])
                            .pop()
                            .expect("one result");
                        let fact = read
                            .fact(result)
                            .map_err(|_| self.fail("a read doesn't decode"))?;
                        fetched.fill(fact);
                    }
                }
            }
        }
        Err(self.fail(format!(
            "subscriber {index}'s snapshot at {at} kept missing"
        )))
    }

    /// The writer keeps the log from no later than any live lease's
    /// position needs, which bounds truncation (property 9).
    fn check_positions(&self) -> Result<(), Failure> {
        if !self.writer().is_ready() {
            return Ok(());
        }
        let needed = (0..self.watchers.len())
            .filter(|index| self.leased(*index).is_some())
            .filter_map(|index| match self.watchers[index].lease {
                Held::Live { position, .. } => Some(position),
                _ => None,
            })
            .min();
        match (needed, self.writer().lease_position()) {
            (Some(needed), Some(kept)) if kept <= needed => Ok(()),
            (None, _) => Ok(()),
            (Some(needed), kept) => Err(self.fail(format!(
                "the writer keeps the log from {kept:?}, past a live lease's position {needed}"
            ))),
        }
    }

    /// Claims released records, or acknowledges the last claim once its
    /// records are deleted, or lets the claim lapse.
    fn consume(&mut self) -> Result<(), Failure> {
        if self.unfinished() {
            self.schedule(self.options.consume_every, Event::Consume);
        }
        if self.consuming || !self.writer().is_ready() {
            return Ok(());
        }
        let token = self.next_token;
        self.next_token += 1;
        self.consuming = true;
        match self.claim.take() {
            Some((claim, entries)) if !self.lease_prng.percent(10) => {
                let acked: Vec<(Version, inodedb_core::delta::Freed)> = entries
                    .iter()
                    .map(|entry| (entry.version, entry.freed))
                    .collect();
                self.lease_calls
                    .insert(token, (self.current, LeaseCall::Ack { entries }));
                self.writer_mut()
                    .acknowledge(token, Caller::Trusted(ROOT), claim, &acked);
            }
            _ => {
                let limit = self.lease_prng.range(1..=20) as usize;
                self.lease_calls
                    .insert(token, (self.current, LeaseCall::Claim));
                self.writer_mut().claim(token, Caller::Trusted(ROOT), limit);
            }
        }
        self.actions()
    }

    fn lease_answer(&mut self, call: LeaseCall, answer: Answer) -> Result<(), Failure> {
        let now = self.now;
        let head = self.model.as_ref().map_or(0, |model| model.version);
        match (call, answer) {
            (LeaseCall::Open { watcher, sent }, Answer::Lease(Ok(granted))) => {
                self.watchers[watcher].lease = Held::Live {
                    id: granted.lease,
                    oldest: granted.oldest,
                    position: granted.oldest,
                    expires: sent + self.drifted(granted.expires_in),
                };
            }
            (LeaseCall::Open { watcher, .. }, _) => self.watchers[watcher].lease = Held::None,
            (
                LeaseCall::Beat {
                    watcher,
                    oldest,
                    position,
                },
                Answer::Lease(Ok(granted)),
            ) => {
                if granted.oldest != oldest {
                    return Err(self.fail(format!(
                        "a heartbeat at {oldest} granted {}",
                        granted.oldest
                    )));
                }
                self.watchers[watcher].lease = Held::Live {
                    id: granted.lease,
                    oldest,
                    position,
                    expires: now + self.drifted(granted.expires_in),
                };
            }
            // A starting writer answers nothing yet; the next delivery
            // beats again.
            (LeaseCall::Beat { .. }, Answer::Unavailable) => {}
            (LeaseCall::Beat { watcher, .. }, Answer::Lease(Err(LeaseError::Expired))) => {
                if self.leased(watcher).is_some() {
                    return Err(self.fail(format!(
                        "subscriber {watcher}'s lease ran out before its time"
                    )));
                }
                self.summary.leases_expired += 1;
                let watcher = &mut self.watchers[watcher];
                watcher.reset(head);
                watcher.lease = Held::None;
            }
            (LeaseCall::Claim, Answer::Claimed { claim, entries }) => {
                self.consuming = false;
                // Property 7: nothing a live lease may still read is
                // released.
                for entry in &entries {
                    for index in 0..self.watchers.len() {
                        if let Some(oldest) = self.leased(index)
                            && entry.version > oldest
                        {
                            return Err(self.fail(format!(
                                "claimed {entry:?}, freed after subscriber {index}'s lease at {oldest}"
                            )));
                        }
                    }
                }
                if !entries.is_empty() {
                    self.claim = Some((claim, entries));
                }
            }
            (LeaseCall::Ack { entries }, Answer::Acked(result)) => {
                self.consuming = false;
                if result.is_ok() {
                    self.summary.acked += entries.len() as u64;
                }
            }
            // A writer that stopped answers nothing more.
            (LeaseCall::Claim | LeaseCall::Ack { .. }, Answer::Unavailable) => {
                self.consuming = false;
            }
            (call, answer) => {
                return Err(self.fail(format!("{call:?} answered {answer:?}")));
            }
        }
        Ok(())
    }

    fn probe(&mut self, index: usize, query: &Query, head: Version) -> Result<(), Failure> {
        let watcher = &self.watchers[index];
        let (root, version, node) = (
            watcher.subscriber.root(),
            watcher.subscriber.version(),
            watcher.node,
        );
        match core_query::run(watcher.subscriber.cache(), root, query) {
            Ok(result) => {
                let Some(model) = self.history.get(&version).cloned() else {
                    return Ok(());
                };
                let Some(at) = model.path_of(node) else {
                    return Ok(());
                };
                self.summary.cache_checks += 1;
                let expected = model.query(&at, query);
                compare_answer(&mut self.mapping, &model, query, &expected, &result).map_err(|why| {
                    self.fail(format!(
                        "subscriber {index} to {at:?}, inode {root}, at {version} answered {query:?}: {why}; got {result:?}, expected {expected:?}"
                    ))
                })
            }
            Err(reads) => {
                for read in reads {
                    let result = self
                        .storage
                        .read_now(&[read.kv()])
                        .pop()
                        .expect("one result");
                    let fact = read
                        .fact(result)
                        .map_err(|_| self.fail("a read doesn't decode"))?;
                    let watcher = &mut self.watchers[index];
                    if self.watch_prng.percent(50) {
                        watcher.subscriber.fill(fact, head);
                    } else {
                        watcher.reads.push((head, fact));
                    }
                }
                Ok(())
            }
        }
    }

    /// Commits to storage, perhaps with a fault: a failure that applies
    /// nothing, or an unknown outcome that applied or didn't.
    fn store(
        &mut self,
        expect: Option<&[u8]>,
        writes: &WriteSet,
    ) -> Result<CommitOutcome, Failure> {
        let fault = self
            .faults
            .percent(self.options.commit_fault_percent)
            .then(|| self.faults.below(3));
        if fault.is_some() {
            self.summary.faults += 1;
        }
        match fault {
            Some(0) => Ok(CommitOutcome::Failed),
            Some(1) => Ok(CommitOutcome::Unknown),
            _ => {
                let outcome = self.storage.commit_now(expect, writes);
                if outcome == CommitOutcome::Committed {
                    self.durable(writes)?;
                    if fault.is_some() {
                        return Ok(CommitOutcome::Unknown);
                    }
                }
                Ok(outcome)
            }
        }
    }

    fn submit(&mut self, client: usize) {
        // A client whose key is being minted waits for the mint's reply.
        let minting = self
            .requests
            .values()
            .any(|request| request.client == client && matches!(request.what, Kindof::Key(_)));
        let state = &mut self.clients[client];
        state.submit_scheduled = false;
        if state.remaining == 0 || state.outstanding >= self.options.in_flight || minting {
            return;
        }
        state.remaining -= 1;
        state.outstanding += 1;
        let more = state.remaining > 0 && state.outstanding < self.options.in_flight;
        let shape = Shape {
            names: self.options.names,
            depth: self.options.depth,
            cond_percent: self.options.cond_percent,
            atomic_percent: self.options.atomic_percent,
            when_percent: self.options.when_percent,
        };
        let number = self.next_request;
        self.next_request += 1;
        let key = self.clients[client].key.clone();
        let (caller, what) = match key {
            Some(key) if !key.minted => {
                // Each mint takes a new id, so a late copy of an earlier
                // one mints a key of its own.
                let id = self.keys_prng.next_u64();
                let root = generate::path(&mut self.keys_prng, &shape);
                self.mints.insert(id, root.names.clone());
                self.clients[client].key = Some(ClientKey { id, minted: false });
                let op = KeyOp::Mint {
                    id,
                    hash: key_hash(id),
                    root,
                    grant: Grant::Write,
                    ttl: 0,
                };
                (Caller::Trusted(ROOT), Kindof::Key(op))
            }
            key => {
                let caller = key.map_or(Caller::Trusted(ROOT), |key| Caller::Key {
                    id: key.id,
                    hash: key_hash(key.id),
                });
                let reads = self.clients[client].reads;
                let what = if self.requests_prng.percent(self.options.query_percent) || reads {
                    Kindof::Query(generate::query(&mut self.requests_prng, &shape))
                } else {
                    let id = u128::from(self.requests_prng.next_u64()) << 64 | u128::from(number);
                    let batch =
                        generate::batch(&mut self.requests_prng, &shape, &self.versions_seen, id);
                    let id = recorded(&batch, caller);
                    self.batches.insert(id, batch.clone());
                    if let Caller::Key { id: key, .. } = caller {
                        self.callers.insert(id, key);
                    }
                    Kindof::Batch(batch)
                };
                (caller, what)
            }
        };
        if self.faults.percent(self.options.partition_percent) {
            let until = self.now + self.faults.range(20..=500);
            let state = &mut self.clients[client];
            state.cut_until = state.cut_until.max(until);
        }
        self.requests.insert(
            number,
            Request {
                client,
                caller,
                what,
                floor: None,
                attempt: 0,
                wait: self.options.retry_after,
                sent: self.now,
            },
        );
        self.send(number);
        if more {
            self.clients[client].submit_scheduled = true;
            let delay = self.delays.range(0..=self.options.think);
            self.schedule(delay, Event::Submit { client });
        }
    }

    /// Sends the request's current try, which the network may lose or
    /// duplicate, and arms its timeout.
    fn send(&mut self, number: u64) {
        let request = &self.requests[&number];
        let (client, caller, attempt, wait, what) = (
            request.client,
            request.caller,
            request.attempt,
            request.wait,
            request.what.clone(),
        );
        let copies = if self.faults.percent(self.options.loss_percent) {
            0
        } else if self.faults.percent(self.options.dup_percent) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            let token = self.next_token;
            self.next_token += 1;
            self.attempts.insert(
                token,
                Attempt {
                    request: number,
                    attempt,
                    client,
                    caller,
                    what: what.clone(),
                    generation: None,
                    floor: 0,
                    first_floor: 0,
                    at_least: 0,
                    to_writer: false,
                },
            );
            let delay = self.network_delay();
            self.schedule(delay, Event::Deliver { token });
        }
        self.schedule(
            wait,
            Event::Timeout {
                request: number,
                attempt,
            },
        );
    }

    fn deliver(&mut self, token: u64) {
        let Some(attempt) = self.attempts.get(&token) else {
            return;
        };
        let cut = self.clients[attempt.client].cut_until > self.now;
        let Some(request) = self.requests.get_mut(&attempt.request).filter(|_| !cut) else {
            // Answered already, or the client is cut off.
            self.attempts.remove(&token);
            return;
        };
        let first_floor = *request.floor.get_or_insert(self.acked);
        // Most reads ask for nothing; some for what clients have seen, and
        // a few for a version no writer has made.
        let durable = self.model.as_ref().map_or(0, |model| model.version);
        let at_least = match self.floors.below(10) {
            0..=5 => 0,
            6..=8 => self.acked,
            _ => durable + self.floors.range(1..=3),
        };
        // A client keeps talking to the writer it reached, through a
        // takeover it has yet to notice.
        let client = self.attempts[&token].client;
        let generation = match self.clients[client].writer {
            Some(generation) if self.procs.contains_key(&generation) => generation,
            _ => self.current,
        };
        self.clients[client].writer = Some(generation);
        let attempt = self.attempts.get_mut(&token).expect("checked");
        attempt.generation = Some(generation);
        attempt.floor = self.acked;
        attempt.first_floor = first_floor;
        attempt.at_least = at_least;
        let caller = attempt.caller;
        let mut what = attempt.what.clone();
        let to_writer = attempt.to_writer;
        // Some stats go by inode, as by a handle: the one the path names
        // now, or any an answer has named, inside the key's root or not.
        if let Kindof::Query(Query::Stat(Target::Path(path))) = &what
            && self.handles.percent(30)
            && let Some(model) = &self.model
        {
            let ino = if self.handles.percent(50) {
                self.root_in(caller_key(caller), model)
                    .and_then(|root| model.node_at_path(&[root, path.names.clone()].concat()))
                    .and_then(|node| self.mapping.ino(node))
            } else {
                self.mapping.any(&mut self.handles)
            };
            if let Some(ino) = ino {
                what = Kindof::Query(Query::Stat(Target::Ino(ino)));
                self.attempts.get_mut(&token).expect("checked").what = what.clone();
            }
        }
        // Some reads at a version storage holds go to a standby that
        // follows the log; it waits for any newer version.
        if let Kindof::Query(query) = &what
            && at_least > 0
            && at_least <= durable
            && !to_writer
            && self.standby_reads.percent(50)
        {
            let ready: Vec<usize> = (0..self.standbys.len())
                .filter(|index| self.standbys[*index].follower.is_some())
                .collect();
            if !ready.is_empty() {
                let index = ready[self.standby_reads.below(ready.len() as u64) as usize];
                let attempt = self.attempts.get_mut(&token).expect("checked");
                attempt.generation = None;
                let follower = self.standbys[index].follower.as_mut().expect("follows");
                follower.query(token, caller, query.clone(), at_least);
                return;
            }
        }
        let writer = &mut self.procs.get_mut(&generation).expect("runs").writer;
        match what {
            Kindof::Batch(batch) => writer.submit(token, caller, batch),
            Kindof::Query(query) => writer.query(token, caller, query, at_least),
            Kindof::Key(op) => writer.submit_key(token, caller, op),
        }
    }

    /// How long a client retries: less than a request record lasts, as
    /// a real client's deadline is, so every retry finds its first answer.
    fn give_up(&self) -> u64 {
        if self.options.request_ttl == FOREVER {
            200_000
        } else {
            self.options.request_ttl / 2
        }
    }

    fn timeout(&mut self, number: u64, attempt: u32) {
        let give_up = self.give_up();
        let Some(request) = self.requests.get_mut(&number) else {
            return;
        };
        if request.attempt != attempt {
            return;
        }
        if self.now - request.sent > give_up {
            // The batch may have applied or not; its copies still in flight
            // are answered and checked, and nobody waits on them.
            let client = request.client;
            self.requests.remove(&number);
            self.summary.given_up += 1;
            let state = &mut self.clients[client];
            state.outstanding -= 1;
            if state.remaining > 0 && !state.submit_scheduled {
                state.submit_scheduled = true;
                let delay = self.delays.range(0..=self.options.think);
                self.schedule(delay, Event::Submit { client });
            }
            return;
        }
        request.attempt += 1;
        request.wait = (request.wait * 2).min(2_000);
        self.summary.retries += 1;
        // A client whose writer went quiet looks for the newest again.
        let client = request.client;
        self.clients[client].writer = None;
        self.send(number);
    }

    fn reply(
        &mut self,
        number: u64,
        attempt: u32,
        result: Option<CommitResult>,
    ) -> Result<(), Failure> {
        let Some(request) = self.requests.get(&number) else {
            return Ok(());
        };
        if request.attempt != attempt || self.clients[request.client].cut_until > self.now {
            return Ok(());
        }
        let request = self.requests.remove(&number).expect("checked");
        if let (Kindof::Batch(batch), Some(result)) = (&request.what, result) {
            let id = recorded(batch, request.caller);
            if !result.committed
                && self.options.request_ttl == FOREVER
                && self.commits.contains_key(&id)
            {
                return Err(self.fail(format!(
                    "a client heard that request {id:x} changed nothing, but it committed"
                )));
            }
            self.accepted.insert(id, result);
        }
        let state = &mut self.clients[request.client];
        state.outstanding -= 1;
        if state.remaining > 0 && !state.submit_scheduled {
            state.submit_scheduled = true;
            let delay = self.delays.range(0..=self.options.think);
            self.schedule(
                delay,
                Event::Submit {
                    client: request.client,
                },
            );
        }
        Ok(())
    }

    /// The writer dies. A commit on its way to storage lands or not;
    /// requests on their way to it are lost, and it answers nothing more.
    /// A new writer starts over the storage, and clients retry.
    fn crash(&mut self) -> Result<(), Failure> {
        let dead = self.current;
        self.procs.remove(&dead);
        self.summary.crashes += 1;
        for (key, event) in std::mem::take(&mut self.events) {
            match event {
                Event::StorageCommit {
                    generation,
                    expect,
                    writes,
                    ..
                } if generation == dead => {
                    let before = self.storage.head_now();
                    if self.faults.percent(50)
                        && self.storage.commit_now(expect.as_deref(), &writes)
                            == CommitOutcome::Committed
                    {
                        self.durable(&writes)?;
                        self.landed(dead, before)?;
                    }
                }
                Event::StorageRead { generation, .. }
                | Event::ReadDone { generation, .. }
                | Event::CommitDone { generation, .. }
                | Event::StorageResolve { generation, .. }
                | Event::ResolveDone { generation, .. }
                    if generation == dead => {}
                Event::Deliver { token } => {
                    self.attempts.remove(&token);
                }
                other => {
                    self.events.insert(key, other);
                }
            }
        }
        self.attempts
            .retain(|_, attempt| attempt.generation != Some(dead));
        // A later writer loads stored leases with fresh lifetimes; what the
        // dead one was asked goes unanswered, and its claims end.
        let unanswered: Vec<(u64, LeaseCall)> = self
            .lease_calls
            .extract_if(.., |_, (generation, _)| *generation == dead)
            .map(|(token, (_, call))| (token, call))
            .collect();
        for (token, call) in unanswered {
            match call {
                LeaseCall::Open { watcher, .. } => {
                    if self.watchers[watcher].lease == Held::Opening(token) {
                        self.watchers[watcher].lease = Held::None;
                    }
                }
                LeaseCall::Beat { .. } => {}
                LeaseCall::Claim | LeaseCall::Ack { .. } => self.consuming = false,
            }
        }
        self.claim = None;
        // A writer that holds the store and lives goes on holding it, and
        // clients turn to it once it is ready. Otherwise the dead writer's
        // replacement takes over at once, from the head as it stands.
        if self.holder != dead && self.procs.contains_key(&self.holder) {
            self.current = self.holder;
        } else {
            self.generation += 1;
            let generation = self.generation;
            let clock = Clock::drawn(&mut self.takeover_prng, self.options.drift_ppm);
            let takeover = if self.procs.is_empty() && self.standbys.is_empty() {
                Takeover::Now
            } else {
                Takeover::From(self.storage.head_now())
            };
            let writer = Writer::new(
                self.config.clone(),
                generation,
                clock.at(self.now),
                [generation as u8; 32],
            )
            .after(takeover);
            self.procs.insert(
                generation,
                Proc {
                    writer,
                    clock,
                    stalled_until: 0,
                },
            );
            self.current = generation;
        }
        self.crashes_left -= 1;
        if self.crashes_left > 0 && self.unfinished() {
            let delay = self.faults.range(1..=self.options.crash_every);
            self.schedule(delay, Event::Crash);
        }
        Ok(())
    }

    fn actions(&mut self) -> Result<(), Failure> {
        loop {
            let mut drained = Vec::new();
            for (generation, proc) in &mut self.procs {
                let actions = proc.writer.drain();
                if !actions.is_empty() {
                    drained.push((*generation, proc.stalled_until, actions));
                }
            }
            let mut followed = Vec::new();
            for (index, standby) in self.standbys.iter_mut().enumerate() {
                if let Some(follower) = &mut standby.follower {
                    let actions = follower.drain();
                    if !actions.is_empty() {
                        followed.push((index, standby.era, actions));
                    }
                }
            }
            if drained.is_empty() && followed.is_empty() {
                return Ok(());
            }
            for (generation, stalled_until, actions) in drained {
                self.carry_out(generation, stalled_until, actions)?;
            }
            for (standby, era, actions) in followed {
                for action in actions {
                    match action {
                        Action::Read { id, reads } => {
                            let delay = self.standby_delay(true);
                            self.schedule(
                                delay,
                                Event::StandbyRead {
                                    standby,
                                    era,
                                    id,
                                    reads,
                                },
                            );
                        }
                        Action::Answer { token, answer } => self.answer(None, token, answer)?,
                        other => {
                            return Err(self.fail(format!("a standby asked for {other:?}")));
                        }
                    }
                }
            }
        }
    }

    /// Schedules a writer's storage calls, held past any stall, and takes
    /// its answers.
    fn carry_out(
        &mut self,
        generation: u64,
        stalled_until: u64,
        actions: Vec<Action>,
    ) -> Result<(), Failure> {
        let stall = stalled_until.saturating_sub(self.now);
        {
            for action in actions {
                match action {
                    Action::Read { id, reads } => {
                        let delay = self.storage_delay().max(stall);
                        self.schedule(
                            delay,
                            Event::StorageRead {
                                generation,
                                id,
                                reads,
                            },
                        );
                    }
                    Action::Commit { id, expect, writes } => {
                        self.summary.transactions += 1;
                        let delay = self.storage_delay().max(stall);
                        self.schedule(
                            delay,
                            Event::StorageCommit {
                                generation,
                                id,
                                expect,
                                writes,
                            },
                        );
                    }
                    Action::Resolve { id } => {
                        let delay = self.storage_delay().max(stall);
                        self.schedule(delay, Event::StorageResolve { generation, id });
                    }
                    Action::Answer { token, answer } => {
                        self.answer(Some(generation), token, answer)?
                    }
                }
            }
        }
        Ok(())
    }

    /// Advances the model through a commit storage just made durable.
    fn durable(&mut self, writes: &WriteSet) -> Result<(), Failure> {
        let mut parts: BTreeMap<u128, BTreeMap<u16, Vec<u8>>> = BTreeMap::new();
        for write in &writes.writes {
            if let keys::KvWrite::Set(key, value) = write
                && let Some(Parsed::System(b'r', rest)) = keys::parse(key)
                && rest.len() == 18
            {
                let id = u128::from_be_bytes(rest[..16].try_into().expect("16 bytes"));
                let part = u16::from_be_bytes(rest[16..].try_into().expect("2 bytes"));
                parts.entry(id).or_default().insert(part, value.clone());
            }
        }
        let mut by_version: BTreeMap<Version, u128> = BTreeMap::new();
        for (id, chunks) in parts {
            let bytes: Vec<u8> = chunks.into_values().flatten().collect();
            let result = CommitResult::decode(&bytes)
                .map_err(|_| self.fail("a request record doesn't decode"))?;
            if by_version.insert(result.version, id).is_some() {
                return Err(self.fail(format!(
                    "two requests committed at version {}",
                    result.version
                )));
            }
        }
        for (version, entry) in &writes.log {
            let entry =
                LogEntry::decode(entry).map_err(|_| self.fail("a log entry doesn't decode"))?;
            if entry.version != *version {
                return Err(self.fail("a log entry's version disagrees with its key"));
            }
            self.summary.commits += 1;
            if *version == 1 && self.model.is_none() {
                self.model = Some(Model::new(entry.time));
                self.history
                    .insert(1, self.model.clone().expect("just set"));
                self.publish(&entry);
                continue;
            }
            let Some(id) = by_version.get(version).copied() else {
                self.unrecorded(&entry)?;
                continue;
            };
            if let Some(earlier) = self.commits.get(&id).and_then(|commits| commits.last())
                && self.live(earlier.time, entry.time)
            {
                return Err(self.fail(format!(
                    "request {id:x} committed at {version} after committing at {}",
                    earlier.version
                )));
            }
            if self.options.request_ttl == FOREVER
                && self
                    .accepted
                    .get(&id)
                    .is_some_and(|result| !result.committed)
            {
                return Err(self.fail(format!(
                    "request {id:x} committed after a client heard it changed nothing"
                )));
            }
            let Some(batch) = self.batches.get(&id).cloned() else {
                return Err(self.fail(format!("version {version} commits unknown request {id:x}")));
            };
            let Some(model) = self.model.as_mut() else {
                return Err(self.fail("a commit before bootstrap"));
            };
            if model.version + 1 != *version {
                let at = model.version;
                return Err(self.fail(format!(
                    "version {version} commits after model version {at}"
                )));
            }
            let root = match self.callers.get(&id) {
                None => Vec::new(),
                Some(key) => match self
                    .keys
                    .get(key)
                    .and_then(|incarnations| key_node(incarnations, model.version))
                    .and_then(|node| model.path_of(node))
                {
                    Some(root) => root,
                    None => {
                        return Err(self.fail(format!(
                            "version {version} commits a batch under key {key:x}, whose root is gone"
                        )));
                    }
                },
            };
            let live = live_keys(&self.keys, model);
            let (outcomes, changed) = model.apply(&root, &batch, *version, entry.time);
            // A batch leaves the keys' incarnations as they were.
            let live_after: BTreeSet<KeyId> = self
                .keys
                .iter()
                .filter(|(_, incarnations)| {
                    key_node(incarnations, *version - 1)
                        .is_some_and(|node| model.path_of(node).is_some())
                })
                .map(|(id, _)| *id)
                .collect();
            if !changed {
                return Err(self.fail(format!("version {version} committed a batch the model says changes nothing: {outcomes:?}")));
            }
            // A commit revokes exactly the keys whose roots it removed.
            let gone: BTreeSet<KeyId> = live.difference(&live_after).copied().collect();
            let mut revoked = BTreeSet::new();
            for (key, record) in &entry.keys {
                if record.is_some() || !revoked.insert(*key) {
                    return Err(self.fail(format!(
                        "version {version} changes key {key:x} as a batch never does"
                    )));
                }
            }
            if revoked != gone {
                return Err(self.fail(format!(
                    "version {version} revoked {revoked:x?}; its removals took {gone:x?}"
                )));
            }
            self.revoked.extend(revoked);
            // Each name the commit set tells which inode a node is.
            for effect in &entry.entries {
                let (Some(after), Some(dir)) = (effect.after, self.mapping.node(effect.dir)) else {
                    continue;
                };
                let Some(mut path) = model.path_of(dir) else {
                    continue;
                };
                path.push(effect.name.clone());
                if let Some(node) = model.node_at(&path)
                    && let Err(why) = self.mapping.bind(after.child, node)
                {
                    return Err(self.fail(format!("version {version} names {path:?}: {why}")));
                }
            }
            self.history.insert(*version, model.clone());
            self.commits.entry(id).or_default().push(Committed {
                version: *version,
                outcomes,
                time: entry.time,
            });
            self.publish(&entry);
        }
        self.check_storage()
    }

    /// Advances the model through a commit without a request record: a
    /// key op's, which mints keys, or the writer's own, which frees
    /// detached subtrees and changes nothing a path reaches.
    fn unrecorded(&mut self, entry: &LogEntry) -> Result<(), Failure> {
        let version = entry.version;
        let Some(model) = self.model.as_mut() else {
            return Err(self.fail("a commit before bootstrap"));
        };
        if model.version + 1 != version {
            let at = model.version;
            return Err(self.fail(format!(
                "version {version} commits after model version {at}"
            )));
        }
        // Freeing a detached subtree removes the links inside it. A file
        // that keeps a name outside is reached through that name alone.
        for effect in &entry.entries {
            if let (Some(before), None) = (effect.before, effect.after)
                && before.kind == Kind::File
                && let Some(id) = self.mapping.node(before.child)
            {
                let named = !model.paths_of(id).is_empty();
                if let Some(node) = model.nodes.get_mut(&id) {
                    node.detached_links = node.detached_links.saturating_sub(1);
                    node.version = version;
                    if node.detached_links == 0 && named {
                        node.detached_under.clear();
                    }
                }
            }
        }
        let reached = entry.entries.iter().map(|effect| effect.dir).find(|ino| {
            self.mapping
                .node(*ino)
                .is_some_and(|node| model.path_of(node).is_some())
        });
        if let Some(ino) = reached {
            return Err(self.fail(format!(
                "version {version}, without a request record, changes directory {ino}, which a path reaches"
            )));
        }
        for effect in &entry.inodes {
            let (Some(after), Some(node)) = (effect.after, self.mapping.node(effect.ino)) else {
                continue;
            };
            if model.path_of(node).is_none() {
                continue;
            }
            // A file a path still reaches loses only its detached links.
            if after.kind != Kind::File || after.nlink != model.nlink(node) {
                return Err(self.fail(format!(
                    "version {version}, without a request record, changes inode {}, which a path reaches, to {after:?}",
                    effect.ino
                )));
            }
        }
        if entry.keys.iter().any(|(_, record)| record.is_none()) {
            return Err(self.fail(format!("version {version} revoked a key by hand")));
        }
        for (id, record) in &entry.keys {
            let Some(record) = record else {
                unreachable!("checked above");
            };
            let Some(path) = self.mints.get(id) else {
                return Err(self.fail(format!("version {version} minted unknown key {id:x}")));
            };
            let Some(node) = model.node_at(path).filter(|node| model.is_dir(*node)) else {
                return Err(self.fail(format!(
                    "version {version} minted key {id:x} at {path:?}, which names no directory"
                )));
            };
            let incarnations = self.keys.entry(*id).or_default();
            if key_node(incarnations, version).is_some_and(|node| model.path_of(node).is_some()) {
                return Err(self.fail(format!("version {version} minted live key {id:x} again")));
            }
            incarnations.push((version, node));
            if let Err(why) = self.mapping.bind(record.root, node) {
                return Err(self.fail(format!("key {id:x}'s root: {why}")));
            }
            self.summary.keys += 1;
        }
        model.advance(version);
        self.history.insert(version, model.clone());
        self.publish(entry);
        Ok(())
    }

    /// The path a caller's paths resolve from, in `model`; None for a key
    /// whose root the model no longer holds.
    fn root_in(
        &self,
        caller: Option<KeyId>,
        model: &Model,
    ) -> Option<Vec<inodedb_core::types::Name>> {
        match caller {
            None => Some(Vec::new()),
            Some(key) => self
                .keys
                .get(&key)
                .and_then(|incarnations| key_node(incarnations, model.version))
                .and_then(|node| model.path_of(node)),
        }
    }

    fn check_storage(&self) -> Result<(), Failure> {
        let dump = self.storage.dump();
        let problems = fsck::check(
            dump.iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        );
        if problems.is_empty() {
            Ok(())
        } else {
            Err(self.fail(format!("fsck: {}", problems.join("; "))))
        }
    }

    fn snapshot(&self, version: Version) -> Result<&Model, Failure> {
        self.history
            .get(&version)
            .ok_or_else(|| self.fail(format!("no model at version {version}")))
    }

    /// Takes an answer from writer `generation`, or from a standby.
    fn answer(
        &mut self,
        generation: Option<u64>,
        token: u64,
        answer: Answer,
    ) -> Result<(), Failure> {
        // Property 4: a writer answers nothing from its state once a
        // successor's commit has landed.
        // Openings and acknowledgments of entries answer once their commit
        // lands, as committed batches do.
        let committed = match self.lease_calls.get(&token) {
            Some((_, LeaseCall::Open { .. })) => true,
            Some((_, LeaseCall::Ack { entries })) => !entries.is_empty(),
            _ => false,
        };
        let read = match &answer {
            Answer::Commit(result) => !result.committed,
            Answer::Key(result) => result.is_err(),
            Answer::Lease(_) | Answer::Acked(_) => !committed,
            Answer::Query { .. }
            | Answer::NotYet { .. }
            | Answer::Denied(_)
            | Answer::Claimed { .. } => true,
            Answer::Rejected(_) | Answer::Unavailable | Answer::NotWriter { .. } => false,
        };
        if read
            && let Some(generation) = generation
            && let Some(&at) = self.superseded.get(&generation)
        {
            return Err(self.fail(format!(
                "writer {generation} answered a read after its successor's commit at {at}: {answer:?}"
            )));
        }
        if let Some((_, call)) = self.lease_calls.remove(&token) {
            return self.lease_answer(call, answer);
        }
        let Some(attempt) = self.attempts.remove(&token) else {
            return Err(self.fail(format!("an answer for unknown token {token}")));
        };
        let result = match (&attempt.what, answer) {
            // A writer that stopped answers nothing more; the client tries
            // the newest.
            (_, Answer::Unavailable) => {
                self.clients[attempt.client].writer = None;
                return Ok(());
            }
            // A standby sends what it can't answer to the writer.
            (Kindof::Query(_), Answer::NotWriter { .. }) if generation.is_none() => {
                self.attempts.insert(
                    token,
                    Attempt {
                        to_writer: true,
                        ..attempt
                    },
                );
                let delay = self.standby_delay(false);
                self.schedule(delay, Event::Deliver { token });
                return Ok(());
            }
            (Kindof::Batch(batch), Answer::Commit(result)) => {
                self.check_commit(&attempt, batch, &result)?;
                Some(result)
            }
            (Kindof::Batch(batch), Answer::Rejected(error)) => {
                self.summary.rejected += 1;
                let expected = if batch.ops.len() > MAX_OPS {
                    Some(BatchError::TooManyOps)
                } else if Model::malformed(batch) {
                    Some(BatchError::Malformed)
                } else {
                    None
                };
                if expected != Some(error) {
                    return Err(self.fail(format!(
                        "batch rejected with {error:?}, expected {expected:?}"
                    )));
                }
                None
            }
            (Kindof::Query(query), Answer::Query { version, result }) => {
                self.summary.queries += 1;
                if generation.is_none() {
                    self.summary.standby_queries += 1;
                }
                if version < attempt.at_least {
                    return Err(self.fail(format!(
                        "a read at {version} that asked for at least {}",
                        attempt.at_least
                    )));
                }
                self.check_query(&attempt, query, version, result)?;
                None
            }
            (Kindof::Key(KeyOp::Mint { id, .. }), Answer::Key(result)) => {
                match result {
                    Ok(_) | Err(KeyError::Exists) => {
                        if !self.keys.contains_key(id) {
                            return Err(self.fail(format!(
                                "key {id:x} answered as minted, which storage never made durable"
                            )));
                        }
                        for client in &mut self.clients {
                            if let Some(key) = &mut client.key
                                && key.id == *id
                            {
                                key.minted = true;
                            }
                        }
                    }
                    Err(KeyError::Fs(_)) => {}
                    Err(other) => {
                        return Err(self.fail(format!("minting key {id:x} failed: {other:?}")));
                    }
                }
                None
            }
            (Kindof::Batch(_) | Kindof::Query(_), Answer::Denied(denied)) => {
                let Caller::Key { id, .. } = attempt.caller else {
                    return Err(self.fail(format!("a trusted caller was denied: {denied:?}")));
                };
                if denied != Denied::Unknown || !self.revoked.contains(&id) {
                    return Err(self.fail(format!(
                        "key {id:x} was denied ({denied:?}) while its root stands"
                    )));
                }
                self.summary.denied += 1;
                for client in &mut self.clients {
                    if let Some(key) = &mut client.key
                        && key.id == id
                    {
                        key.minted = false;
                    }
                }
                None
            }
            (Kindof::Query(_), Answer::NotYet { version }) => {
                let durable = self.model.as_ref().map_or(0, |model| model.version);
                if attempt.at_least <= version || version < attempt.floor || version > durable {
                    return Err(self.fail(format!(
                        "a read asking for {} heard the writer is at {version}, with {} acknowledged",
                        attempt.at_least, attempt.floor
                    )));
                }
                None
            }
            (what, answer) => {
                return Err(self.fail(format!("unexpected answer {answer:?} to {what:?}")));
            }
        };
        let client = &self.clients[attempt.client];
        if client.cut_until > self.now || self.faults.percent(self.options.loss_percent) {
            return Ok(());
        }
        let delay = self.network_delay();
        self.schedule(
            delay,
            Event::Reply {
                request: attempt.request,
                attempt: attempt.attempt,
                result,
            },
        );
        Ok(())
    }

    fn check_commit(
        &mut self,
        attempt: &Attempt,
        batch: &Batch,
        result: &CommitResult,
    ) -> Result<(), Failure> {
        self.summary.batches += 1;
        if Model::malformed(batch) {
            return Err(self.fail("a malformed batch was run"));
        }
        let id = recorded(batch, attempt.caller);
        let generation = attempt.generation.expect("delivered");
        if self.options.request_ttl == FOREVER {
            match self.answers.get(&(generation, id)) {
                Some(first) if first != result => {
                    return Err(self.fail(format!(
                        "request {id:x} answered {result:?}, first {first:?}"
                    )));
                }
                Some(_) => {}
                None => {
                    self.answers.insert((generation, id), result.clone());
                }
            }
        }
        if result.committed {
            let Some(commit) = self
                .commits
                .get(&id)
                .and_then(|commits| {
                    commits
                        .iter()
                        .find(|commit| commit.version == result.version)
                })
                .cloned()
            else {
                return Err(self.fail(format!(
                    "request {id:x} answered as committed at {}, which storage never made durable",
                    result.version
                )));
            };
            if result.version <= attempt.first_floor {
                return Err(self.fail(format!(
                    "version {} after version {} was acknowledged",
                    result.version, attempt.first_floor
                )));
            }
            self.compare_outcomes(&commit.outcomes, result)?;
            self.acked = self.acked.max(commit.version);
            self.versions_seen.push(commit.version);
        } else {
            self.summary.skipped_batches += 1;
            // A retry may get the answer of an earlier copy, which ran after
            // the first copy arrived.
            if result.version < attempt.first_floor {
                return Err(self.fail(format!(
                    "a batch saw version {} after {} was acknowledged",
                    result.version, attempt.first_floor
                )));
            }
            let mut model = self.snapshot(result.version)?.clone();
            let key = self.callers.get(&id).copied();
            let Some(root) = self.root_in(key, &model) else {
                return Err(self.fail(format!(
                    "a batch under key {key:x?} ran at {}, after its root went",
                    result.version
                )));
            };
            let (outcomes, changed) = model.apply(&root, batch, result.version + 1, self.now);
            if changed {
                return Err(self.fail(format!(
                    "a batch the model says changes the tree at {} committed nothing: {outcomes:?}",
                    result.version
                )));
            }
            self.compare_outcomes(&outcomes, result)?;
        }
        Ok(())
    }

    fn compare_outcomes(
        &mut self,
        expected: &[ModelOutcome],
        result: &CommitResult,
    ) -> Result<(), Failure> {
        if expected.len() != result.outcomes.len() {
            return Err(self.fail(format!(
                "{} outcomes, expected {}",
                result.outcomes.len(),
                expected.len()
            )));
        }
        for (index, (model, actual)) in expected.iter().zip(&result.outcomes).enumerate() {
            if let Err(why) = model.matches(actual, &mut self.mapping) {
                return Err(self.fail(format!(
                    "op {index} at version {}: {why}; all: {:?}",
                    result.version, result.outcomes
                )));
            }
        }
        Ok(())
    }

    fn check_query(
        &mut self,
        attempt: &Attempt,
        query: &Query,
        version: Version,
        result: QueryResult,
    ) -> Result<(), Failure> {
        if version < attempt.floor {
            return Err(self.fail(format!(
                "a read at {version} after {} was acknowledged",
                attempt.floor
            )));
        }
        let model = self.snapshot(version)?.clone();
        let key = caller_key(attempt.caller);
        let Some(root) = self.root_in(key, &model) else {
            return Err(self.fail(format!(
                "a read under key {key:x?} at {version}, after its root went"
            )));
        };
        // A stat by inode reaches what any of its names puts under the
        // key's root, nothing a name outside puts, and possibly a file no
        // name holds, which reads until it is freed.
        if let Query::Stat(Target::Ino(ino)) = query {
            let node = self.mapping.node(*ino).expect("asked by a bound inode");
            let paths = model.paths_of(node);
            let expected = if paths.iter().any(|path| path.starts_with(&root)) {
                ModelAnswer::Stat(node)
            } else if !paths.is_empty() {
                ModelAnswer::Failed(Error::NotFound)
            } else {
                // A file unlinked, or a directory removed, reads by inode
                // until it is freed, for keys above a name it last had or a
                // link of it that a removed subtree held; one not yet made
                // doesn't.
                let root_node = model.node_at_path(&root);
                let reached = model.nodes.get(&node).filter(|known| {
                    key.is_none()
                        || root_node.is_some_and(|top| {
                            known.lost_under.contains(&top) || known.detached_under.contains(&top)
                        })
                });
                return match (result, reached) {
                    (QueryResult::Failed(Error::NotFound), _) => Ok(()),
                    (QueryResult::Stat { ino: got, inode }, Some(known))
                        if got == *ino
                            && inode.kind == known.kind
                            && (known.kind == Kind::Dir
                                || inode.size == known.value.len() as u64) =>
                    {
                        Ok(())
                    }
                    (other, _) => Err(self.fail(format!(
                        "a stat of unnamed inode {ino} at {version} by key {key:x?} under {root:?} answered {other:?}; the model has {:?}",
                        model.nodes.get(&node)
                    ))),
                };
            };
            return compare_answer(&mut self.mapping, &model, query, &expected, &result).map_err(
                |why| {
                    self.fail(format!(
                        "{query:?} at {version}: {why}; got {result:?}, expected {expected:?}"
                    ))
                },
            );
        }
        let expected = model.query(&root, query);
        compare_answer(&mut self.mapping, &model, query, &expected, &result).map_err(|why| {
            self.fail(format!(
                "{query:?} at {version}: {why}; got {result:?}, expected {expected:?}"
            ))
        })
    }
}

/// Compares the core's answer with the model's, binding inodes to nodes.
fn compare_answer(
    mapping: &mut Mapping,
    model: &Model,
    query: &Query,
    expected: &ModelAnswer,
    result: &QueryResult,
) -> Result<(), String> {
    match (expected, result) {
        (ModelAnswer::Failed(want), QueryResult::Failed(got)) if want == got => Ok(()),
        (ModelAnswer::Stat(node), QueryResult::Stat { ino, inode }) => {
            mapping.bind(*ino, *node)?;
            check_inode(model, *node, inode)
        }
        (
            ModelAnswer::Read(node, value),
            QueryResult::Read {
                ino,
                inode,
                value: got,
            },
        ) => {
            mapping.bind(*ino, *node)?;
            check_inode(model, *node, inode)?;
            if value == got {
                Ok(())
            } else {
                Err("values differ".into())
            }
        }
        (
            ModelAnswer::List(entries, more),
            QueryResult::List {
                entries: got,
                more: got_more,
                ..
            },
        ) => {
            if entries.len() != got.len() || more != got_more {
                return Err("listings differ in length".into());
            }
            let Query::List { attrs, values, .. } = query else {
                unreachable!("a list answers a list");
            };
            for ((name, named), listed) in entries.iter().zip(got) {
                if *name != listed.name {
                    return Err("names differ".into());
                }
                mapping.bind(listed.entry.child, named.node)?;
                let node = &model.nodes[&named.node];
                if listed.entry.kind != node.kind || listed.entry.version != named.entry_version {
                    return Err(format!("entry {name:?} differs"));
                }
                if *attrs {
                    let Some(inode) = &listed.inode else {
                        return Err("attributes missing".into());
                    };
                    check_inode(model, named.node, inode)?;
                }
                if *values && node.kind == Kind::File && listed.value.as_ref() != Some(&node.value)
                {
                    return Err(format!("value of {name:?} differs"));
                }
            }
            Ok(())
        }
        (ModelAnswer::Many(expected), QueryResult::Many(results)) => {
            let Query::Many(queries) = query else {
                unreachable!("many answers many");
            };
            if expected.len() != results.len() {
                return Err("a different number of answers".into());
            }
            for ((query, expected), result) in queries.iter().zip(expected).zip(results) {
                compare_answer(mapping, model, query, expected, result)?;
            }
            Ok(())
        }
        _ => Err("answers differ".into()),
    }
}

fn check_inode(
    model: &Model,
    node: NodeId,
    inode: &inodedb_core::types::Inode,
) -> Result<(), String> {
    let expected = &model.nodes[&node];
    let nlink = match expected.kind {
        Kind::File => model.nlink(node),
        Kind::Dir => 1,
    };
    let size = match expected.kind {
        Kind::File => expected.value.len() as u64,
        Kind::Dir => 0,
    };
    let fields = (
        inode.kind,
        inode.version,
        inode.mtime,
        inode.mode,
        inode.nlink,
        inode.size,
    );
    let want = (
        expected.kind,
        expected.version,
        expected.mtime,
        expected.mode,
        nlink,
        size,
    );
    if fields != want || inode.life != Life::Live {
        return Err(format!(
            "inode {fields:?} {:?}, expected {want:?}",
            inode.life
        ));
    }
    Ok(())
}

/// A seed from a number, or from the first 16 hex digits of a commit hash.
pub fn parse_seed(text: &str) -> Result<u64, String> {
    if let Ok(seed) = text.parse() {
        return Ok(seed);
    }
    match text.get(..16) {
        Some(prefix) if text.chars().all(|c| c.is_ascii_hexdigit()) => {
            u64::from_str_radix(prefix, 16).map_err(|error| error.to_string())
        }
        _ => Err(format!("{text:?} is neither a number nor a commit hash")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_parse_from_numbers_and_hashes() {
        assert_eq!(parse_seed("42"), Ok(42));
        assert_eq!(
            parse_seed("0123456789abcdef0123456789abcdef01234567"),
            Ok(0x0123_4567_89ab_cdef)
        );
        assert!(parse_seed("main").is_err());
    }
}
