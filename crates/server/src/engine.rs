//! The writer's task: it owns the core's writer, carries out the actions
//! it returns over a storage backend, and routes answers back to the
//! requests waiting on them.

use inodedb_core::auth::{Caller, Denied, Grant, KeyOp, Scope};
use inodedb_core::delta::Freed;
use inodedb_core::keys::{self, KvRead, KvResult, WriteSet};
use inodedb_core::lease::{ClaimId, LeaseId, LeaseRecord};
use inodedb_core::log::LogEntry;
use inodedb_core::ops::Batch;
use inodedb_core::query::Query;
use inodedb_core::standby::Follower;
use inodedb_core::types::{Millis, Version};
use inodedb_core::writer::{Action, Answer, CommitOutcome, Config, Takeover, Token, Writer};
use inodedb_storage::{Storage, StorageError};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tonic::Status;

enum Command {
    Submit {
        caller: Caller,
        batch: Batch,
        reply: oneshot::Sender<Answer>,
    },
    Key {
        caller: Caller,
        op: KeyOp,
        reply: oneshot::Sender<Answer>,
    },
    Query {
        caller: Caller,
        query: Query,
        at_least: Version,
        reply: oneshot::Sender<Answer>,
    },
    Authorize {
        caller: Caller,
        need: Grant,
        reply: oneshot::Sender<Result<Scope, Status>>,
    },
    Lease {
        caller: Caller,
        lease: Option<LeaseId>,
        record: LeaseRecord,
        /// Opens at the committed version in place of the record's.
        current: bool,
        release: bool,
        reply: oneshot::Sender<Answer>,
    },
    Claim {
        caller: Caller,
        limit: usize,
        reply: oneshot::Sender<Answer>,
    },
    Ack {
        caller: Caller,
        claim: ClaimId,
        entries: Vec<(Version, Freed)>,
        reply: oneshot::Sender<Answer>,
    },
    Ready {
        reply: oneshot::Sender<Ready>,
    },
    ReadDone {
        era: u64,
        id: u64,
        results: Vec<KvResult>,
    },
    CommitDone {
        era: u64,
        id: u64,
        outcome: CommitOutcome,
    },
    ResolveDone {
        era: u64,
        id: u64,
        head: Option<Vec<u8>>,
    },
    /// The writer acts on the time alone, and a standby reads the head.
    Tick,
    /// A standby's read of the head record.
    Head {
        era: u64,
        head: Option<Vec<u8>>,
    },
    /// The log's next entries, for a standby.
    Entries {
        era: u64,
        entries: Vec<(Version, Vec<u8>)>,
    },
}

/// How often the engine acts on the time alone: a writer renews its lease
/// and commits heartbeats, and a standby reads the head record.
const TICK: Duration = Duration::from_millis(100);

/// What a server that can answer reports: a started writer, or a standby
/// that has read the store.
#[derive(Clone, Copy, Debug)]
pub struct Ready {
    /// The secret handles' MACs are made under.
    pub secret: [u8; 32],
    /// How many keys the store holds.
    pub keys: usize,
}

/// A handle on the engine's task, which runs the writer or a standby.
#[derive(Clone)]
pub struct Engine {
    commands: mpsc::Sender<Command>,
    stopped: watch::Receiver<bool>,
    committed: watch::Receiver<Version>,
    keep: watch::Receiver<Option<Keep>>,
}

/// What log truncation must keep, which only a writer within its lease
/// knows, since it alone holds every lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Keep {
    /// The lowest changefeed position a live lease reports.
    pub position: Option<Version>,
}

/// The metadata key that names the writer's address on `NotWriter`.
pub const WRITER_KEY: &str = "inodedb-writer";

/// The status a standby answers what only the writer takes with.
pub fn not_writer(address: &str) -> Status {
    let mut status = Status::unavailable("this server is a standby; ask the writer");
    status.metadata_mut().insert(
        crate::service::ERROR_KEY,
        tonic::metadata::MetadataValue::from_static("not-writer"),
    );
    if let Ok(address) = address.parse() {
        status.metadata_mut().insert(WRITER_KEY, address);
    }
    status
}

/// Milliseconds since the epoch by the wall clock.
fn wall() -> Millis {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as Millis)
        .unwrap_or(0)
}

/// Milliseconds since the epoch, kept by a monotonic timer from the wall
/// time at the first call. It never runs fast, so a standby that waits on
/// it waits at least as long as it means to; it stops while the host
/// sleeps, which only lengthens the wait.
fn steady() -> Millis {
    static START: std::sync::OnceLock<(std::time::Instant, Millis)> = std::sync::OnceLock::new();
    let (start, at) = START.get_or_init(|| (std::time::Instant::now(), wall()));
    at + start.elapsed().as_millis() as Millis
}

/// The writer's time: the later of the timer and the wall clock, so it
/// never runs slow. A wall clock stepped back leaves its lease as long as
/// it was; one stepped forward, or a host that slept, ends the lease early.
fn writer_now() -> Millis {
    steady().max(wall())
}

fn stopped() -> Status {
    Status::unavailable("the writer has stopped")
}

impl Engine {
    /// Starts the engine's task, which ends once every `Engine` drops. On
    /// a store that admits `standby`s it follows the log until the writer
    /// stops renewing, then takes over, and follows again once fenced;
    /// otherwise it writes at once and stops once fenced.
    pub fn spawn<S: Storage + Clone>(
        storage: S,
        config: Config,
        standby: bool,
    ) -> (Engine, JoinHandle<()>) {
        let (commands, inbox) = mpsc::channel(8192);
        let (stop, stopped) = watch::channel(false);
        let (publish, committed) = watch::channel(0);
        let (keeps, keep) = watch::channel(None);
        let task = tokio::spawn(run(
            storage,
            config,
            standby,
            commands.downgrade(),
            inbox,
            stop,
            Published {
                committed: publish,
                keep: keeps,
            },
        ));
        (
            Engine {
                commands,
                stopped,
                committed,
                keep,
            },
            task,
        )
    }

    /// What log truncation must keep while this server writes within its
    /// lease, and None otherwise, as it changes.
    pub fn keep(&self) -> watch::Receiver<Option<Keep>> {
        self.keep.clone()
    }

    /// The writer's committed version, as it changes.
    pub fn committed(&self) -> watch::Receiver<Version> {
        self.committed.clone()
    }

    /// Resolves once the writer stops for good: fenced, on a server with no
    /// standby role to fall back to.
    pub async fn stopped(&self) {
        let mut stopped = self.stopped.clone();
        let _ = stopped.wait_for(|stopped| *stopped).await;
    }

    async fn ask<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<T, Status> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())
    }

    /// Asks for an answer, which a standby may give as `NotWriter`.
    async fn answer(
        &self,
        command: impl FnOnce(oneshot::Sender<Answer>) -> Command,
    ) -> Result<Answer, Status> {
        match self.ask(command).await? {
            Answer::NotWriter { address } => Err(not_writer(&address)),
            answer => Ok(answer),
        }
    }

    pub async fn submit(&self, caller: Caller, batch: Batch) -> Result<Answer, Status> {
        self.answer(|reply| Command::Submit {
            caller,
            batch,
            reply,
        })
        .await
    }

    pub async fn key(&self, caller: Caller, op: KeyOp) -> Result<Answer, Status> {
        self.answer(|reply| Command::Key { caller, op, reply })
            .await
    }

    pub async fn query(
        &self,
        caller: Caller,
        query: Query,
        at_least: Version,
    ) -> Result<Answer, Status> {
        self.answer(|reply| Command::Query {
            caller,
            query,
            at_least,
            reply,
        })
        .await
    }

    /// The caller's scope under the committed keys.
    pub async fn authorize(&self, caller: Caller, need: Grant) -> Result<Scope, Status> {
        self.ask(|reply| Command::Authorize {
            caller,
            need,
            reply,
        })
        .await?
    }

    /// Opens a lease (`lease` None), beats it, or releases it.
    pub async fn lease(
        &self,
        caller: Caller,
        lease: Option<LeaseId>,
        record: LeaseRecord,
        current: bool,
        release: bool,
    ) -> Result<Answer, Status> {
        self.answer(|reply| Command::Lease {
            caller,
            lease,
            record,
            current,
            release,
            reply,
        })
        .await
    }

    pub async fn claim(&self, caller: Caller, limit: usize) -> Result<Answer, Status> {
        self.answer(|reply| Command::Claim {
            caller,
            limit,
            reply,
        })
        .await
    }

    pub async fn ack(
        &self,
        caller: Caller,
        claim: ClaimId,
        entries: Vec<(Version, Freed)>,
    ) -> Result<Answer, Status> {
        self.answer(|reply| Command::Ack {
            caller,
            claim,
            entries,
            reply,
        })
        .await
    }

    /// Waits for the writer to start.
    pub async fn ready(&self) -> Result<Ready, Status> {
        self.ask(|reply| Command::Ready { reply }).await
    }
}

/// What the engine's task tells the server as it runs.
struct Published {
    committed: watch::Sender<Version>,
    keep: watch::Sender<Option<Keep>>,
}

/// Retries a storage call until it succeeds: the writer has no answer for
/// a read or resolution that failed, so it waits.
async fn persist<T, F, Fut>(mut call: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StorageError>>,
{
    let mut delay = Duration::from_millis(10);
    loop {
        match call().await {
            Ok(value) => return value,
            Err(error) => {
                eprintln!("level=warn msg=\"storage call failed\" error=\"{error}\"");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(1));
            }
        }
    }
}

/// What the server is now: a standby following the log, or the writer.
enum Role {
    Follow(Box<Follower>),
    Write(Box<Writer>),
}

/// The engine's task state.
struct Node<S> {
    storage: S,
    config: Config,
    /// Whether the store admits standbys, so a fenced writer follows again
    /// rather than stopping.
    standby: bool,
    commands: mpsc::WeakSender<Command>,
    role: Role,
    /// Counts roles, so a storage call's result reaches only the role that
    /// asked for it.
    era: u64,
    waiting: BTreeMap<Token, oneshot::Sender<Answer>>,
    failures: Arc<AtomicU32>,
    /// Whether a read of the head is in flight.
    polling: bool,
    /// The task reading the log for the follower.
    following: Option<JoinHandle<()>>,
}

fn random_secret() -> [u8; 32] {
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).expect("the system's random source");
    secret
}

/// A writer with a random id, which tells its commits from any earlier
/// writer's.
fn writer(config: &Config, secret: [u8; 32]) -> Writer {
    let id = getrandom::u64().expect("the system's random source");
    Writer::new(config.clone(), id, writer_now(), secret)
}

impl<S: Storage + Clone> Node<S> {
    fn follow(&mut self) {
        self.era += 1;
        self.polling = false;
        if let Some(task) = self.following.take() {
            task.abort();
        }
        let follower = Role::Follow(Box::new(Follower::new(self.config.clone(), steady())));
        // A follower starting over takes the reads its predecessor kept.
        if let (Role::Follow(old), Role::Follow(new)) =
            (std::mem::replace(&mut self.role, follower), &mut self.role)
        {
            for read in old.hand_over().waiting {
                new.query(read.token, read.caller, read.query, read.at_least);
            }
        }
    }

    /// Becomes the writer, with the follower's cache and its waiting reads.
    fn take_over(&mut self, takeover: Takeover) {
        let Role::Follow(follower) = std::mem::replace(
            &mut self.role,
            Role::Follow(Box::new(Follower::new(self.config.clone(), steady()))),
        ) else {
            return;
        };
        eprintln!("level=info msg=\"taking over as the writer\"");
        let secret = follower.handle_secret().unwrap_or_else(random_secret);
        let handover = (*follower).hand_over();
        let mut writer = writer(&self.config, secret)
            .after(takeover)
            .warm(handover.cache, handover.version);
        for read in handover.waiting {
            writer.query(read.token, read.caller, read.query, read.at_least);
        }
        self.era += 1;
        self.polling = false;
        if let Some(task) = self.following.take() {
            task.abort();
        }
        self.role = Role::Write(Box::new(writer));
    }

    /// Answers a request only the writer takes.
    fn not_writer(follower: &Follower, reply: oneshot::Sender<Answer>) {
        let _ = reply.send(Answer::NotWriter {
            address: follower.address(),
        });
    }

    fn handle(&mut self, command: Command, next: &mut Token) {
        let waiting = &mut self.waiting;
        match (&mut self.role, command) {
            (
                Role::Write(writer),
                Command::Submit {
                    caller,
                    batch,
                    reply,
                },
            ) => {
                writer.submit(token(waiting, next, reply), caller, batch);
            }
            (Role::Write(writer), Command::Key { caller, op, reply }) => {
                writer.submit_key(token(waiting, next, reply), caller, op);
            }
            (
                Role::Write(writer),
                Command::Lease {
                    caller,
                    lease,
                    record,
                    current,
                    release,
                    reply,
                },
            ) => {
                let token = token(waiting, next, reply);
                match lease {
                    None => writer.open_lease(token, caller, (!current).then_some(record)),
                    Some(lease) if release => writer.release_lease(token, caller, lease),
                    Some(lease) => writer.beat_lease(token, caller, lease, record),
                }
            }
            (
                Role::Write(writer),
                Command::Claim {
                    caller,
                    limit,
                    reply,
                },
            ) => {
                writer.claim(token(waiting, next, reply), caller, limit);
            }
            (
                Role::Write(writer),
                Command::Ack {
                    caller,
                    claim,
                    entries,
                    reply,
                },
            ) => {
                writer.acknowledge(token(waiting, next, reply), caller, claim, &entries);
            }
            (
                Role::Follow(follower),
                Command::Submit { reply, .. }
                | Command::Key { reply, .. }
                | Command::Lease { reply, .. }
                | Command::Claim { reply, .. }
                | Command::Ack { reply, .. },
            ) => Node::<S>::not_writer(follower, reply),
            (
                role,
                Command::Query {
                    caller,
                    query,
                    at_least,
                    reply,
                },
            ) => {
                let token = token(waiting, next, reply);
                match role {
                    Role::Write(writer) => writer.query(token, caller, query, at_least),
                    Role::Follow(follower) => follower.query(token, caller, query, at_least),
                }
            }
            (
                Role::Write(writer),
                Command::Authorize {
                    caller,
                    need,
                    reply,
                },
            ) => {
                // A starting writer has yet to load the keys.
                let _ = reply.send(if writer.is_ready() {
                    writer.authorize(&caller, need).map_err(crate::auth::denied)
                } else {
                    Err(Status::unavailable("the writer is starting"))
                });
            }
            (
                Role::Follow(follower),
                Command::Authorize {
                    caller,
                    need,
                    reply,
                },
            ) => {
                // A key the standby doesn't know may be newer than the log it
                // has read, so the writer, which knows every key, decides.
                let _ = reply.send(if !follower.is_started() {
                    Err(Status::unavailable("the standby is starting"))
                } else {
                    match follower.authorize(&caller, need) {
                        Err(Denied::Unknown) => Err(not_writer(&follower.address())),
                        other => other.map_err(crate::auth::denied),
                    }
                });
            }
            (_, Command::Ready { .. }) => unreachable!("the run loop keeps these"),
            (role, Command::ReadDone { era, id, results }) if era == self.era => match role {
                Role::Write(writer) => writer.read_done(id, results),
                Role::Follow(follower) => {
                    if follower.read_done(id, results).is_err() {
                        eprintln!(
                            "level=error msg=\"storage holds what the standby can't read; starting over\""
                        );
                        self.follow();
                    }
                }
            },
            (Role::Write(writer), Command::CommitDone { era, id, outcome }) if era == self.era => {
                writer.commit_done(id, outcome);
            }
            (Role::Write(writer), Command::ResolveDone { era, id, head }) if era == self.era => {
                writer.resolve_done(id, head);
            }
            (Role::Write(writer), Command::Tick) => writer.tick(),
            (Role::Follow(follower), Command::Tick) => {
                if follower.is_started() && !self.polling {
                    self.polling = true;
                    let (storage, commands, era) =
                        (self.storage.clone(), self.commands.clone(), self.era);
                    tokio::spawn(async move {
                        let mut results =
                            persist(|| storage.snapshot_read(vec![KvRead::Get(keys::head())]))
                                .await;
                        let head = match results.pop() {
                            Some(KvResult::Get(head)) => head,
                            _ => None,
                        };
                        report(commands, Command::Head { era, head }).await;
                    });
                }
            }
            (Role::Follow(follower), Command::Head { era, head }) if era == self.era => {
                self.polling = false;
                match follower.observe(head) {
                    Ok(Some(takeover)) => self.take_over(takeover),
                    Ok(None) => {}
                    Err(_) => eprintln!("level=error msg=\"the head record doesn't decode\""),
                }
            }
            (Role::Follow(follower), Command::Entries { era, entries }) if era == self.era => {
                for (version, bytes) in entries {
                    match LogEntry::decode(&bytes) {
                        Ok(entry)
                            if version == follower.version() + 1 && entry.version == version =>
                        {
                            follower.apply(&entry);
                        }
                        Ok(_) if version <= follower.version() => {}
                        // The log no longer holds where the follower is.
                        _ => {
                            eprintln!(
                                "level=warn msg=\"the standby fell behind the log; starting over\""
                            );
                            return self.follow();
                        }
                    }
                }
            }
            // A result for an earlier role, or for nothing this role asks.
            _ => {}
        }
    }

    /// Carries out the role's actions, and starts following the log once
    /// a follower knows where it continues.
    fn carry_out(&mut self) {
        if let Role::Follow(follower) = &self.role
            && follower.is_started()
            && self.following.is_none()
        {
            let (storage, commands, era) = (self.storage.clone(), self.commands.clone(), self.era);
            let from = follower.version() + 1;
            self.following = Some(tokio::spawn(follow_log(storage, commands, era, from)));
        }
        loop {
            let actions = match &mut self.role {
                Role::Write(writer) => writer.drain(),
                Role::Follow(follower) => follower.drain(),
            };
            if actions.is_empty() {
                return;
            }
            for action in actions {
                let era = self.era;
                match action {
                    Action::Read { id, reads } => {
                        let (storage, commands) = (self.storage.clone(), self.commands.clone());
                        tokio::spawn(async move {
                            let results = persist(|| storage.snapshot_read(reads.clone())).await;
                            report(commands, Command::ReadDone { era, id, results }).await;
                        });
                    }
                    Action::Commit { id, expect, writes } => {
                        let (storage, commands) = (self.storage.clone(), self.commands.clone());
                        let failures = self.failures.clone();
                        tokio::spawn(async move {
                            let outcome = commit(&storage, expect, writes, &failures).await;
                            report(commands, Command::CommitDone { era, id, outcome }).await;
                        });
                    }
                    Action::Resolve { id } => {
                        let (storage, commands) = (self.storage.clone(), self.commands.clone());
                        tokio::spawn(async move {
                            let head = persist(|| inodedb_storage::resolve(&storage)).await;
                            report(commands, Command::ResolveDone { era, id, head }).await;
                        });
                    }
                    Action::Answer { token, answer } => {
                        if let Some(reply) = self.waiting.remove(&token) {
                            let _ = reply.send(answer);
                        }
                    }
                }
            }
        }
    }

    /// What a server that can answer reports, once it can.
    fn ready(&self) -> Option<Ready> {
        match &self.role {
            Role::Write(writer) if writer.is_ready() => Some(Ready {
                secret: writer.handle_secret(),
                keys: writer.keys().len(),
            }),
            Role::Follow(follower) if follower.is_started() => {
                follower.handle_secret().map(|secret| Ready {
                    secret,
                    keys: follower.keys(),
                })
            }
            _ => None,
        }
    }
}

/// Holds a reply under the next token.
fn token(
    waiting: &mut BTreeMap<Token, oneshot::Sender<Answer>>,
    next: &mut Token,
    reply: oneshot::Sender<Answer>,
) -> Token {
    let token = *next;
    *next += 1;
    waiting.insert(token, reply);
    token
}

/// Reads the log from `from` on for a follower, waiting for more at its
/// end, until the engine is gone.
async fn follow_log<S: Storage>(
    storage: S,
    commands: mpsc::WeakSender<Command>,
    era: u64,
    mut from: Version,
) {
    loop {
        let entries = persist(|| storage.read_log(from, LOG_BATCH)).await;
        let Some(&(last, _)) = entries.last() else {
            persist(|| storage.wait_past(from - 1)).await;
            continue;
        };
        from = last + 1;
        let Some(commands) = commands.upgrade() else {
            return;
        };
        if commands
            .send(Command::Entries { era, entries })
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Log entries a follower reads at once.
const LOG_BATCH: usize = 256;

async fn run<S: Storage + Clone>(
    storage: S,
    config: Config,
    standby: bool,
    commands: mpsc::WeakSender<Command>,
    mut inbox: mpsc::Receiver<Command>,
    stop: watch::Sender<bool>,
    published: Published,
) {
    let role = if standby {
        Role::Follow(Box::new(Follower::new(config.clone(), steady())))
    } else {
        Role::Write(Box::new(writer(&config, random_secret())))
    };
    let mut node = Node {
        storage,
        config,
        standby,
        commands: commands.clone(),
        role,
        era: 0,
        waiting: BTreeMap::new(),
        failures: Arc::new(AtomicU32::new(0)),
        polling: false,
        following: None,
    };
    let mut readying: Vec<oneshot::Sender<Ready>> = Vec::new();
    let mut next_token: Token = 1;
    // The era whose writer has said it writes.
    let mut announced = None;
    tokio::spawn({
        let commands = commands.clone();
        async move {
            let mut ticks = tokio::time::interval(TICK);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                let Some(commands) = commands.upgrade() else {
                    return;
                };
                if commands.send(Command::Tick).await.is_err() {
                    return;
                }
            }
        }
    });
    node.carry_out();
    while let Some(command) = inbox.recv().await {
        match &mut node.role {
            Role::Write(writer) => writer.set_now(writer_now()),
            Role::Follow(follower) => follower.set_now(steady()),
        }
        match command {
            Command::Ready { reply } => readying.push(reply),
            command => node.handle(command, &mut next_token),
        }
        node.carry_out();
        if let Role::Write(writer) = &node.role
            && writer.is_stopped()
        {
            if node.standby {
                eprintln!("level=warn msg=\"another writer took over; following it\"");
                node.follow();
                node.carry_out();
            } else {
                readying.clear();
                stop.send_replace(true);
            }
        }
        if let Role::Write(writer) = &node.role
            && writer.is_ready()
            && announced != Some(node.era)
        {
            announced = Some(node.era);
            eprintln!(
                "level=info msg=writing epoch={} version={}",
                writer.head().epoch,
                writer.version()
            );
        }
        let (version, keep) = match &node.role {
            Role::Write(writer) => (
                writer.version(),
                (writer.is_ready() && writer.is_fresh()).then(|| Keep {
                    position: writer.lease_position(),
                }),
            ),
            Role::Follow(follower) => (follower.version(), None),
        };
        published.committed.send_if_modified(|published| {
            let changed = *published != version;
            *published = version;
            changed
        });
        published.keep.send_if_modified(|published| {
            let changed = *published != keep;
            *published = keep;
            changed
        });
        if let Some(ready) = node.ready() {
            for reply in readying.drain(..) {
                let _ = reply.send(ready);
            }
        }
    }
}

/// Sends a storage call's result back to the engine, if it still runs.
async fn report(commands: mpsc::WeakSender<Command>, command: Command) {
    if let Some(commands) = commands.upgrade() {
        let _ = commands.send(command).await;
    }
}

/// Commits a group. A failure waits before the writer hears of it, longer
/// after each failure in a row, since the writer runs the group again at
/// once.
async fn commit<S: Storage>(
    storage: &S,
    expect: Option<Vec<u8>>,
    writes: WriteSet,
    failures: &AtomicU32,
) -> CommitOutcome {
    let outcome = inodedb_storage::commit(storage, expect.as_deref(), &writes).await;
    match outcome {
        // An unknown outcome often resolves as a failure, which the writer
        // runs again at once, so it waits as a failure does.
        CommitOutcome::Failed | CommitOutcome::Unknown => {
            let streak = failures.fetch_add(1, Ordering::AcqRel).min(7);
            eprintln!(
                "level=warn msg=\"commit failed or went unknown\" outcome={outcome:?} streak={}",
                streak + 1
            );
            tokio::time::sleep(Duration::from_millis(10 << streak)).await;
        }
        CommitOutcome::Fenced => {
            eprintln!("level=error msg=\"another writer committed; stopping\"");
        }
        CommitOutcome::Committed => failures.store(0, Ordering::Release),
    }
    outcome
}
