//! A cache of the key's tree, kept current by `Watch`.
//!
//! A background task follows the changefeed into a subscriber from the
//! core. Reads answer from the cache at the version it holds; a miss lists
//! the directory it lacks and fills the cache with what comes back, at the
//! version the listing reflects. When a listing is newer than the feed, the
//! read answers from the server and the listing fills the cache once the
//! feed catches up. A listing of some other directory than the cache
//! expects, as when the feed has yet to show a rename, fills nothing.
//!
//! Handles carry a MAC bound to the key, so the cache keeps each inode's
//! MAC from the handles the server sends.
//!
//! A snapshot reads the cache as of the version it opened at, through the
//! subscriber's undo records. It fetches what it misses by handle and
//! waits for the feed to reach the version the answer reflects, so every
//! change since the snapshot has its undo record.

use crate::{Client, Entry, Error, FsError, Handle, Inode, Lease, List, Listing, Value, Version};
use bytes::Bytes;
use inodedb_core::cache::{Budget, Cache};
use inodedb_core::feed::{Change, Subscriber};
use inodedb_core::log::{EntryEffect, InodeEffect};
use inodedb_core::query::{self, Listed, Query, QueryResult, Target};
use inodedb_core::snapshot::Miss;
use inodedb_core::types::{self as core, Ino, Name, Path};
use inodedb_core::view::{Fact, Read, View};
use inodedb_proto as pb;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

struct State {
    subscriber: Subscriber,
    /// What the subscriber may hold.
    budget: Budget,
    /// The key root's inode, once known.
    root: Option<Ino>,
    /// The MAC of each inode's handle, as the server sent it.
    macs: BTreeMap<Ino, Bytes>,
    /// Why the cache stopped: the server refused its key.
    failed: Option<(tonic::Code, String)>,
    /// Counts the subscribers the cache has started, so a snapshot of an
    /// earlier one knows it ended.
    generation: u64,
}

/// MACs kept beyond what the cache holds before pruning.
const SPARE_MACS: usize = 65_536;

impl State {
    fn learn(&mut self, handle: &Option<pb::Handle>) {
        if let Some(handle) = handle {
            self.macs.insert(handle.ino, handle.mac.clone());
        }
    }

    fn learn_listing(&mut self, listing: &Listing) {
        self.learn(&Some(listing.dir.to_pb()));
        for entry in &listing.entries {
            self.learn(&Some(entry.handle.to_pb()));
        }
    }

    fn learn_change(&mut self, change: &pb::Change) {
        for effect in &change.entries {
            self.learn(&effect.dir);
            for listed in effect.before.iter().chain(&effect.after) {
                self.learn(&listed.handle);
            }
        }
        for effect in &change.inodes {
            self.learn(&effect.handle);
            for handle in &effect.holders {
                self.learn(&Some(handle.clone()));
            }
        }
        for handle in &change.entered {
            self.learn(&Some(handle.clone()));
        }
    }

    /// Forgets the MACs of inodes neither the cache nor a snapshot's
    /// records name, once there are many more MACs than those.
    fn prune(&mut self) {
        let (cache, undo) = (self.subscriber.cache(), self.subscriber.undo());
        // Snapshots need the MACs of what only their undo records name.
        if self.macs.len() > 2 * (cache.inodes() + undo.mentioned()) + SPARE_MACS {
            self.macs
                .retain(|ino, _| cache.holds(*ino) || undo.mentions(*ino));
        }
    }

    fn handle(&self, ino: Ino) -> pb::Handle {
        pb::Handle {
            ino,
            mac: self.macs.get(&ino).cloned().unwrap_or_default(),
        }
    }

    /// A listing the cache answered, at `version`.
    fn listing(&self, dir: Ino, entries: Vec<Listed>, version: Version) -> Listing {
        Listing {
            version,
            newest: version,
            dir: Handle::from_pb(Some(self.handle(dir))),
            entries: entries
                .into_iter()
                .map(|listed| {
                    let handle = self.handle(listed.entry.child);
                    Entry::from_pb(pb::Listed {
                        name: listed.name.to_vec().into(),
                        kind: match listed.entry.kind {
                            core::Kind::Dir => pb::Kind::Dir,
                            core::Kind::File => pb::Kind::File,
                        } as i32,
                        handle: Some(handle.clone()),
                        entry_version: listed.entry.version,
                        inode: listed
                            .inode
                            .map(|inode| to_pb(&client_inode(handle, &inode))),
                        value: listed.value,
                    })
                })
                .collect(),
        }
    }
}

/// A client whose reads answer from a local cache.
pub struct Cached {
    client: Client,
    state: Arc<Mutex<State>>,
    version: watch::Receiver<Version>,
    task: JoinHandle<()>,
    hits: AtomicU64,
}

impl Drop for Cached {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn kind(kind: i32) -> core::Kind {
    match pb::Kind::try_from(kind) {
        Ok(pb::Kind::File) => core::Kind::File,
        _ => core::Kind::Dir,
    }
}

fn core_inode(inode: &pb::Inode) -> core::Inode {
    core::Inode {
        kind: kind(inode.kind),
        version: inode.version,
        mtime: inode.mtime,
        mode: inode.mode,
        nlink: inode.nlink,
        size: inode.size,
        value: inode.value_version,
        parent: 0,
        life: match pb::Life::try_from(inode.life) {
            Ok(pb::Life::Detached) => core::Life::Detached(inode.version),
            Ok(pb::Life::Freed) => core::Life::Freed(inode.version),
            _ => core::Life::Live,
        },
    }
}

fn core_entry(listed: &pb::Listed) -> core::Entry {
    core::Entry {
        child: listed.handle.as_ref().map_or(0, |handle| handle.ino),
        kind: kind(listed.kind),
        version: listed.entry_version,
    }
}

fn ino(handle: &Option<pb::Handle>) -> Ino {
    handle.as_ref().map_or(0, |handle| handle.ino)
}

fn change(change: &pb::Change) -> Change {
    Change {
        version: change.version,
        time: change.time,
        entries: change
            .entries
            .iter()
            .map(|effect| EntryEffect {
                dir: ino(&effect.dir),
                name: Name::from(&effect.name[..]),
                before: effect.before.as_ref().map(core_entry),
                after: effect.after.as_ref().map(core_entry),
            })
            .collect(),
        inodes: change
            .inodes
            .iter()
            .map(|effect| InodeEffect {
                ino: ino(&effect.handle),
                before: effect.before.as_ref().map(core_inode),
                after: effect.after.as_ref().map(core_inode),
                holders: effect.holders.iter().map(|handle| handle.ino).collect(),
            })
            .collect(),
        entered: change.entered.iter().map(|handle| handle.ino).collect(),
        ..Change::default()
    }
}

fn client_inode(handle: pb::Handle, inode: &core::Inode) -> Inode {
    Inode::from_pb(pb::Inode {
        handle: Some(handle),
        kind: match inode.kind {
            core::Kind::Dir => pb::Kind::Dir,
            core::Kind::File => pb::Kind::File,
        } as i32,
        version: inode.version,
        mtime: inode.mtime,
        mode: inode.mode,
        nlink: inode.nlink,
        size: inode.size,
        value_version: inode.value,
        life: match inode.life {
            core::Life::Live => pb::Life::Live,
            core::Life::Detached(_) => pb::Life::Detached,
            core::Life::Freed(_) => pb::Life::Freed,
        } as i32,
    })
}

fn failed(error: core::Error) -> Error {
    Error::Fs(match error {
        core::Error::NotFound => FsError::NotFound,
        core::Error::Exists => FsError::Exists,
        core::Error::NotDir => FsError::NotDir,
        core::Error::IsDir => FsError::IsDir,
        core::Error::NotEmpty => FsError::NotEmpty,
        core::Error::Invalid => FsError::Invalid,
        core::Error::TooLarge => FsError::TooLarge,
    })
}

fn parse(path: impl AsRef<[u8]>) -> Result<Path, Error> {
    Path::parse(path.as_ref()).map_err(|_| Error::Fs(FsError::Invalid))
}

impl Client {
    /// A cached view of this client's tree, within the default budget. It
    /// follows the changefeed in the background until dropped.
    pub fn cached(&self) -> Cached {
        self.cached_within(Budget::default())
    }

    /// A cached view of this client's tree that holds no more than
    /// `budget`.
    pub fn cached_within(&self, budget: Budget) -> Cached {
        let state = Arc::new(Mutex::new(State {
            subscriber: Subscriber::with_budget(0, 0, budget),
            budget,
            root: None,
            macs: BTreeMap::new(),
            failed: None,
            generation: 0,
        }));
        let (publish, version) = watch::channel(0);
        let task = tokio::spawn(follow(self.clone(), state.clone(), publish));
        Cached {
            client: self.clone(),
            state,
            version,
            task,
            hits: AtomicU64::new(0),
        }
    }
}

/// Whether the server refused the key itself, which no retry mends.
fn refused(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
    )
}

/// Stops the cache for good, as when its key is revoked.
fn stop(state: &Mutex<State>, status: &tonic::Status) {
    let mut state = state.lock().expect("no panics holding the cache");
    state.failed = Some((status.code(), status.message().to_string()));
    state.root = None;
}

/// Longest between heartbeats of the cache's lease.
const HEARTBEAT: Duration = Duration::from_secs(10);

/// What the heartbeat task tells the follow loop.
#[derive(Default)]
struct Beating {
    /// The oldest version the last heartbeat reported, which the next may
    /// not lower.
    reported: Version,
    /// The lease ran out, or the server refused the key.
    lost: bool,
}

/// A task that stops when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Renews the cache's lease on its own schedule, whatever the feed's
/// stream does, reporting the oldest version the cache and its snapshots
/// use and the feed's position.
async fn heartbeat(
    mut lease: Lease,
    state: Arc<Mutex<State>>,
    beating: Arc<Mutex<Beating>>,
    lost: Arc<Notify>,
) {
    loop {
        let every = (lease.expires_in() / 3)
            .min(HEARTBEAT)
            .max(Duration::from_millis(1));
        tokio::time::sleep(every).await;
        let (oldest, position) = {
            let state = state.lock().expect("no panics holding the cache");
            (state.subscriber.oldest(), state.subscriber.version())
        };
        let oldest = oldest.max(beating.lock().expect("no panics").reported);
        match lease.beat(oldest, position).await {
            Ok(()) => beating.lock().expect("no panics").reported = oldest,
            Err(Error::Status(status)) if refused(&status) => {
                stop(&state, &status);
                beating.lock().expect("no panics").lost = true;
                lost.notify_one();
                return;
            }
            Err(Error::LeaseExpired) => {
                beating.lock().expect("no panics").lost = true;
                lost.notify_one();
                return;
            }
            // A lost stream resumes at the next heartbeat.
            Err(_) => {}
        }
    }
}

/// Follows the feed: resumes after a lost stream, and starts over empty
/// when the log no longer holds the subscriber's position or its lease
/// runs out. The lease holds the log past the cache's position and keeps
/// freed records readable for handles the cache gave out.
async fn follow(client: Client, state: Arc<Mutex<State>>, publish: watch::Sender<Version>) {
    let mut delay = Duration::from_millis(20);
    let mut position: Option<Version> = None;
    let beating = Arc::new(Mutex::new(Beating::default()));
    let lost = Arc::new(Notify::new());
    let mut leased: Option<AbortOnDrop> = None;
    loop {
        if state.lock().expect("no panics").failed.is_some() {
            return;
        }
        if std::mem::take(&mut beating.lock().expect("no panics").lost) {
            leased = None;
            position = None;
        }
        if position.is_none() {
            let start = match client.stat("").await {
                Err(Error::Status(status)) if refused(&status) => return stop(&state, &status),
                Ok(root) if leased.is_some() => {
                    let reported = beating.lock().expect("no panics").reported;
                    Ok((root, client.version().max(reported)))
                }
                Ok(root) => match client.lease_current().await {
                    Ok(lease) => {
                        let start = lease.oldest();
                        beating.lock().expect("no panics").reported = start;
                        let task = heartbeat(lease, state.clone(), beating.clone(), lost.clone());
                        leased = Some(AbortOnDrop(tokio::spawn(task)));
                        Ok((root, start))
                    }
                    Err(Error::Status(status)) if refused(&status) => {
                        return stop(&state, &status);
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            match start {
                Ok((root, start)) => {
                    {
                        let mut state = state.lock().expect("no panics holding the cache");
                        state.subscriber =
                            Subscriber::with_budget(root.handle.ino, start, state.budget);
                        state.generation += 1;
                        state.root = Some(root.handle.ino);
                        state.macs.clear();
                        state.learn(&Some(root.handle.to_pb()));
                    }
                    position = Some(start);
                    publish.send_replace(start);
                }
                Err(_) => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                    continue;
                }
            }
        }
        let from = position.expect("set above") + 1;
        let mut rpc = client.rpc();
        let stream = rpc
            .watch(client.request(pb::WatchRequest {
                path: Bytes::new(),
                from,
            }))
            .await;
        let mut changes = match stream {
            Ok(response) => response.into_inner(),
            Err(status) if refused(&status) => return stop(&state, &status),
            Err(_) => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
                continue;
            }
        };
        // The stream's timeout covers only its headers, so the stream runs
        // with none; HTTP/2 keepalive notices a dead server.
        loop {
            let next = tokio::select! {
                next = changes.message() => next,
                () = lost.notified() => break,
            };
            let next = match next {
                Ok(Some(next)) => next,
                Err(status) if refused(&status) => return stop(&state, &status),
                Ok(None) | Err(_) => break,
            };
            if next.compacted {
                position = None;
                break;
            }
            let applied = {
                let mut state = state.lock().expect("no panics holding the cache");
                state.learn_change(&next);
                state.subscriber.apply(&change(&next));
                state.prune();
                state.subscriber.version()
            };
            position = Some(applied);
            publish.send_replace(applied);
            delay = Duration::from_millis(20);
        }
        if !beating.lock().expect("no panics").lost {
            tokio::time::sleep(delay).await;
        }
    }
}

/// The inodes a path's names reach through what the cache holds, from the
/// root: the root first, then one per name until a name the cache lacks.
fn reached(view: &dyn View, root: Ino, path: &Path) -> Vec<Ino> {
    let mut inos = vec![root];
    for name in &path.names {
        match view.entry(*inos.last().expect("starts with the root"), name) {
            Ok(Some(entry)) => inos.push(entry.child),
            _ => break,
        }
    }
    inos
}

impl Cached {
    /// Reads the cache answered without the server.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// The version the cache reflects.
    pub fn version(&self) -> Version {
        *self.version.borrow()
    }

    /// Waits until the cache reflects at least `version`.
    pub async fn wait_for(&self, version: Version) -> Result<(), Error> {
        let mut versions = self.version.clone();
        let waited = versions
            .wait_for(|current| *current >= version)
            .await
            .map(|_| ());
        // The cache stops by dropping its sender, after saying why.
        self.working()?;
        waited.map_err(|_| Error::Status(tonic::Status::cancelled("the cache stopped")))
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("no panics holding the cache")
    }

    /// Fails once the server has refused the cache's key.
    fn working(&self) -> Result<(), Error> {
        match &self.state().failed {
            Some((code, message)) => Err(Error::Status(tonic::Status::new(*code, message.clone()))),
            None => Ok(()),
        }
    }

    /// Answers from the cache with the version it reflects, or names the
    /// read it lacks.
    #[allow(clippy::type_complexity)]
    fn local(&self, query: &Query) -> Result<(QueryResult, Version), Option<(Read, Vec<Ino>)>> {
        let state = self.state();
        let Some(root) = state.root else {
            return Err(None);
        };
        let cache = state.subscriber.cache();
        let version = state.subscriber.version();
        query::run(cache, root, query)
            .map(|result| (result, version))
            .map_err(|reads| {
                let path = match query {
                    Query::Stat(Target::Path(path))
                    | Query::List {
                        target: Target::Path(path),
                        ..
                    } => path,
                    Query::Read {
                        target: Target::Path(path),
                        ..
                    } => path,
                    _ => return None,
                };
                reads
                    .into_iter()
                    .next()
                    .map(|read| (read, reached(cache, root, path)))
            })
    }

    /// Lists the directory at `names` with attributes and fills the cache,
    /// if `dir` is the directory listed.
    async fn fill_dir(&self, names: &[Name], dir: Ino) -> Result<(), Error> {
        let path = names.join(&b'/');
        let listing = self
            .client
            .list_with(
                path,
                &List {
                    attrs: true,
                    ..List::default()
                },
            )
            .await?;
        self.fill_listing(dir, &listing);
        Ok(())
    }

    fn fill_listing(&self, dir: Ino, listing: &Listing) {
        // A listing over several versions holds no one state, and a
        // listing of another directory says nothing of this one.
        let Some(facts) = listing_facts(dir, listing) else {
            return;
        };
        let mut state = self.state();
        state.learn_listing(listing);
        for fact in facts {
            state.subscriber.fill(fact, listing.version);
        }
    }

    /// Fills what `read` names, for a query on `path`, from the server.
    async fn fill(&self, path: &Path, read: Read, reached: &[Ino]) -> Result<(), Error> {
        let depth = |ino: Ino| reached.iter().position(|reached| *reached == ino);
        match read {
            Read::Entries { dir, .. } | Read::Entry(dir, _) => match depth(dir) {
                Some(depth) => self.fill_dir(&path.names[..depth], dir).await,
                None => Ok(()),
            },
            Read::Inode(ino) => match depth(ino) {
                Some(0) => {
                    let (at, root) = self.client.stat_at(crate::Target::from("")).await?;
                    if root.handle.ino == ino {
                        let mut state = self.state();
                        state.learn(&Some(root.handle.to_pb()));
                        state
                            .subscriber
                            .fill(Fact::Inode(ino, Some(core_inode(&to_pb(&root)))), at);
                    }
                    Ok(())
                }
                Some(depth) => {
                    self.fill_dir(&path.names[..depth - 1], reached[depth - 1])
                        .await
                }
                None => Ok(()),
            },
            Read::Value(ino, version) => {
                let handle = Handle::from_pb(Some(self.state().handle(ino)));
                let (at, value) = self.client.read_at(&handle, version).await?;
                self.state()
                    .subscriber
                    .fill(Fact::Value(ino, version, Some(value.value)), at);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Answers `query` from the cache, filling what it lacks, with the
    /// version the answer reflects; None when the fills land after the
    /// cache's version.
    async fn answer(
        &self,
        path: &Path,
        query: &Query,
    ) -> Result<Option<(QueryResult, Version)>, Error> {
        for round in 0..=path.names.len() + 2 {
            match self.local(query) {
                Ok(answered) => {
                    if round == 0 {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(Some(answered));
                }
                Err(None) => return Ok(None),
                Err(Some((read, reached))) => self.fill(path, read, &reached).await?,
            }
        }
        Ok(None)
    }

    pub async fn stat(&self, path: impl AsRef<[u8]>) -> Result<Inode, Error> {
        self.working()?;
        let parsed = parse(&path)?;
        match self
            .answer(&parsed, &Query::Stat(Target::Path(parsed.clone())))
            .await?
        {
            Some((QueryResult::Stat { ino, inode }, _)) => {
                Ok(client_inode(self.state().handle(ino), &inode))
            }
            Some((QueryResult::Failed(error), _)) => Err(failed(error)),
            _ => self.client.stat(path.as_ref()).await,
        }
    }

    pub async fn read(&self, path: impl AsRef<[u8]>) -> Result<Value, Error> {
        self.working()?;
        let parsed = parse(&path)?;
        let query = Query::Read {
            target: Target::Path(parsed.clone()),
            version: None,
        };
        match self.answer(&parsed, &query).await? {
            Some((QueryResult::Read { ino, inode, value }, _)) => Ok(Value {
                inode: client_inode(self.state().handle(ino), &inode),
                value,
            }),
            Some((QueryResult::Failed(error), _)) => Err(failed(error)),
            _ => self.client.read(path.as_ref()).await,
        }
    }

    /// A directory's entries with their attributes.
    pub async fn list(&self, path: impl AsRef<[u8]>) -> Result<Listing, Error> {
        self.list_with(path, false).await
    }

    /// A directory's entries with their attributes and values.
    pub async fn list_with_values(&self, path: impl AsRef<[u8]>) -> Result<Listing, Error> {
        self.list_with(path, true).await
    }

    async fn list_with(&self, path: impl AsRef<[u8]>, values: bool) -> Result<Listing, Error> {
        self.working()?;
        let parsed = parse(&path)?;
        match self
            .answer(&parsed, &whole_listing(parsed.clone(), values))
            .await?
        {
            Some((QueryResult::List { dir, entries, .. }, version)) => {
                Ok(self.state().listing(dir, entries, version))
            }
            Some((QueryResult::Failed(error), _)) => Err(failed(error)),
            _ => {
                self.client
                    .list_with(
                        path,
                        &List {
                            attrs: true,
                            values,
                            ..List::default()
                        },
                    )
                    .await
            }
        }
    }
}

/// A query for every entry of the directory at `path`, with attributes.
fn whole_listing(path: Path, values: bool) -> Query {
    Query::List {
        target: Target::Path(path),
        after: None,
        prefix: None,
        limit: usize::MAX,
        budget: usize::MAX,
        attrs: true,
        values,
    }
}

/// Rounds of fetching a read through a snapshot may take.
const SNAPSHOT_ROUNDS: usize = 64;

/// The facts a listing with attributes holds, if it reflects one version
/// of `dir`.
fn listing_facts(dir: Ino, listing: &Listing) -> Option<Vec<Fact>> {
    if listing.newest != listing.version || listing.dir.ino != dir {
        return None;
    }
    let mut facts = vec![Fact::Listing {
        dir,
        entries: listing
            .entries
            .iter()
            .map(|entry| {
                let kind = match entry.kind {
                    crate::Kind::Dir => core::Kind::Dir,
                    crate::Kind::File => core::Kind::File,
                };
                let entry_of = core::Entry {
                    child: entry.handle.ino,
                    kind,
                    version: entry.entry_version,
                };
                (Name::from(&entry.name[..]), Some(entry_of))
            })
            .collect(),
        complete: true,
    }];
    for entry in &listing.entries {
        if let Some(inode) = &entry.inode {
            facts.push(Fact::Inode(
                entry.handle.ino,
                Some(core_inode(&to_pb(inode))),
            ));
        }
    }
    Some(facts)
}

impl Cached {
    /// Opens a snapshot at the cache's current version, once the cache
    /// has started. Every read through it sees that version until it
    /// drops, and the cache's lease holds what it reads.
    pub async fn snapshot(&self) -> Result<Snapshot<'_>, Error> {
        let mut versions = self.version.clone();
        loop {
            self.working()?;
            {
                let mut state = self.state();
                if state.root.is_some() {
                    let version = state.subscriber.open_snapshot();
                    return Ok(Snapshot {
                        cached: self,
                        version,
                        generation: state.generation,
                    });
                }
            }
            if versions.changed().await.is_err() {
                self.working()?;
                return Err(Error::Status(tonic::Status::cancelled("the cache stopped")));
            }
        }
    }
}

/// The cache as of one version, until dropped.
pub struct Snapshot<'a> {
    cached: &'a Cached,
    version: Version,
    generation: u64,
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        let mut state = self.cached.state();
        if state.generation == self.generation {
            state.subscriber.close_snapshot(self.version);
        }
    }
}

impl Snapshot<'_> {
    /// The version every read through the snapshot sees.
    pub fn version(&self) -> Version {
        self.version
    }

    pub async fn stat(&self, path: impl AsRef<[u8]>) -> Result<Inode, Error> {
        let parsed = parse(&path)?;
        match self.answer(&Query::Stat(Target::Path(parsed))).await? {
            QueryResult::Stat { ino, inode } => {
                Ok(client_inode(self.cached.state().handle(ino), &inode))
            }
            QueryResult::Failed(error) => Err(failed(error)),
            other => Err(unexpected(other)),
        }
    }

    pub async fn read(&self, path: impl AsRef<[u8]>) -> Result<Value, Error> {
        let parsed = parse(&path)?;
        let query = Query::Read {
            target: Target::Path(parsed),
            version: None,
        };
        match self.answer(&query).await? {
            QueryResult::Read { ino, inode, value } => Ok(Value {
                inode: client_inode(self.cached.state().handle(ino), &inode),
                value,
            }),
            QueryResult::Failed(error) => Err(failed(error)),
            other => Err(unexpected(other)),
        }
    }

    /// A directory's entries with their attributes.
    pub async fn list(&self, path: impl AsRef<[u8]>) -> Result<Listing, Error> {
        self.list_with(path, false).await
    }

    /// A directory's entries with their attributes and values.
    pub async fn list_with_values(&self, path: impl AsRef<[u8]>) -> Result<Listing, Error> {
        self.list_with(path, true).await
    }

    async fn list_with(&self, path: impl AsRef<[u8]>, values: bool) -> Result<Listing, Error> {
        match self.answer(&whole_listing(parse(&path)?, values)).await? {
            QueryResult::List { dir, entries, .. } => {
                Ok(self.cached.state().listing(dir, entries, self.version))
            }
            QueryResult::Failed(error) => Err(failed(error)),
            other => Err(unexpected(other)),
        }
    }

    /// Answers `query` as of the snapshot's version, fetching what the
    /// cache lacks.
    async fn answer(&self, query: &Query) -> Result<QueryResult, Error> {
        self.cached.working()?;
        let mut fetched = Cache::new();
        for _ in 0..SNAPSHOT_ROUNDS {
            let reads = {
                let state = self.cached.state();
                let Some(root) = state.root.filter(|_| state.generation == self.generation) else {
                    return Err(Error::SnapshotTooOld);
                };
                match state.subscriber.run_at(self.version, root, query, &fetched) {
                    Ok(result) => return Ok(result),
                    Err(Miss::TooOld) => return Err(Error::SnapshotTooOld),
                    Err(Miss::Reads(reads)) => reads,
                }
            };
            for read in reads {
                if let Some((at, facts)) = self.fetch(&read).await? {
                    // Once the feed reaches the fetch's version, every
                    // change since the snapshot has its undo record.
                    self.cached.wait_for(at).await?;
                    for fact in facts {
                        fetched.fill(fact);
                    }
                }
            }
        }
        Err(Error::Status(tonic::Status::unavailable(
            "a read through the snapshot kept missing",
        )))
    }

    /// Reads what `read` names from the server by handle, with the version
    /// it reflects; None when the answer spans versions and should be
    /// asked again.
    async fn fetch(&self, read: &Read) -> Result<Option<(Version, Vec<Fact>)>, Error> {
        let client = &self.cached.client;
        // The answers reflect the snapshot's version or a later one.
        client.saw(self.version);
        let handle = |ino: Ino| Handle::from_pb(Some(self.cached.state().handle(ino)));
        let gone = |error: Error| match error {
            Error::Fs(FsError::NotFound) => Error::SnapshotTooOld,
            error => error,
        };
        match read {
            Read::Entries { dir, .. } | Read::Entry(dir, _) => {
                let options = List {
                    attrs: true,
                    ..List::default()
                };
                let listing = client
                    .list_dir(&handle(*dir), &options)
                    .await
                    .map_err(gone)?;
                self.cached.state().learn_listing(&listing);
                Ok(listing_facts(*dir, &listing).map(|facts| (listing.version, facts)))
            }
            Read::Inode(ino) => {
                let (at, inode) = client
                    .stat_at(crate::Target::from(&handle(*ino)))
                    .await
                    .map_err(gone)?;
                Ok(Some((
                    at,
                    vec![Fact::Inode(*ino, Some(core_inode(&to_pb(&inode))))],
                )))
            }
            Read::Value(ino, version) => {
                let (at, value) = client
                    .read_at(&handle(*ino), *version)
                    .await
                    .map_err(gone)?;
                Ok(Some((
                    at,
                    vec![Fact::Value(*ino, *version, Some(value.value))],
                )))
            }
            _ => Err(Error::SnapshotTooOld),
        }
    }
}

fn unexpected(result: QueryResult) -> Error {
    Error::Status(tonic::Status::internal(format!(
        "the cache answered {result:?}"
    )))
}

fn to_pb(inode: &Inode) -> pb::Inode {
    pb::Inode {
        handle: Some(inode.handle.to_pb()),
        kind: match inode.kind {
            crate::Kind::Dir => pb::Kind::Dir,
            crate::Kind::File => pb::Kind::File,
        } as i32,
        version: inode.version,
        mtime: inode.mtime,
        mode: inode.mode,
        nlink: inode.nlink,
        size: inode.size,
        value_version: inode.value_version,
        life: match inode.life {
            crate::Life::Live => pb::Life::Live,
            crate::Life::Detached => pb::Life::Detached,
            crate::Life::Freed => pb::Life::Freed,
        } as i32,
    }
}
