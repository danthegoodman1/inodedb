//! The changefeed's server side: the log as an object the service holds,
//! the newest entries in memory, `Watch` streams, and truncation by age and
//! lease positions.

use crate::auth::Scope;
use crate::convert;
use inodedb_core::auth::KeyId;
use inodedb_core::feed::{Change, route};
use inodedb_core::log::LogEntry;
use inodedb_core::types::{Ino, Millis, Version};
use inodedb_proto as pb;
use inodedb_storage::{Storage, StorageError};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tonic::Status;

type Boxed<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;

/// The parts of a backend the service uses beyond the engine: the log, for
/// streams and truncation, and a snapshot of everything, for checks.
pub trait Log: Send + Sync + 'static {
    fn read(&self, from: Version, limit: usize) -> Boxed<'_, Vec<(Version, Vec<u8>)>>;
    fn wait_past(&self, version: Version) -> Boxed<'_, Version>;
    fn truncate(&self, below: Version) -> Boxed<'_, ()>;
    fn dump(&self) -> Boxed<'_, inodedb_core::keys::Pairs>;
}

impl<S: Storage> Log for S {
    fn read(&self, from: Version, limit: usize) -> Boxed<'_, Vec<(Version, Vec<u8>)>> {
        Box::pin(self.read_log(from, limit))
    }

    fn wait_past(&self, version: Version) -> Boxed<'_, Version> {
        Box::pin(Storage::wait_past(self, version))
    }

    fn truncate(&self, below: Version) -> Boxed<'_, ()> {
        Box::pin(self.truncate_log(below))
    }

    fn dump(&self) -> Boxed<'_, inodedb_core::keys::Pairs> {
        Box::pin(inodedb_storage::dump(self))
    }
}

/// How long the log keeps entries.
#[derive(Clone, Debug)]
pub struct Retention {
    /// Every entry stays at least this long.
    pub min: Duration,
    /// An entry a live lease's position needs stays up to this long.
    pub max: Duration,
    /// How often truncation runs.
    pub every: Duration,
}

impl Default for Retention {
    fn default() -> Retention {
        Retention {
            min: Duration::from_secs(5 * 60),
            max: Duration::from_secs(60 * 60),
            every: Duration::from_secs(10),
        }
    }
}

impl Retention {
    /// Whether the log keeps, at `now`, the entry for `version` written at
    /// `time`, while `position` is the lowest live lease's position. A
    /// lease at `position` has applied every entry up to it.
    pub fn keeps(
        &self,
        now: Millis,
        time: Millis,
        version: Version,
        position: Option<Version>,
    ) -> bool {
        let age = Duration::from_millis(now.saturating_sub(time));
        age < self.min || (age < self.max && position.is_some_and(|position| version > position))
    }
}

/// Entries read from the log per round.
const READ_BATCH: usize = 256;

/// The most entries, and encoded bytes, the tail holds.
const TAIL_ENTRIES: usize = 4_096;
const TAIL_BYTES: usize = 64 << 20;

/// The newest committed log entries, decoded, in version order with no
/// gaps. The engine adds each entry its writer commits or its standby
/// reads, so streams that keep up read neither storage nor a decoder. It
/// serves entries only while they are younger than half the log's minimum
/// retention, which truncation never takes, so a stream sees `Compacted`
/// exactly when storage no longer holds where it starts.
pub struct Tail {
    entries: Mutex<Entries>,
    young: Millis,
}

#[derive(Default)]
struct Entries {
    held: VecDeque<(Version, Arc<LogEntry>, usize)>,
    bytes: usize,
}

impl Tail {
    pub fn new(retention: &Retention) -> Tail {
        Tail {
            entries: Mutex::default(),
            young: u64::try_from(retention.min.as_millis() / 2).unwrap_or(Millis::MAX),
        }
    }

    /// Adds the entries of versions after the newest held, decoding each;
    /// one that leaves a gap starts the tail over from it.
    pub fn extend<'a>(&self, entries: impl IntoIterator<Item = &'a (Version, Vec<u8>)>) {
        let mut tail = self
            .entries
            .lock()
            .expect("no thread panics holding the tail");
        for (version, bytes) in entries {
            let Ok(entry) = decode(*version, bytes) else {
                continue;
            };
            match tail.held.back() {
                Some((last, _, _)) if *version <= *last => continue,
                Some((last, _, _)) if *version != last + 1 => {
                    tail.held.clear();
                    tail.bytes = 0;
                }
                _ => {}
            }
            tail.held
                .push_back((*version, Arc::new(entry), bytes.len()));
            tail.bytes += bytes.len();
            while tail.held.len() > TAIL_ENTRIES || tail.bytes > TAIL_BYTES {
                let Some((_, _, size)) = tail.held.pop_front() else {
                    break;
                };
                tail.bytes -= size;
            }
        }
    }

    /// Up to `limit` entries from version `from` on, if the tail holds it;
    /// none if `from` is the version after the tail's newest, and this
    /// server's reads have yet to reach it, so no entry exists for it yet.
    fn read(
        &self,
        from: Version,
        limit: usize,
        reached: Version,
    ) -> Option<Vec<(Version, Arc<LogEntry>)>> {
        let tail = self
            .entries
            .lock()
            .expect("no thread panics holding the tail");
        let (first, _, _) = tail.held.front()?;
        let skip = usize::try_from(from.checked_sub(*first)?).ok()?;
        if skip == tail.held.len() && reached < from {
            return Some(Vec::new());
        }
        let (_, entry, _) = tail.held.get(skip)?;
        (now().saturating_sub(entry.time) < self.young).then(|| {
            tail.held
                .iter()
                .skip(skip)
                .take(limit)
                .map(|(version, entry, _)| (*version, entry.clone()))
                .collect()
        })
    }
}

/// Up to `limit` entries from version `from` on: from the tail when it
/// holds `from`, else from storage. `reached` is the version this server's
/// reads reach.
async fn entries(
    log: &dyn Log,
    tail: &Tail,
    from: Version,
    limit: usize,
    reached: Version,
) -> Result<Vec<(Version, Arc<LogEntry>)>, Status> {
    if let Some(entries) = tail.read(from, limit, reached) {
        return Ok(entries);
    }
    log.read(from, limit)
        .await
        .map_err(storage)?
        .into_iter()
        .map(|(version, bytes)| Ok((version, Arc::new(decode(version, &bytes)?))))
        .collect()
}

fn decode(version: Version, bytes: &[u8]) -> Result<LogEntry, Status> {
    let entry = LogEntry::decode(bytes)
        .map_err(|_| Status::data_loss(format!("log entry {version} doesn't decode")))?;
    if entry.version != version {
        return Err(Status::data_loss(format!(
            "log entry {version} names version {}",
            entry.version
        )));
    }
    Ok(entry)
}

fn storage(error: StorageError) -> Status {
    Status::unavailable(error.to_string())
}

/// Who a stream is for.
pub struct Watcher {
    pub scope: Scope,
    /// The key's root: the stream shows only what lies under it, and ends
    /// when the watched directory leaves it.
    pub key_root: Ino,
    /// The stream ends with an error when this key is revoked, or when it
    /// expires (zero never).
    pub key: Option<KeyId>,
    pub expires: Millis,
    /// Turns true when the server shuts down.
    pub closing: tokio::sync::watch::Receiver<bool>,
    /// The version this server's reads reach: the writer's committed
    /// version, or the version a standby has applied. Storage's log can
    /// lead it briefly, and the stream announces only versions reads can
    /// reach here.
    pub committed: tokio::sync::watch::Receiver<Version>,
}

impl Watcher {
    /// Resolves with the status that ends the stream: the key expired, or
    /// the server is shutting down.
    async fn ended(&mut self) -> Status {
        let expiry = async {
            if self.expires == 0 {
                std::future::pending::<()>().await;
            }
            let left = self.expires.saturating_sub(now());
            tokio::time::sleep(Duration::from_millis(left)).await;
        };
        tokio::select! {
            () = expiry => Status::unauthenticated("the key expired"),
            _ = async { self.closing.wait_for(|closing| *closing).await.is_ok() } => {
                Status::unavailable("the server is shutting down")
            }
        }
    }
}

/// Streams the changes under `root` from version `from` on, in order. A
/// change with no effects reports that the stream has caught up to its
/// version. The stream ends after `Compacted` when the log no longer holds
/// `from`; with an error when its key goes or the server shuts down; and
/// when the client leaves. `head` is a version known committed.
pub async fn watch(
    log: Arc<dyn Log>,
    tail: Arc<Tail>,
    mut watcher: Watcher,
    root: Ino,
    mut from: Version,
    mut head: Version,
    out: mpsc::Sender<Result<pb::Change, Status>>,
) {
    let scope = watcher.scope.clone();
    let mut committed = watcher.committed.clone();
    let failed = |status| {
        let out = out.clone();
        async move {
            let _ = out.send(Err(status)).await;
        }
    };
    loop {
        let entries = tokio::select! {
            () = out.closed() => return,
            status = watcher.ended() => return failed(status).await,
            entries = entries(&*log, &tail, from, READ_BATCH, *committed.borrow()) => entries,
        };
        let entries = match entries {
            Ok(entries) => entries,
            Err(status) => return failed(status).await,
        };
        // Every committed version has an entry until truncation takes it.
        let missing = match entries.first() {
            Some((first, _)) => *first > from,
            None => from <= head,
        };
        if missing {
            let _ = out
                .send(Ok(pb::Change {
                    version: from,
                    compacted: true,
                    ..pb::Change::default()
                }))
                .await;
            return;
        }
        let Some(&(last, _)) = entries.last() else {
            // Caught up: wait for this server's reads to reach the next
            // version, whose entry storage then holds.
            let waited = tokio::select! {
                () = out.closed() => return,
                status = watcher.ended() => return failed(status).await,
                waited = async {
                    committed.wait_for(|committed| *committed >= from).await.map(|seen| *seen)
                } => waited,
            };
            match waited {
                Ok(latest) => head = head.max(latest),
                Err(_) => return failed(Status::unavailable("the writer has stopped")).await,
            }
            continue;
        };
        // Reads asking for the versions sent must find them.
        let reached = tokio::select! {
            () = out.closed() => return,
            status = watcher.ended() => return failed(status).await,
            reached = async {
                committed.wait_for(|committed| *committed >= last).await.is_ok()
            } => reached,
        };
        if !reached {
            return failed(Status::unavailable("the writer has stopped")).await;
        }
        let mut sent = 0;
        for (version, entry) in &entries {
            if watcher
                .key
                .is_some_and(|key| entry.keys.contains(&(key, None)))
            {
                return failed(Status::unauthenticated("the key was revoked")).await;
            }
            let route = route(entry, root, watcher.key_root);
            if let Some(change) = route {
                if out
                    .send(Ok(convert::change(&change, &scope)))
                    .await
                    .is_err()
                {
                    return;
                }
                sent = *version;
            }
            if entry.chains.get(&root).is_some_and(|chains| {
                chains.before.contains(&watcher.key_root)
                    && !chains.after.contains(&watcher.key_root)
            }) {
                return failed(Status::not_found(
                    "the watched directory left the key's tree",
                ))
                .await;
            }
        }
        if sent < last {
            let progress = Change {
                version: last,
                ..Change::default()
            };
            if out
                .send(Ok(convert::change(&progress, &scope)))
                .await
                .is_err()
            {
                return;
            }
        }
        head = head.max(last);
        from = last + 1;
    }
}

fn now() -> Millis {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as Millis)
        .unwrap_or(0)
}

/// Deletes log entries the retention no longer keeps, oldest first, until
/// the task is aborted. `keep` carries what the writer's leases need, and
/// None while this server isn't the writer within its lease.
pub async fn truncate(
    log: Arc<dyn Log>,
    retention: Retention,
    keep: watch::Receiver<Option<crate::engine::Keep>>,
) {
    let mut low: Version = 0;
    loop {
        tokio::time::sleep(retention.every).await;
        let Some(crate::engine::Keep { position }) = *keep.borrow() else {
            continue;
        };
        let now = now();
        let mut below = low;
        loop {
            let entries = match log.read(below, READ_BATCH).await {
                Ok(entries) => entries,
                Err(error) => {
                    eprintln!("level=warn msg=\"log truncation can't read\" error=\"{error}\"");
                    break;
                }
            };
            let full = entries.len() == READ_BATCH;
            let mut young = false;
            for (version, bytes) in &entries {
                match LogEntry::decode(bytes) {
                    Ok(entry) if !retention.keeps(now, entry.time, *version, position) => {
                        below = version + 1;
                    }
                    _ => {
                        young = true;
                        break;
                    }
                }
            }
            if young || !full {
                break;
            }
        }
        // The server may have stopped writing while the pass read.
        if below > low && keep.borrow().is_some() {
            match log.truncate(below).await {
                Ok(()) => low = below,
                Err(error) => {
                    eprintln!("level=warn msg=\"log truncation failed\" error=\"{error}\"");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(version: Version, time: Millis) -> (Version, Vec<u8>) {
        let entry = LogEntry {
            version,
            time,
            ..LogEntry::default()
        };
        (version, entry.encode())
    }

    fn versions(entries: Option<Vec<(Version, Arc<LogEntry>)>>) -> Option<Vec<Version>> {
        entries.map(|entries| entries.into_iter().map(|(version, _)| version).collect())
    }

    /// A log whose reads count, and hold nothing.
    #[derive(Default)]
    struct Counted(std::sync::atomic::AtomicUsize);

    impl Log for Counted {
        fn read(&self, _: Version, _: usize) -> Boxed<'_, Vec<(Version, Vec<u8>)>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Box::pin(async { Ok(Vec::new()) })
        }
        fn wait_past(&self, _: Version) -> Boxed<'_, Version> {
            Box::pin(std::future::pending())
        }
        fn truncate(&self, _: Version) -> Boxed<'_, ()> {
            Box::pin(async { Ok(()) })
        }
        fn dump(&self) -> Boxed<'_, inodedb_core::keys::Pairs> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[test]
    fn the_tail_holds_the_newest_young_entries_without_gaps() {
        let tail = Tail::new(&Retention::default());
        let now = now();
        tail.extend(&[entry(5, now), entry(6, now), entry(7, now)]);
        assert_eq!(versions(tail.read(6, 10, 7)), Some(vec![6, 7]));
        assert_eq!(versions(tail.read(5, 2, 7)), Some(vec![5, 6]));
        assert_eq!(versions(tail.read(4, 10, 7)), None, "before the tail");
        assert_eq!(
            versions(tail.read(8, 10, 7)),
            Some(vec![]),
            "caught up with the tail and the reads"
        );
        assert_eq!(
            versions(tail.read(8, 10, 8)),
            None,
            "reads reach past the tail"
        );
        assert_eq!(versions(tail.read(9, 10, 9)), None, "past the tail");
        // A gap starts the tail over.
        tail.extend(&[entry(9, now)]);
        assert_eq!(versions(tail.read(7, 10, 9)), None);
        assert_eq!(versions(tail.read(9, 10, 9)), Some(vec![9]));
        // An entry as old as half the minimum retention is storage's to give.
        let old = now - Retention::default().min.as_millis() as u64 / 2;
        tail.extend(&[entry(10, old)]);
        assert_eq!(versions(tail.read(10, 10, 10)), None);
        assert_eq!(versions(tail.read(9, 10, 10)), Some(vec![9, 10]));
    }

    #[test]
    fn the_tail_drops_its_oldest_entries_past_its_bound() {
        let tail = Tail::new(&Retention::default());
        let now = now();
        let entries: Vec<_> = (1..=TAIL_ENTRIES as u64 + 10)
            .map(|version| entry(version, now))
            .collect();
        tail.extend(&entries);
        assert_eq!(versions(tail.read(10, 1, 0)), None);
        assert_eq!(versions(tail.read(11, 1, 0)), Some(vec![11]));
    }

    #[tokio::test]
    async fn a_stream_that_keeps_up_reads_no_storage() {
        let tail = Tail::new(&Retention::default());
        let log = Counted::default();
        tail.extend(&[entry(1, now()), entry(2, now())]);
        let read = entries(&log, &tail, 1, READ_BATCH, 2).await.expect("reads");
        assert_eq!(read.len(), 2);
        assert!(
            entries(&log, &tail, 3, READ_BATCH, 2)
                .await
                .expect("reads")
                .is_empty()
        );
        assert_eq!(log.0.load(std::sync::atomic::Ordering::Relaxed), 0);
        entries(&log, &tail, 3, READ_BATCH, 3).await.expect("reads");
        assert_eq!(
            log.0.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "past the tail"
        );
    }

    #[test]
    fn leases_keep_what_their_positions_need_up_to_the_maximum() {
        let retention = Retention {
            min: Duration::from_millis(100),
            max: Duration::from_millis(1_000),
            every: Duration::from_millis(10),
        };
        let now = 10_000;
        assert!(retention.keeps(now, now - 99, 5, None), "younger than min");
        assert!(
            !retention.keeps(now, now - 100, 5, None),
            "no lease needs it"
        );
        assert!(
            retention.keeps(now, now - 500, 5, Some(4)),
            "a lease needs it"
        );
        assert!(
            !retention.keeps(now, now - 500, 5, Some(5)),
            "applied already"
        );
        assert!(
            !retention.keeps(now, now - 1_000, 5, Some(4)),
            "older than max"
        );
    }
}
