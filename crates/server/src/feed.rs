//! The changefeed's server side: the log as an object the service holds,
//! `Watch` streams, and truncation by age and lease positions.

use crate::auth::Scope;
use crate::convert;
use inodedb_core::auth::KeyId;
use inodedb_core::feed::{Change, route};
use inodedb_core::log::LogEntry;
use inodedb_core::types::{Ino, Millis, Version};
use inodedb_proto as pb;
use inodedb_storage::{Storage, StorageError};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
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
            entries = log.read(from, READ_BATCH) => entries,
        };
        let entries = match entries {
            Ok(entries) => entries,
            Err(error) => return failed(storage(error)).await,
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
        let Some((last, _)) = entries.last().map(|(version, bytes)| (*version, bytes)) else {
            // Caught up: wait for the next commit.
            let waited = tokio::select! {
                () = out.closed() => return,
                status = watcher.ended() => return failed(status).await,
                waited = log.wait_past(from.saturating_sub(1)) => waited,
            };
            match waited {
                Ok(latest) => head = head.max(latest),
                Err(error) => return failed(storage(error)).await,
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
        for (version, bytes) in &entries {
            let entry = match decode(*version, bytes) {
                Ok(entry) => entry,
                Err(status) => return failed(status).await,
            };
            if watcher
                .key
                .is_some_and(|key| entry.keys.contains(&(key, None)))
            {
                return failed(Status::unauthenticated("the key was revoked")).await;
            }
            let route = route(&entry, root, watcher.key_root);
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
    use super::Retention;
    use std::time::Duration;

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
