//! InodeDB's SQLite backend: one database file in WAL mode with
//! `synchronous = FULL`, so a commit is on disk before it returns. A lock
//! file admits one process. Commits apply one at a time under a mutex and
//! are validated optimistically; snapshot reads run in one read
//! transaction on a pooled connection. A thread of the store's own copies
//! the WAL into the database file, so commits never wait on a checkpoint.

use inodedb_core::keys::{KeyRange, KvRead, KvResult, KvWrite, Pairs};
use inodedb_core::types::Version;
use inodedb_storage::occ::{ReadSet, Validator, Written};
use inodedb_storage::{CommitError, Storage, StorageError, Txn, check_limits};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::watch;

/// How often the checkpointing thread looks for new commits.
const CHECKPOINT_EVERY: Duration = Duration::from_millis(100);

struct Shared {
    path: PathBuf,
    writer: Mutex<Connection>,
    validator: Mutex<Validator>,
    readers: Mutex<Vec<Connection>>,
    latest: watch::Sender<Version>,
    /// Commits so far, which the checkpointing thread watches.
    commits: AtomicU64,
    _lock: File,
}

#[derive(Clone)]
pub struct Sqlite {
    shared: Arc<Shared>,
}

fn unavailable(error: impl std::fmt::Display) -> StorageError {
    StorageError::Unavailable(error.to_string())
}

fn connect(path: &Path) -> Result<Connection, StorageError> {
    let conn = Connection::open(path).map_err(unavailable)?;
    conn.busy_timeout(std::time::Duration::from_secs(30))
        .map_err(unavailable)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(unavailable)?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(unavailable)?;
    Ok(conn)
}

/// SQLite reads `LIMIT -1` as no limit.
fn limit(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(-1)
}

fn get(conn: &Connection, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
    conn.prepare_cached("SELECT v FROM kv WHERE k = ?1")
        .and_then(|mut statement| {
            statement
                .query_row(params![key], |row| row.get(0))
                .optional()
        })
        .map_err(unavailable)
}

fn scan(conn: &Connection, range: &KeyRange, max: usize) -> Result<Pairs, StorageError> {
    let mut statement = conn
        .prepare_cached("SELECT k, v FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k LIMIT ?3")
        .map_err(unavailable)?;
    let rows = statement
        .query_map(params![range.start, range.end, limit(max)], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(unavailable)?;
    rows.collect::<Result<_, _>>().map_err(unavailable)
}

impl Sqlite {
    /// Opens the database at `path`, which must already hold a store, so a
    /// mistyped path creates nothing. It opens as `open` does, taking the
    /// store's lock, so it needs a writable directory.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Sqlite, StorageError> {
        let path = path.as_ref();
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(unavailable)?;
        let tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('kv', 'log')",
                [],
                |row| row.get(0),
            )
            .map_err(unavailable)?;
        if tables != 2 {
            return Err(StorageError::Unavailable(format!(
                "{} holds no InodeDB store",
                path.display()
            )));
        }
        drop(conn);
        Sqlite::open(path)
    }

    /// Opens or creates the database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Sqlite, StorageError> {
        let path = path.as_ref().to_path_buf();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(unavailable)?;
        }
        let lock = File::create(path.with_extension("lock")).map_err(unavailable)?;
        lock.try_lock().map_err(|_| {
            StorageError::Unavailable(format!("{} is open in another process", path.display()))
        })?;
        let writer = connect(&path)?;
        writer
            .pragma_update(None, "wal_autocheckpoint", 0)
            .map_err(unavailable)?;
        writer
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;
                 CREATE TABLE IF NOT EXISTS log (version INTEGER PRIMARY KEY, entry BLOB NOT NULL);",
            )
            .map_err(unavailable)?;
        let latest: Option<i64> = writer
            .query_row("SELECT max(version) FROM log", [], |row| row.get(0))
            .map_err(unavailable)?;
        let checkpoints = connect(&path)?;
        let shared = Arc::new(Shared {
            path,
            writer: Mutex::new(writer),
            validator: Mutex::new(Validator::default()),
            readers: Mutex::new(Vec::new()),
            latest: watch::Sender::new(latest.unwrap_or(0) as Version),
            commits: AtomicU64::new(0),
            _lock: lock,
        });
        let weak = Arc::downgrade(&shared);
        std::thread::Builder::new()
            .name("sqlite-checkpoint".into())
            .spawn(move || checkpoint(weak, checkpoints))
            .map_err(unavailable)?;
        Ok(Sqlite { shared })
    }

    fn validator(&self) -> MutexGuard<'_, Validator> {
        self.shared
            .validator
            .lock()
            .expect("no thread panics holding the validator")
    }

    /// Runs `work` on a pooled reader connection, on a blocking thread.
    async fn with_reader<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let pooled = shared.readers.lock().expect("pool").pop();
            let conn = match pooled {
                Some(conn) => conn,
                None => connect(&shared.path)?,
            };
            let result = work(&conn);
            shared.readers.lock().expect("pool").push(conn);
            result
        })
        .await
        .map_err(unavailable)?
    }
}

impl Storage for Sqlite {
    type Txn = SqliteTxn;

    async fn snapshot_read(&self, reads: Vec<KvRead>) -> Result<Vec<KvResult>, StorageError> {
        self.with_reader(move |conn| {
            conn.execute_batch("BEGIN").map_err(unavailable)?;
            let results: Result<Vec<KvResult>, StorageError> = reads
                .iter()
                .map(|read| match read {
                    KvRead::Get(key) => Ok(KvResult::Get(get(conn, key)?)),
                    KvRead::Scan { range, limit } => Ok(KvResult::Scan(scan(conn, range, *limit)?)),
                })
                .collect();
            conn.execute_batch("COMMIT").map_err(unavailable)?;
            results
        })
        .await
    }

    async fn begin(&self) -> Result<SqliteTxn, StorageError> {
        let (id, start) = self.validator().begin();
        Ok(SqliteTxn {
            store: self.clone(),
            id,
            start,
            reads: ReadSet::default(),
            writes: BTreeMap::new(),
            ranges: Vec::new(),
            log: Vec::new(),
        })
    }

    async fn read_log(
        &self,
        from: Version,
        max: usize,
    ) -> Result<Vec<(Version, Vec<u8>)>, StorageError> {
        self.with_reader(move |conn| {
            let mut statement = conn
                .prepare_cached(
                    "SELECT version, entry FROM log WHERE version >= ?1 ORDER BY version LIMIT ?2",
                )
                .map_err(unavailable)?;
            let rows = statement
                .query_map(params![from as i64, limit(max)], |row| {
                    Ok((row.get::<_, i64>(0)? as Version, row.get(1)?))
                })
                .map_err(unavailable)?;
            rows.collect::<Result<_, _>>().map_err(unavailable)
        })
        .await
    }

    async fn wait_past(&self, version: Version) -> Result<Version, StorageError> {
        let mut latest = self.shared.latest.subscribe();
        let seen = latest
            .wait_for(|latest| *latest > version)
            .await
            .map_err(|_| StorageError::Unavailable("closed".into()))?;
        Ok(*seen)
    }

    async fn truncate_log(&self, below: Version) -> Result<(), StorageError> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let conn = shared
                .writer
                .lock()
                .expect("no thread panics holding the writer");
            conn.execute("DELETE FROM log WHERE version < ?1", params![below as i64])
                .map(|_| ())
                .map_err(unavailable)
        })
        .await
        .map_err(unavailable)?
    }
}

pub struct SqliteTxn {
    store: Sqlite,
    id: u64,
    start: u64,
    reads: ReadSet,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ranges: Vec<KeyRange>,
    log: Vec<(Version, Vec<u8>)>,
}

impl Drop for SqliteTxn {
    fn drop(&mut self) {
        if let Ok(mut validator) = self.store.shared.validator.lock() {
            validator.end(self.id);
        }
    }
}

impl Txn for SqliteTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(write) = self.writes.get(key) {
            return Ok(write.clone());
        }
        if self.ranges.iter().any(|range| range.contains(key)) {
            return Ok(None);
        }
        self.reads.keys.insert(key.to_vec());
        let key = key.to_vec();
        self.store.with_reader(move |conn| get(conn, &key)).await
    }

    async fn scan(&mut self, range: KeyRange, max: usize) -> Result<Pairs, StorageError> {
        let scanned = range.clone();
        let stored = self
            .store
            .with_reader(move |conn| scan(conn, &scanned, usize::MAX))
            .await?;
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = stored
            .into_iter()
            .filter(|(key, _)| !self.ranges.iter().any(|cleared| cleared.contains(key)))
            .collect();
        for (key, write) in &self.writes {
            if range.contains(key) {
                match write {
                    Some(value) => merged.insert(key.clone(), value.clone()),
                    None => merged.remove(key),
                };
            }
        }
        self.reads.ranges.push(range);
        Ok(merged.into_iter().take(max).collect())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }

    fn clear(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    fn clear_range(&mut self, range: KeyRange) {
        self.writes.retain(|key, _| !range.contains(key));
        self.ranges.push(range);
    }

    fn append_log(&mut self, version: Version, entry: &[u8]) {
        self.log.push((version, entry.to_vec()));
    }

    async fn commit(mut self) -> Result<(), CommitError> {
        let mut writes: Vec<KvWrite> = self.ranges.drain(..).map(KvWrite::ClearRange).collect();
        writes.extend(std::mem::take(&mut self.writes).into_iter().map(
            |(key, value)| match value {
                Some(value) => KvWrite::Set(key, value),
                None => KvWrite::Clear(key),
            },
        ));
        let log = std::mem::take(&mut self.log);
        check_limits(&writes, log.iter().map(|(_, entry)| entry.as_slice()))?;
        let shared = self.store.shared.clone();
        let reads = std::mem::take(&mut self.reads);
        let start = self.start;
        tokio::task::spawn_blocking(move || apply(&shared, start, &reads, &writes, &log))
            .await
            .map_err(|error| CommitError::Unavailable(error.to_string()))?
    }
}

/// Copies committed pages from the WAL into the database file while the
/// store lives, without blocking commits or readers.
fn checkpoint(shared: Weak<Shared>, conn: Connection) {
    let mut seen = 0;
    loop {
        std::thread::sleep(CHECKPOINT_EVERY);
        let Some(shared) = shared.upgrade() else {
            return;
        };
        let commits = shared.commits.load(Ordering::Relaxed);
        drop(shared);
        if commits != seen {
            seen = commits;
            // A busy or failed checkpoint leaves the WAL for the next one.
            let _ = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
        }
    }
}

fn apply(
    shared: &Shared,
    start: u64,
    reads: &ReadSet,
    writes: &[KvWrite],
    log: &[(Version, Vec<u8>)],
) -> Result<(), CommitError> {
    let mut conn = shared
        .writer
        .lock()
        .expect("no thread panics holding the writer");
    if !shared
        .validator
        .lock()
        .expect("validator")
        .valid(start, reads)
    {
        return Err(CommitError::Conflict);
    }
    let failed = |error: rusqlite::Error| CommitError::Unavailable(error.to_string());
    let txn = conn.transaction().map_err(failed)?;
    let mut written = Vec::with_capacity(writes.len());
    for write in writes {
        match write {
            KvWrite::Set(key, value) => {
                txn.prepare_cached("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)")
                    .and_then(|mut statement| statement.execute(params![key, value]))
                    .map_err(failed)?;
                written.push(Written::Key(key.clone()));
            }
            KvWrite::Clear(key) => {
                txn.prepare_cached("DELETE FROM kv WHERE k = ?1")
                    .and_then(|mut statement| statement.execute(params![key]))
                    .map_err(failed)?;
                written.push(Written::Key(key.clone()));
            }
            KvWrite::ClearRange(range) => {
                txn.prepare_cached("DELETE FROM kv WHERE k >= ?1 AND k < ?2")
                    .and_then(|mut statement| statement.execute(params![range.start, range.end]))
                    .map_err(failed)?;
                written.push(Written::Range(range.clone()));
            }
        }
    }
    let mut latest = None;
    for (version, entry) in log {
        txn.prepare_cached("INSERT OR REPLACE INTO log (version, entry) VALUES (?1, ?2)")
            .and_then(|mut statement| statement.execute(params![*version as i64, entry]))
            .map_err(failed)?;
        latest = latest.max(Some(*version));
    }
    // A commit that fails may still have reached the disk. Later
    // transactions treat its writes as made, so they conflict with it.
    let committed = txn.commit();
    shared.validator.lock().expect("validator").record(written);
    committed.map_err(|_| CommitError::Unknown)?;
    shared.commits.fetch_add(1, Ordering::Relaxed);
    if let Some(latest) = latest {
        shared.latest.send_if_modified(|current| {
            let advanced = latest > *current;
            *current = (*current).max(latest);
            advanced
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Commits leave their pages in the WAL, and the store's own thread
    /// copies them into the database file.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_thread_checkpoints_the_wal() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("store.db");
        let store = Sqlite::open(&path).expect("opens");
        let inline: i64 = store
            .shared
            .writer
            .lock()
            .expect("the writer")
            .pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
            .expect("reads the pragma");
        assert_eq!(inline, 0, "commits never checkpoint");
        for index in 0..100u32 {
            let mut txn = store.begin().await.expect("begins");
            txn.set(&index.to_be_bytes(), &[7; 4_096]);
            txn.commit().await.expect("commits");
        }
        let checkpointed = async {
            while std::fs::metadata(&path).expect("the database file").len() < 400_000 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), checkpointed)
            .await
            .expect("the database file takes the WAL's pages");
    }
}
