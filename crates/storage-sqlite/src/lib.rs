//! InodeDB's SQLite backend: one database file in WAL mode with
//! `synchronous = FULL`, so a commit is on disk before it returns. A lock
//! file admits one process. Commits apply one at a time under a mutex and
//! are validated optimistically; snapshot reads run in one read
//! transaction on a pooled connection. A thread of the store's own copies
//! the WAL into the database file, so commits rarely wait on a checkpoint.
//!
//! A sync that must also grow a file commits the file system's journal, which
//! costs up to three times the write itself on local NVMe and twice on gp3
//! (`BENCHMARKS.md`, "Devices"). So the WAL file is written to its full size
//! when the store opens, and the thread sends commits back to its start before
//! they pass its end, so they overwrite written blocks. `.cargo/config.toml`
//! builds SQLite to sync its WAL with `fdatasync`, which leaves timestamps to
//! the journal's own time.

use inodedb_core::keys::{self, KeyRange, KvRead, KvResult, KvWrite, Pairs, WriteSet};
use inodedb_core::types::Version;
use inodedb_core::writer::CommitOutcome;
use inodedb_storage::occ::{ReadSet, Validator, Written};
use inodedb_storage::{CommitError, Storage, StorageError, Txn, check_limits};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::watch;

/// How often the checkpointing thread looks for new commits. A test's WAL is
/// small, and fills between looks at the usual pace.
const CHECKPOINT_EVERY: Duration = Duration::from_millis(if cfg!(test) { 5 } else { 100 });
/// The size the WAL file is written to.
const WAL_BYTES: u64 = if cfg!(test) { 8 << 20 } else { 128 << 20 };
/// The WAL's frames past which the checkpointing thread sends commits back to
/// its start, leaving room for what commits write until it next looks. Frames
/// are SQLite's default 4 KiB page and a small header.
const WAL_WRAP: i64 = (WAL_BYTES / 2 / 4120) as i64;

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
        // SQLite deletes a WAL it finds beside an empty database, so this comes
        // after the tables.
        write_wal(&path).map_err(unavailable)?;
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

    async fn commit_group(&self, expect: Option<Vec<u8>>, writes: Arc<WriteSet>) -> CommitOutcome {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || commit_group(&shared, expect.as_deref(), &writes))
            .await
            .unwrap_or(CommitOutcome::Failed)
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

/// Writes the WAL file to its full size, so commits overwrite written blocks.
/// It only adds zeros past the file's end, where SQLite reads no frames, so
/// what a crash left in the file stays, and commits go on from where the log
/// ends, back to the start once the checkpointing thread sends them there.
fn write_wal(database: &Path) -> std::io::Result<()> {
    let mut path = database.as_os_str().to_owned();
    path.push("-wal");
    let mut wal = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let mut length = wal.metadata()?.len();
    let zeros = vec![0u8; 1 << 20];
    while length < WAL_BYTES {
        wal.write_all(&zeros)?;
        length += zeros.len() as u64;
    }
    wal.sync_all()
}

/// Copies committed pages from the WAL into the database file while the
/// store lives, without blocking commits or readers. Under steady commits a
/// passive checkpoint never catches up, and the WAL would grow without end, so
/// once it reaches `WAL_WRAP` frames the thread takes the writer's lock for
/// long enough to finish the copy, and commits go back to the WAL's start.
fn checkpoint(shared: Weak<Shared>, conn: Connection) {
    let mut seen = 0;
    loop {
        std::thread::sleep(CHECKPOINT_EVERY);
        let Some(shared) = shared.upgrade() else {
            return;
        };
        let commits = shared.commits.load(Ordering::Relaxed);
        if commits != seen {
            seen = commits;
            // A busy or failed checkpoint leaves the WAL for the next one.
            let frames = conn
                .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    row.get::<_, i64>(1)
                })
                .unwrap_or(0);
            if frames >= WAL_WRAP {
                // A second pass leaves the writer's wait only what commits
                // wrote during the first.
                let _ = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
                let _writer = shared.writer.lock().expect("no panics hold the writer");
                let _ = conn.query_row("PRAGMA wal_checkpoint(RESTART)", [], |_| Ok(()));
            }
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
    write(shared, &mut conn, writes, log)
}

/// Commits a group if the head record holds `expect`, reading and writing
/// under the writer's lock, so nothing commits between them.
fn commit_group(shared: &Shared, expect: Option<&[u8]>, writes: &WriteSet) -> CommitOutcome {
    let mut conn = shared
        .writer
        .lock()
        .expect("no thread panics holding the writer");
    match get(&conn, &keys::head()) {
        Ok(head) if head.as_deref() == expect => {}
        Ok(_) => return CommitOutcome::Fenced,
        Err(_) => return CommitOutcome::Failed,
    }
    let log = writes.log.iter().map(|(_, entry)| entry.as_slice());
    if check_limits(&writes.writes, log).is_err() {
        return CommitOutcome::Failed;
    }
    match write(shared, &mut conn, &writes.writes, &writes.log) {
        Ok(()) => CommitOutcome::Committed,
        Err(CommitError::Unknown) => CommitOutcome::Unknown,
        Err(_) => CommitOutcome::Failed,
    }
}

/// Applies writes in one synced transaction on the writer's connection.
fn write(
    shared: &Shared,
    conn: &mut Connection,
    writes: &[KvWrite],
    log: &[(Version, Vec<u8>)],
) -> Result<(), CommitError> {
    let failed = |error: rusqlite::Error| CommitError::Unavailable(error.to_string());
    let txn = conn.transaction().map_err(failed)?;
    for write in writes {
        match write {
            KvWrite::Set(key, value) => {
                txn.prepare_cached("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)")
                    .and_then(|mut statement| statement.execute(params![key, value]))
                    .map_err(failed)?;
            }
            KvWrite::Clear(key) => {
                txn.prepare_cached("DELETE FROM kv WHERE k = ?1")
                    .and_then(|mut statement| statement.execute(params![key]))
                    .map_err(failed)?;
            }
            KvWrite::ClearRange(range) => {
                txn.prepare_cached("DELETE FROM kv WHERE k >= ?1 AND k < ?2")
                    .and_then(|mut statement| statement.execute(params![range.start, range.end]))
                    .map_err(failed)?;
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
    shared
        .validator
        .lock()
        .expect("validator")
        .record(|| writes.iter().map(Written::of).collect());
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

    fn wal_bytes(path: &Path) -> u64 {
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        std::fs::metadata(wal).expect("the WAL").len()
    }

    /// Commits find the WAL written, and under steady commits the thread sends
    /// them back to its start, so they keep overwriting it.
    #[tokio::test(flavor = "multi_thread")]
    async fn commits_keep_overwriting_a_written_wal() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("store.db");
        let store = Sqlite::open(&path).expect("opens");
        assert!(wal_bytes(&path) >= WAL_BYTES, "the WAL starts written");
        // Small commits without a pause, so a passive checkpoint never finds
        // the WAL all copied, and the thread gets several turns.
        let value = vec![7; 2_000];
        let started = std::time::Instant::now();
        let mut written = 0;
        let mut round = 0u32;
        while written < 3 * WAL_BYTES && started.elapsed() < Duration::from_secs(5) {
            let mut txn = store.begin().await.expect("begins");
            txn.set(&round.to_be_bytes(), &value);
            txn.commit().await.expect("commits");
            written += 4096;
            round += 1;
        }
        assert!(wal_bytes(&path) < 2 * WAL_BYTES, "{}", wal_bytes(&path));
    }

    /// What a crash leaves in the WAL survives the next open, which only adds
    /// to a file that came up short.
    #[tokio::test(flavor = "multi_thread")]
    async fn commits_in_the_wal_survive_a_crash() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("store.db");
        let store = Sqlite::open(&path).expect("opens");
        let mut txn = store.begin().await.expect("begins");
        txn.set(b"k", b"v");
        txn.commit().await.expect("commits");
        // Copies of the files as a crash would leave them, with the WAL cut
        // to what it holds.
        let copy = dir.path().join("copy.db");
        std::fs::copy(&path, &copy).expect("copies the database");
        let mut from = path.as_os_str().to_owned();
        from.push("-wal");
        let mut to = copy.as_os_str().to_owned();
        to.push("-wal");
        let mut wal = std::fs::read(from).expect("reads the WAL");
        wal.truncate(1 << 20);
        std::fs::write(to, wal).expect("writes the copy");
        let reopened = Sqlite::open(&copy).expect("opens the copy");
        assert_eq!(
            reopened
                .snapshot_read(vec![KvRead::Get(b"k".to_vec())])
                .await
                .expect("reads"),
            vec![KvResult::Get(Some(b"v".to_vec()))]
        );
        drop(store);
    }
}
