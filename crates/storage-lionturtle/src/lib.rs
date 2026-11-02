//! InodeDB's lionturtle backend. lionturtle has one keyspace, so the tree lives
//! beneath the byte `00` and the log beneath `01`, keyed by big-endian version.
//! Every commit is durable before it returns. Commits apply one at a time under
//! a mutex and are validated optimistically; snapshot reads use a lionturtle
//! snapshot.
//!
//! A sync that must also grow a file commits the file system's journal, which
//! costs up to four times the write itself on local NVMe (`BENCHMARKS.md`,
//! "Devices"). So the store zero-fills each log segment ahead of its writes,
//! and commits overwrite written blocks.
//!
//! lionturtle's file storage takes no lock, so the backend holds one beside the
//! store's own directory, which lionturtle takes to hold only its files.

use bytes::Bytes;
use inodedb_core::keys::{self, KeyRange, KvRead, KvResult, KvWrite, Pairs, WriteSet};
use inodedb_core::types::Version;
use inodedb_core::writer::CommitOutcome;
use inodedb_storage::occ::{ReadSet, Validator, Written};
use inodedb_storage::{CommitError, Storage, StorageError, Txn, check_limits};
use lionturtle::storage::FsStorage;
use lionturtle::{Db, Options, Snapshot, WriteOp};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::watch;

/// The first byte of every key in the tree.
const TREE: u8 = 0;
/// The first byte of every key in the log.
const LOG: u8 = 1;
/// What the block cache holds; the writer's own cache answers most reads, so
/// storage sees the rest.
const CACHE: u64 = 256 << 20;
/// Log entries one write of a truncation deletes.
const TRUNCATE_CHUNK: usize = 4096;

struct Shared {
    db: Db,
    commit: Mutex<()>,
    validator: Mutex<Validator>,
    latest: watch::Sender<Version>,
    /// Held until the store closes, which fields declared above it do first.
    _lock: Option<File>,
}

#[derive(Clone)]
pub struct Lionturtle {
    shared: Arc<Shared>,
}

fn unavailable(error: impl std::fmt::Display) -> StorageError {
    StorageError::Unavailable(error.to_string())
}

fn failed(error: lionturtle::Error) -> StorageError {
    match error {
        lionturtle::Error::Corruption(why) => StorageError::Corrupt(why),
        other => unavailable(other),
    }
}

fn tree_key(key: &[u8]) -> Vec<u8> {
    let mut prefixed = Vec::with_capacity(key.len() + 1);
    prefixed.push(TREE);
    prefixed.extend_from_slice(key);
    prefixed
}

fn log_key(version: Version) -> [u8; 9] {
    let mut key = [LOG; 9];
    key[1..].copy_from_slice(&version.to_be_bytes());
    key
}

fn log_version(key: &[u8]) -> Result<Version, StorageError> {
    key.strip_prefix(&[LOG])
        .and_then(|version| version.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| StorageError::Corrupt("log key".into()))
}

/// `result`, once the store is known to be running. A durable write that fails
/// leaves its batch in the memtable, readable though it may never reach the
/// disk, and stops the store. Reads must then fail, or a writer resolving that batch's
/// outcome would find its head and take it as committed.
fn running<T>(db: &Db, result: Result<T, StorageError>) -> Result<T, StorageError> {
    if db.is_poisoned() {
        return Err(unavailable(lionturtle::Error::Poisoned));
    }
    result
}

fn scan_tree(snapshot: &Snapshot, range: &KeyRange, limit: usize) -> Result<Pairs, StorageError> {
    let pairs = snapshot
        .scan(&tree_key(&range.start), Some(&tree_key(&range.end)), limit)
        .map_err(failed)?;
    Ok(pairs
        .into_iter()
        .map(|(key, value)| (key[1..].to_vec(), value.to_vec()))
        .collect())
}

impl Lionturtle {
    /// Opens or creates the store at `path`, a directory holding the lock file
    /// and the store's own directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Lionturtle, StorageError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(unavailable)?;
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join("lock"))
            .map_err(unavailable)?;
        lock.try_lock().map_err(unavailable)?;
        let storage = FsStorage::new(path.join("store")).map_err(unavailable)?;
        Self::with(Arc::new(storage), Some(lock))
    }

    /// Opens or creates the store on `storage`, with no lock.
    pub fn on(storage: Arc<dyn lionturtle::storage::Storage>) -> Result<Lionturtle, StorageError> {
        Self::with(storage, None)
    }

    fn with(
        storage: Arc<dyn lionturtle::storage::Storage>,
        lock: Option<File>,
    ) -> Result<Lionturtle, StorageError> {
        let options = Options {
            cache_bytes: CACHE,
            preallocate_log: true,
            ..Options::default()
        };
        let db = Db::open(storage, options).map_err(failed)?;
        let latest = match db.prefix_rev(&[LOG]).next().map_err(failed)? {
            Some((key, _)) => log_version(&key)?,
            None => 0,
        };
        Ok(Lionturtle {
            shared: Arc::new(Shared {
                db,
                commit: Mutex::new(()),
                validator: Mutex::new(Validator::default()),
                latest: watch::Sender::new(latest),
                _lock: lock,
            }),
        })
    }

    fn validator(&self) -> MutexGuard<'_, Validator> {
        self.shared
            .validator
            .lock()
            .expect("no thread panics holding the validator")
    }
}

impl Storage for Lionturtle {
    type Txn = LionturtleTxn;

    async fn snapshot_read(&self, reads: Vec<KvRead>) -> Result<Vec<KvResult>, StorageError> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let snapshot = shared.db.snapshot();
            let results = reads
                .iter()
                .map(|read| match read {
                    KvRead::Get(key) => {
                        let value = snapshot.get(&tree_key(key)).map_err(failed)?;
                        Ok(KvResult::Get(value.map(|value| value.to_vec())))
                    }
                    KvRead::Scan { range, limit } => {
                        Ok(KvResult::Scan(scan_tree(&snapshot, range, *limit)?))
                    }
                })
                .collect();
            running(&shared.db, results)
        })
        .await
        .map_err(unavailable)?
    }

    async fn begin(&self) -> Result<LionturtleTxn, StorageError> {
        let (id, start) = self.validator().begin();
        Ok(LionturtleTxn {
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
        limit: usize,
    ) -> Result<Vec<(Version, Vec<u8>)>, StorageError> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let pairs = shared
                .db
                .scan(&log_key(from), Some(&[LOG + 1]), limit)
                .map_err(failed)?;
            let entries = pairs
                .into_iter()
                .map(|(key, entry)| Ok((log_version(&key)?, entry.to_vec())))
                .collect();
            running(&shared.db, entries)
        })
        .await
        .map_err(unavailable)?
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
        tokio::task::spawn_blocking(move || truncate_log(&shared.db, below))
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

/// Deletes what the log holds below `below`, oldest first, a chunk at a time:
/// lionturtle deletes key by key, and a crash partway leaves the log's newest
/// entries whole. Entries below the newest never change, so commits go on
/// meanwhile.
fn truncate_log(db: &Db, below: Version) -> Result<(), StorageError> {
    let mut entries = db.iter(&[LOG], Some(&log_key(below)));
    let mut deletes = Vec::with_capacity(TRUNCATE_CHUNK);
    let mut deleted = false;
    while let Some((key, _)) = entries.next().map_err(failed)? {
        deletes.push(WriteOp::delete(key));
        if deletes.len() == TRUNCATE_CHUNK {
            db.write(&deletes, false).map_err(failed)?;
            deletes.clear();
            deleted = true;
        }
    }
    deleted |= !deletes.is_empty();
    db.write(&deletes, false).map_err(failed)?;
    if deleted {
        db.sync().map_err(failed)?;
    }
    Ok(())
}

pub struct LionturtleTxn {
    store: Lionturtle,
    id: u64,
    start: u64,
    reads: ReadSet,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ranges: Vec<KeyRange>,
    log: Vec<(Version, Vec<u8>)>,
}

impl Drop for LionturtleTxn {
    fn drop(&mut self) {
        if let Ok(mut validator) = self.store.shared.validator.lock() {
            validator.end(self.id);
        }
    }
}

impl Txn for LionturtleTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(write) = self.writes.get(key) {
            return Ok(write.clone());
        }
        if self.ranges.iter().any(|range| range.contains(key)) {
            return Ok(None);
        }
        self.reads.keys.insert(key.to_vec());
        let (shared, key) = (self.store.shared.clone(), tree_key(key));
        tokio::task::spawn_blocking(move || {
            let value = shared.db.get(&key).map_err(failed);
            running(&shared.db, value).map(|value| value.map(|value| value.to_vec()))
        })
        .await
        .map_err(unavailable)?
    }

    async fn scan(&mut self, range: KeyRange, limit: usize) -> Result<Pairs, StorageError> {
        // Each delete this transaction holds hides at most one stored pair, so
        // this many hold the answer however they merge with its writes.
        let hidden = self
            .writes
            .iter()
            .filter(|(key, write)| write.is_none() && range.contains(key))
            .count();
        let wanted = limit.saturating_add(hidden);
        let (shared, scanned, cleared) = (
            self.store.shared.clone(),
            range.clone(),
            self.ranges.clone(),
        );
        let stored = tokio::task::spawn_blocking(move || {
            let (start, end) = (tree_key(&scanned.start), tree_key(&scanned.end));
            let mut entries = shared.db.iter(&start, Some(&end));
            let mut pairs = Vec::new();
            while pairs.len() < wanted {
                let Some((key, value)) = entries.next().map_err(failed)? else {
                    break;
                };
                if !cleared.iter().any(|range| range.contains(&key[1..])) {
                    pairs.push((key[1..].to_vec(), value.to_vec()));
                }
            }
            running(&shared.db, Ok(pairs))
        })
        .await
        .map_err(unavailable)??;
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = stored.into_iter().collect();
        for (key, write) in &self.writes {
            if range.contains(key) {
                match write {
                    Some(value) => merged.insert(key.clone(), value.clone()),
                    None => merged.remove(key),
                };
            }
        }
        self.reads.ranges.push(range);
        Ok(merged.into_iter().take(limit).collect())
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

fn apply(
    shared: &Shared,
    start: u64,
    reads: &ReadSet,
    writes: &[KvWrite],
    log: &[(Version, Vec<u8>)],
) -> Result<(), CommitError> {
    let _commit = shared
        .commit
        .lock()
        .expect("no thread panics holding the commit lock");
    if !shared
        .validator
        .lock()
        .expect("validator")
        .valid(start, reads)
    {
        return Err(CommitError::Conflict);
    }
    write(shared, writes, log)
}

/// Commits a group if the head record holds `expect`, reading and writing
/// under the commit lock, so nothing commits between them.
fn commit_group(shared: &Shared, expect: Option<&[u8]>, writes: &WriteSet) -> CommitOutcome {
    let _commit = shared
        .commit
        .lock()
        .expect("no thread panics holding the commit lock");
    let head = shared.db.get(&tree_key(&keys::head())).map_err(failed);
    match running(&shared.db, head) {
        Ok(head) if head.as_deref() == expect => {}
        Ok(_) => return CommitOutcome::Fenced,
        Err(_) => return CommitOutcome::Failed,
    }
    let log = writes.log.iter().map(|(_, entry)| entry.as_slice());
    if check_limits(&writes.writes, log).is_err() {
        return CommitOutcome::Failed;
    }
    match write(shared, &writes.writes, &writes.log) {
        Ok(()) => CommitOutcome::Committed,
        Err(CommitError::Unknown) => CommitOutcome::Unknown,
        Err(_) => CommitOutcome::Failed,
    }
}

/// What a batch does, in the order its writes give. The caller holds the commit
/// lock, so what a range clears stays as it reads here.
fn batch(
    db: &Db,
    writes: &[KvWrite],
    log: &[(Version, Vec<u8>)],
) -> Result<Vec<WriteOp>, StorageError> {
    let mut ops = Vec::with_capacity(writes.len() + log.len());
    // Groups put their range clears first, so most clear nothing of their own.
    let mut any_set = false;
    for write in writes {
        match write {
            KvWrite::Set(key, value) => {
                any_set = true;
                ops.push(WriteOp::put(tree_key(key), Bytes::copy_from_slice(value)));
            }
            KvWrite::Clear(key) => ops.push(WriteOp::delete(tree_key(key))),
            KvWrite::ClearRange(range) => {
                if any_set {
                    ops.retain(|op| !range.contains(&op.key[1..]));
                }
                let (start, end) = (tree_key(&range.start), tree_key(&range.end));
                let mut stored = db.iter(&start, Some(&end));
                while let Some((key, _)) = stored.next().map_err(failed)? {
                    ops.push(WriteOp::delete(key));
                }
            }
        }
    }
    for (version, entry) in log {
        ops.push(WriteOp::put(
            Bytes::copy_from_slice(&log_key(*version)),
            Bytes::copy_from_slice(entry),
        ));
    }
    Ok(ops)
}

/// Applies writes in one durable write. The caller holds the commit lock.
fn write(
    shared: &Shared,
    writes: &[KvWrite],
    log: &[(Version, Vec<u8>)],
) -> Result<(), CommitError> {
    let db = &shared.db;
    let ops = running(db, batch(db, writes, log))
        .map_err(|error| CommitError::Unavailable(error.to_string()))?;
    let committed = db.write(&ops, true);
    // A write that fails may still have reached the log. Later transactions
    // treat its writes as made, so they conflict with it.
    shared
        .validator
        .lock()
        .expect("validator")
        .record(|| writes.iter().map(Written::of).collect());
    committed.map_err(|error| match error {
        lionturtle::Error::Poisoned => CommitError::Unavailable(error.to_string()),
        _ => CommitError::Unknown,
    })?;
    if let Some(latest) = log.iter().map(|(version, _)| *version).max() {
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
    use lionturtle::sim::{FaultConfig, SimStorage};

    /// A disk whose crash loses every write its owner left unsynced.
    fn disk(seed: u64) -> SimStorage {
        SimStorage::new(
            seed,
            FaultConfig {
                crash_keep_sector: 0.0,
                ..FaultConfig::default()
            },
        )
    }

    fn on(disk: &SimStorage) -> Lionturtle {
        Lionturtle::on(Arc::new(disk.clone())).expect("opens")
    }

    /// A group that moves the head from `version - 1` to `version`, sets a key
    /// of its own and logs an entry.
    fn group(version: Version) -> WriteSet {
        let mut writes = WriteSet::default();
        writes.set(keys::head(), version.to_be_bytes().to_vec());
        writes.set(format!("key-{version}").into_bytes(), b"value".to_vec());
        writes
            .log
            .push((version, format!("entry-{version}").into_bytes()));
        writes
    }

    fn head_at(version: Version) -> Option<Vec<u8>> {
        (version > 0).then(|| version.to_be_bytes().to_vec())
    }

    async fn commit(store: &Lionturtle, version: Version) -> CommitOutcome {
        store
            .commit_group(head_at(version - 1), Arc::new(group(version)))
            .await
    }

    async fn read(store: &Lionturtle, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let results = store.snapshot_read(vec![KvRead::Get(key.to_vec())]).await?;
        match results.into_iter().next() {
            Some(KvResult::Get(value)) => Ok(value),
            other => panic!("asked for a get, and got {other:?}"),
        }
    }

    /// Every acknowledged commit survives the power going out, with its log
    /// entry, and the store knows its newest version again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_commit_outlives_a_crash() {
        for seed in 0..4 {
            let disk = disk(seed);
            let store = on(&disk);
            for version in 1..=50 {
                assert_eq!(commit(&store, version).await, CommitOutcome::Committed);
            }
            disk.crash();
            drop(store);
            let store = on(&disk);
            assert_eq!(read(&store, &keys::head()).await.unwrap(), head_at(50));
            assert!(read(&store, b"key-50").await.unwrap().is_some());
            let log = store.read_log(1, usize::MAX).await.unwrap();
            assert_eq!(log.len(), 50);
            assert_eq!(store.wait_past(49).await.unwrap(), 50);
        }
    }

    /// A durable write that fails leaves its batch readable in the memtable. The
    /// store answers nothing more, so a writer resolving the outcome waits for a
    /// restart rather than find its own head and take it as committed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_write_stops_the_store_until_it_reopens() {
        let disk = disk(1);
        let store = on(&disk);
        assert_eq!(commit(&store, 1).await, CommitOutcome::Committed);
        disk.set_faults(FaultConfig {
            write_error: 1.0,
            crash_keep_sector: 0.0,
            ..FaultConfig::default()
        });
        let outcome = commit(&store, 2).await;
        assert_eq!(outcome, CommitOutcome::Unknown);
        assert!(read(&store, &keys::head()).await.is_err());
        assert!(store.read_log(1, 10).await.is_err());
        let mut txn = store.begin().await.unwrap();
        assert!(txn.get(&keys::head()).await.is_err());
        assert!(txn.scan(KeyRange::prefix(vec![1]), 10).await.is_err());
        drop(txn);
        assert_eq!(commit(&store, 2).await, CommitOutcome::Failed);
        disk.set_faults(FaultConfig {
            crash_keep_sector: 0.0,
            ..FaultConfig::default()
        });
        disk.crash();
        drop(store);
        let store = on(&disk);
        assert_eq!(read(&store, &keys::head()).await.unwrap(), head_at(1));
        assert_eq!(commit(&store, 2).await, CommitOutcome::Committed);
    }

    /// Truncation deletes in chunks; the store ends with exactly the entries
    /// from `below` on, through a crash too.
    #[tokio::test(flavor = "multi_thread")]
    async fn truncation_covers_every_chunk() {
        let disk = disk(3);
        let store = on(&disk);
        let entries = (TRUNCATE_CHUNK * 2 + 10) as Version;
        let mut writes = WriteSet::default();
        for version in 1..=entries {
            writes.log.push((version, b"entry".to_vec()));
        }
        assert_eq!(
            store.commit_group(None, Arc::new(writes)).await,
            CommitOutcome::Committed
        );
        let below = entries - 5;
        store.truncate_log(below).await.unwrap();
        let versions = |log: Vec<(Version, Vec<u8>)>| -> Vec<Version> {
            log.into_iter().map(|(version, _)| version).collect()
        };
        let kept: Vec<Version> = (below..=entries).collect();
        assert_eq!(versions(store.read_log(0, usize::MAX).await.unwrap()), kept);
        disk.crash();
        drop(store);
        let store = on(&disk);
        assert_eq!(versions(store.read_log(0, usize::MAX).await.unwrap()), kept);
    }

    /// Each log segment runs ahead of its writes, so a commit's sync overwrites
    /// written blocks.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_log_runs_ahead_of_its_writes() {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let store = Lionturtle::open(dir.path()).expect("opens");
        assert_eq!(commit(&store, 1).await, CommitOutcome::Committed);
        let longest = || -> u64 {
            std::fs::read_dir(dir.path().join("store"))
                .expect("lists the store")
                .map(|file| file.expect("an entry"))
                .filter(|file| file.file_name().to_string_lossy().starts_with("wal-"))
                .map(|file| file.metadata().expect("its size").len())
                .max()
                .unwrap_or(0)
        };
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while longest() < 1 << 20 && std::time::Instant::now() < until {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(longest() >= 1 << 20, "the log holds {} bytes", longest());
    }
}
