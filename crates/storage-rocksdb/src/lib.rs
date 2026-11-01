//! InodeDB's RocksDB backend: the tree in the default column family and
//! the log in its own, keyed by big-endian version. Every commit syncs the
//! WAL before it returns, and RocksDB's lock file admits one process.
//! Commits apply one at a time under a mutex and are validated
//! optimistically; snapshot reads use a RocksDB snapshot.

use inodedb_core::keys::{KeyRange, KvRead, KvResult, KvWrite, Pairs};
use inodedb_core::types::Version;
use inodedb_storage::occ::{ReadSet, Validator, Written};
use inodedb_storage::{CommitError, Storage, StorageError, Txn, check_limits};
use rocksdb::{
    ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, ReadOptions, WriteBatch,
    WriteOptions,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::watch;

const LOG: &str = "log";

struct Shared {
    db: DB,
    commit: Mutex<()>,
    validator: Mutex<Validator>,
    latest: watch::Sender<Version>,
}

#[derive(Clone)]
pub struct Rocks {
    shared: Arc<Shared>,
}

fn unavailable(error: impl std::fmt::Display) -> StorageError {
    StorageError::Unavailable(error.to_string())
}

fn scan_db(
    db: &DB,
    options: ReadOptions,
    range: &KeyRange,
    limit: usize,
) -> Result<Pairs, StorageError> {
    let mut pairs = Vec::new();
    let iter = db.iterator_opt(
        IteratorMode::From(&range.start, Direction::Forward),
        options,
    );
    for item in iter {
        if pairs.len() >= limit {
            break;
        }
        let (key, value) = item.map_err(unavailable)?;
        if key.as_ref() >= range.end.as_slice() {
            break;
        }
        pairs.push((key.to_vec(), value.to_vec()));
    }
    Ok(pairs)
}

impl Rocks {
    /// Opens or creates the database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Rocks, StorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let families = vec![
            ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, Options::default()),
            ColumnFamilyDescriptor::new(LOG, Options::default()),
        ];
        let db = DB::open_cf_descriptors(&options, path, families).map_err(unavailable)?;
        let latest = {
            let log = db.cf_handle(LOG).expect("the log family exists");
            match db.iterator_cf(log, IteratorMode::End).next() {
                Some(item) => {
                    let (key, _) = item.map_err(unavailable)?;
                    u64::from_be_bytes(
                        key.as_ref()
                            .try_into()
                            .map_err(|_| StorageError::Corrupt("log key".into()))?,
                    )
                }
                None => 0,
            }
        };
        Ok(Rocks {
            shared: Arc::new(Shared {
                db,
                commit: Mutex::new(()),
                validator: Mutex::new(Validator::default()),
                latest: watch::Sender::new(latest),
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

fn synced() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

impl Storage for Rocks {
    type Txn = RocksTxn;

    async fn snapshot_read(&self, reads: Vec<KvRead>) -> Result<Vec<KvResult>, StorageError> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let db = &shared.db;
            let snapshot = db.snapshot();
            reads
                .iter()
                .map(|read| match read {
                    KvRead::Get(key) => Ok(KvResult::Get(snapshot.get(key).map_err(unavailable)?)),
                    KvRead::Scan { range, limit } => {
                        let mut options = ReadOptions::default();
                        options.set_snapshot(&snapshot);
                        Ok(KvResult::Scan(scan_db(db, options, range, *limit)?))
                    }
                })
                .collect()
        })
        .await
        .map_err(unavailable)?
    }

    async fn begin(&self) -> Result<RocksTxn, StorageError> {
        let (id, start) = self.validator().begin();
        Ok(RocksTxn {
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
        let db = &self.shared.db;
        let log = db.cf_handle(LOG).expect("the log family exists");
        let start = from.to_be_bytes();
        let mut entries = Vec::new();
        for item in db.iterator_cf(log, IteratorMode::From(&start, Direction::Forward)) {
            if entries.len() >= limit {
                break;
            }
            let (key, value) = item.map_err(unavailable)?;
            let version = u64::from_be_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| StorageError::Corrupt("log key".into()))?,
            );
            entries.push((version, value.to_vec()));
        }
        Ok(entries)
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
            let log = shared.db.cf_handle(LOG).expect("the log family exists");
            let mut batch = WriteBatch::default();
            batch.delete_range_cf(log, 0u64.to_be_bytes(), below.to_be_bytes());
            shared.db.write_opt(batch, &synced()).map_err(unavailable)
        })
        .await
        .map_err(unavailable)?
    }
}

pub struct RocksTxn {
    store: Rocks,
    id: u64,
    start: u64,
    reads: ReadSet,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ranges: Vec<KeyRange>,
    log: Vec<(Version, Vec<u8>)>,
}

impl Drop for RocksTxn {
    fn drop(&mut self) {
        if let Ok(mut validator) = self.store.shared.validator.lock() {
            validator.end(self.id);
        }
    }
}

impl Txn for RocksTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(write) = self.writes.get(key) {
            return Ok(write.clone());
        }
        if self.ranges.iter().any(|range| range.contains(key)) {
            return Ok(None);
        }
        self.reads.keys.insert(key.to_vec());
        let (shared, key) = (self.store.shared.clone(), key.to_vec());
        tokio::task::spawn_blocking(move || shared.db.get(key).map_err(unavailable))
            .await
            .map_err(unavailable)?
    }

    async fn scan(&mut self, range: KeyRange, limit: usize) -> Result<Pairs, StorageError> {
        let (shared, scanned) = (self.store.shared.clone(), range.clone());
        let stored = tokio::task::spawn_blocking(move || {
            scan_db(&shared.db, ReadOptions::default(), &scanned, usize::MAX)
        })
        .await
        .map_err(unavailable)??;
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
    let db = &shared.db;
    let tree = db
        .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
        .expect("the default family exists");
    let log_family = db.cf_handle(LOG).expect("the log family exists");
    let mut batch = WriteBatch::default();
    let mut written = Vec::with_capacity(writes.len());
    for write in writes {
        match write {
            KvWrite::Set(key, value) => {
                batch.put_cf(tree, key, value);
                written.push(Written::Key(key.clone()));
            }
            KvWrite::Clear(key) => {
                batch.delete_cf(tree, key);
                written.push(Written::Key(key.clone()));
            }
            KvWrite::ClearRange(range) => {
                batch.delete_range_cf(tree, &range.start, &range.end);
                written.push(Written::Range(range.clone()));
            }
        }
    }
    let mut latest = None;
    for (version, entry) in log {
        batch.put_cf(log_family, version.to_be_bytes(), entry);
        latest = latest.max(Some(*version));
    }
    // A write that fails may still have reached the WAL. Later
    // transactions treat its writes as made, so they conflict with it.
    let committed = db.write_opt(batch, &synced());
    shared.validator.lock().expect("validator").record(written);
    committed.map_err(|_| CommitError::Unknown)?;
    if let Some(latest) = latest {
        shared.latest.send_if_modified(|current| {
            let advanced = latest > *current;
            *current = (*current).max(latest);
            advanced
        });
    }
    Ok(())
}
