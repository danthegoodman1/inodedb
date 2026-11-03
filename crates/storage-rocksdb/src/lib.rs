//! InodeDB's RocksDB backend: the tree in the default column family and
//! the log in its own, keyed by big-endian version. Every commit syncs the
//! WAL before it returns, and RocksDB's lock file admits one process.
//! Commits apply one at a time under a mutex and are validated
//! optimistically; snapshot reads use a RocksDB snapshot.
//!
//! A sync that must also grow a file commits the file system's journal, which
//! costs up to four times the write itself on local NVMe and twice on gp3
//! (`BENCHMARKS.md`, "Devices"). So the WAL's files are written once when the
//! store opens and overwritten from then on.

use inodedb_core::keys::{self, KeyRange, KvRead, KvResult, KvWrite, Pairs, WriteSet};
use inodedb_core::types::Version;
use inodedb_core::writer::CommitOutcome;
use inodedb_storage::occ::{ReadSet, Validator, Written};
use inodedb_storage::{CommitError, Storage, StorageError, Txn, check_limits};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Direction,
    IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::watch;

const LOG: &str = "log";
/// The family that writes the WAL's files when the store opens, then goes.
const FILLER: &str = "filler";
/// The family whose one key keeps the WAL's files from coming free until all
/// are written, then goes.
const PIN: &str = "pin";
/// What each column family holds in memory before a flush, and so about what
/// one WAL file takes.
const WRITE_BUFFER: usize = 64 << 20;
/// WAL files kept to overwrite, beside the one in use. Files older than the
/// newest must stay until both families flush what they hold, so a few are in
/// use at a time.
const WAL_FILES: usize = 4;
/// The live WAL that forces a flush of what the oldest file holds. Files run
/// short of full, since a memtable fills before its WAL does, so this leaves
/// room to stay within the files kept.
const MAX_LIVE_WAL: usize = (WAL_FILES - 1) * WRITE_BUFFER;
/// What the block cache holds, beside the OS's page cache; the writer's own
/// cache answers most reads, so storage sees the rest.
const BLOCK_CACHE: usize = 256 << 20;

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
        options.set_recycle_log_file_num(WAL_FILES);
        options.set_max_total_wal_size(MAX_LIVE_WAL as u64);
        options.set_max_background_jobs(4);
        // One thread writes at a time, so inserts need no synchronization.
        options.set_allow_concurrent_memtable_write(false);
        options.set_enable_write_thread_adaptive_yield(false);
        let cache = Cache::new_lru_cache(BLOCK_CACHE);
        // A crash while the WAL's files are written leaves their families, and
        // RocksDB opens a store only with every family it holds.
        let families = vec![
            ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, family(&cache)),
            ColumnFamilyDescriptor::new(LOG, family(&cache)),
            ColumnFamilyDescriptor::new(FILLER, filler()),
            ColumnFamilyDescriptor::new(PIN, Options::default()),
        ];
        let mut db = DB::open_cf_descriptors(&options, path, families).map_err(unavailable)?;
        write_wal_files(&mut db).map_err(unavailable)?;
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

/// A family's options: point reads check a Bloom filter first, in the memtable
/// and in each table, since most keys a cold read asks for live in one level.
fn family(cache: &Cache) -> Options {
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_bloom_filter(10.0, false);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    let mut options = Options::default();
    options.set_block_based_table_factory(&table);
    options.set_write_buffer_size(WRITE_BUFFER);
    options.set_memtable_whole_key_filtering(true);
    options.set_memtable_prefix_bloom_ratio(0.05);
    options.set_compression_type(DBCompressionType::Lz4);
    options
}

/// The filler's options: its zeros flush to almost nothing.
fn filler() -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Lz4);
    options
}

/// Writes the WAL's files, so commits overwrite written blocks, and a sync
/// has no journal to commit. RocksDB reuses a WAL file once every family has
/// flushed what it holds, and forgets such files when it closes, so each open
/// writes them. The pin's key sits in the first file and holds every later
/// one, so each pass takes a new file, and they all come free when the pin
/// flushes. One more pass fills the file then in use, and moves on to a written
/// one. Passes stay short of the live WAL that would flush the pin early; a
/// file that a memtable outlasts grows once, and keeps its length from then on.
fn write_wal_files(db: &mut DB) -> Result<(), rocksdb::Error> {
    const CHUNK: usize = 1 << 20;
    let chunks = MAX_LIVE_WAL / WAL_FILES / CHUNK - 1;
    let zeros = vec![0u8; CHUNK];
    let mut options = WriteOptions::default();
    options.set_sync(false);
    {
        let filler = db.cf_handle(FILLER).expect("opened with the store");
        let pin = db.cf_handle(PIN).expect("opened with the store");
        let fill = || -> Result<(), rocksdb::Error> {
            for chunk in 0..chunks {
                db.put_cf_opt(filler, chunk.to_be_bytes(), &zeros, &options)?;
            }
            db.flush_wal(true)?;
            db.flush_cf(filler)
        };
        db.put_cf_opt(pin, b"pin", b"", &options)?;
        for _ in 0..WAL_FILES {
            fill()?;
        }
        db.flush_cf(pin)?;
        fill()?;
    }
    db.drop_cf(FILLER)?;
    db.drop_cf(PIN)
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

    async fn commit_group(&self, expect: Option<Vec<u8>>, writes: Arc<WriteSet>) -> CommitOutcome {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || commit_group(&shared, expect.as_deref(), &writes))
            .await
            .unwrap_or(CommitOutcome::Failed)
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
    write(shared, writes, log)
}

/// Commits a group if the head record holds `expect`, reading and writing
/// under the commit lock, so nothing commits between them.
fn commit_group(shared: &Shared, expect: Option<&[u8]>, writes: &WriteSet) -> CommitOutcome {
    let _commit = shared
        .commit
        .lock()
        .expect("no thread panics holding the commit lock");
    match shared.db.get(keys::head()) {
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

/// Applies writes in one synced write. The caller holds the commit lock.
fn write(
    shared: &Shared,
    writes: &[KvWrite],
    log: &[(Version, Vec<u8>)],
) -> Result<(), CommitError> {
    let db = &shared.db;
    let tree = db
        .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
        .expect("the default family exists");
    let log_family = db.cf_handle(LOG).expect("the log family exists");
    let mut batch = WriteBatch::default();
    for write in writes {
        match write {
            KvWrite::Set(key, value) => batch.put_cf(tree, key, value),
            KvWrite::Clear(key) => batch.delete_cf(tree, key),
            KvWrite::ClearRange(range) => batch.delete_range_cf(tree, &range.start, &range.end),
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
    shared
        .validator
        .lock()
        .expect("validator")
        .record(|| writes.iter().map(Written::of).collect());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A crash while the store writes its WAL's files leaves their families
    /// behind; the next open takes them, and they go once the files are written.
    #[test]
    fn a_store_opens_after_a_crash_amid_writing_its_wal() {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let families = [rocksdb::DEFAULT_COLUMN_FAMILY_NAME, LOG, FILLER, PIN];
        drop(DB::open_cf(&options, dir.path(), families).expect("makes the families"));
        drop(Rocks::open(dir.path()).expect("opens"));
        let held = DB::list_cf(&Options::default(), dir.path()).expect("lists the families");
        assert_eq!(held, [rocksdb::DEFAULT_COLUMN_FAMILY_NAME, LOG]);
    }
}
