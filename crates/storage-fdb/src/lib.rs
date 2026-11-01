//! InodeDB's FoundationDB backend. A store lives under one key prefix: the
//! tree beneath `<prefix> 0x00` and the log beneath `<prefix> 0x01`, keyed
//! by big-endian version and then a chunk number, since an entry can pass
//! FoundationDB's value limit; `<prefix> 0x02` holds the newest logged
//! version. Stores share a cluster without touching. Each `Txn` is one
//! FoundationDB transaction, whose serializable reads and conflict checks
//! are the trait's. `wait_past` watches the newest version's key, which
//! every transaction that appends to the log sets.
//!
//! A writer's handle starts its reads and transactions from the read version
//! of its latest commit, or of its latest request for one, for up to a
//! second, so a round of reads or a commit skips asking the cluster for a
//! read version first.
//!
//! The client library runs a network thread per process; `boot` starts it,
//! and the returned guard stops it when dropped.
#![cfg(feature = "fdb")]

use foundationdb::api::NetworkAutoStop;
use foundationdb::options::StreamingMode;
use foundationdb::{Database, FdbError, RangeOption, Transaction};
use futures_util::future::try_join_all;
use inodedb_core::keys::{KeyRange, KvRead, KvResult, Pairs};
use inodedb_core::types::Version;
use inodedb_storage::{
    CommitError, MAX_KEY, MAX_TRANSACTION, MAX_VALUE, Storage, StorageError, Txn,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TREE: u8 = 0x00;
const LOG: u8 = 0x01;
const LATEST: u8 = 0x02;

/// How long a writer's handle reuses a read version it knows, well inside
/// the five seconds FoundationDB keeps old versions readable.
const REUSE: Duration = Duration::from_secs(1);

/// The longest prefix, which with the tree's byte keeps the trait's keys
/// within FoundationDB's.
pub const MAX_PREFIX: usize = 10_000 - MAX_KEY - 1;

/// What the network thread needs stopped before the process exits.
pub struct Network(#[allow(dead_code)] NetworkAutoStop);

static BOOTED: Mutex<bool> = Mutex::new(false);

/// Starts the client's network thread. It runs once per process: a second
/// call returns None.
pub fn boot() -> Option<Network> {
    let mut booted = BOOTED.lock().expect("no panics holding the boot flag");
    if *booted {
        return None;
    }
    *booted = true;
    // Safety: the caller holds the guard until the process is done with
    // FoundationDB, and drops it before exiting.
    Some(Network(unsafe { foundationdb::boot() }))
}

/// Starts the network thread for a process that never stops it, such as a
/// test binary; once it runs, does nothing.
pub fn boot_for_process() {
    if let Some(network) = boot() {
        std::mem::forget(network);
    }
}

#[derive(Clone)]
pub struct Fdb {
    db: Arc<Database>,
    prefix: Arc<[u8]>,
    /// On a writer's handle, a read version that holds every commit the
    /// handle made, and when the handle learned it.
    known: Option<Arc<Mutex<Known>>>,
}

type Known = Option<(i64, Instant)>;

fn unavailable(error: impl std::fmt::Display) -> StorageError {
    StorageError::Unavailable(error.to_string())
}

impl Fdb {
    /// Opens the store under `prefix` in the cluster `cluster_file` names,
    /// or the default cluster. The network thread must be running.
    pub fn open(cluster_file: Option<&str>, prefix: &[u8]) -> Result<Fdb, StorageError> {
        if prefix.len() > MAX_PREFIX {
            return Err(StorageError::Unavailable(format!(
                "a store's prefix holds at most {MAX_PREFIX} bytes"
            )));
        }
        let db = Database::new(cluster_file).map_err(unavailable)?;
        Ok(Fdb {
            db: Arc::new(db),
            prefix: prefix.into(),
            known: None,
        })
    }

    fn tree(&self, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.prefix.len() + 1 + key.len());
        out.extend_from_slice(&self.prefix);
        out.push(TREE);
        out.extend_from_slice(key);
        out
    }

    /// Where a version's log chunks begin.
    fn log(&self, version: Version) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.prefix.len() + 11);
        out.extend_from_slice(&self.prefix);
        out.push(LOG);
        out.extend_from_slice(&version.to_be_bytes());
        out
    }

    fn chunk(&self, version: Version, index: u16) -> Vec<u8> {
        let mut out = self.log(version);
        out.extend_from_slice(&index.to_be_bytes());
        out
    }

    /// Just past every log key.
    fn log_end(&self) -> Vec<u8> {
        let mut out = self.prefix.to_vec();
        out.push(LOG + 1);
        out
    }

    fn latest_key(&self) -> Vec<u8> {
        let mut out = self.prefix.to_vec();
        out.push(LATEST);
        out
    }

    /// A transaction at the latest committed state.
    fn latest_trx(&self) -> Result<Transaction, StorageError> {
        self.db.create_trx().map_err(unavailable)
    }

    /// A transaction for the trait's reads and commits: on a writer's
    /// handle, from a recent read version it knows.
    async fn trx(&self) -> Result<Transaction, StorageError> {
        let trx = self.latest_trx()?;
        let Some(known) = &self.known else {
            return Ok(trx);
        };
        let recent = known
            .lock()
            .expect("no thread panics holding the read version")
            .filter(|(_, at)| at.elapsed() < REUSE);
        match recent {
            Some((version, _)) => trx.set_read_version(version),
            None => {
                let asked = Instant::now();
                let version = trx.get_read_version().await.map_err(unavailable)?;
                self.learn(version, asked);
            }
        }
        Ok(trx)
    }

    /// Records a read version that holds every commit so far.
    fn learn(&self, version: i64, at: Instant) {
        if let Some(known) = &self.known {
            let mut known = known
                .lock()
                .expect("no thread panics holding the read version");
            if known.is_none_or(|(newest, _)| version > newest) {
                *known = Some((version, at));
            }
        }
    }

    /// Drops the known read version, so the next transaction asks for the
    /// latest.
    fn forget(&self) {
        if let Some(known) = &self.known {
            *known
                .lock()
                .expect("no thread panics holding the read version") = None;
        }
    }

    /// Up to `limit` pairs of a range of the tree, with the prefix taken
    /// off their keys.
    async fn scan(
        &self,
        trx: &Transaction,
        range: &KeyRange,
        limit: usize,
        snapshot: bool,
    ) -> Result<Pairs, StorageError> {
        let begin = self.tree(&range.start);
        let end = self.tree(&range.end);
        let strip = self.prefix.len() + 1;
        scan(trx, &begin, &end, limit, snapshot).await.map(|pairs| {
            pairs
                .into_iter()
                .map(|(key, value)| (key[strip..].to_vec(), value))
                .collect()
        })
    }

    /// The newest logged version, or zero.
    async fn latest(&self, trx: &Transaction) -> Result<Version, StorageError> {
        match trx
            .get(&self.latest_key(), true)
            .await
            .map_err(unavailable)?
        {
            Some(bytes) => bytes
                .as_ref()
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| StorageError::Corrupt("the newest version".into())),
            None => Ok(0),
        }
    }

    fn version_of(&self, key: &[u8]) -> Result<Version, StorageError> {
        let start = self.prefix.len() + 1;
        key.get(start..start + 8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| StorageError::Corrupt("a log key".into()))
    }
}

/// Up to `limit` pairs from `begin` to `end`, paging as FoundationDB
/// returns them.
async fn scan(
    trx: &Transaction,
    begin: &[u8],
    end: &[u8],
    limit: usize,
    snapshot: bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
    let mut pairs = Vec::new();
    if limit == 0 {
        return Ok(pairs);
    }
    let mut option = Some(RangeOption {
        limit: (limit != usize::MAX).then_some(limit),
        mode: StreamingMode::WantAll,
        ..RangeOption::from((begin, end))
    });
    let mut iteration = 1;
    while let Some(current) = option {
        let values = trx
            .get_range(&current, iteration, snapshot)
            .await
            .map_err(unavailable)?;
        pairs.extend(
            values
                .iter()
                .map(|pair| (pair.key().to_vec(), pair.value().to_vec())),
        );
        option = current.next_range(&values);
        iteration += 1;
    }
    Ok(pairs)
}

/// What a failed commit means for the trait.
fn commit_error(error: &FdbError) -> CommitError {
    match error.code() {
        1020 => CommitError::Conflict,
        2101..=2103 => CommitError::TooLarge,
        _ if error.is_maybe_committed() => CommitError::Unknown,
        _ => CommitError::Unavailable(error.to_string()),
    }
}

impl Storage for Fdb {
    type Txn = FdbTxn;

    fn for_writer(&self) -> Fdb {
        Fdb {
            known: Some(Arc::default()),
            ..self.clone()
        }
    }

    async fn snapshot_read(&self, reads: Vec<KvRead>) -> Result<Vec<KvResult>, StorageError> {
        let trx = self.trx().await?;
        // Every read goes out at once, so a round costs one trip to the
        // cluster rather than one per key.
        let results = try_join_all(reads.iter().map(|read| async {
            Ok(match read {
                KvRead::Get(key) => KvResult::Get(
                    trx.get(&self.tree(key), true)
                        .await
                        .map_err(unavailable)?
                        .map(|value| value.to_vec()),
                ),
                KvRead::Scan { range, limit } => {
                    KvResult::Scan(self.scan(&trx, range, *limit, true).await?)
                }
            })
        }))
        .await;
        // A read version too old to read, among other failures.
        if results.is_err() {
            self.forget();
        }
        results
    }

    async fn begin(&self) -> Result<FdbTxn, StorageError> {
        Ok(FdbTxn {
            store: self.clone(),
            trx: self.trx().await?,
            bytes: 0,
            too_large: false,
            latest: None,
        })
    }

    async fn read_log(
        &self,
        from: Version,
        limit: usize,
    ) -> Result<Vec<(Version, Vec<u8>)>, StorageError> {
        let trx = self.latest_trx()?;
        let (begin, end) = (self.log(from), self.log_end());
        let mut entries: Vec<(Version, Vec<u8>)> = Vec::new();
        let mut option = Some(RangeOption {
            mode: StreamingMode::Iterator,
            ..RangeOption::from((begin.as_slice(), end.as_slice()))
        });
        let mut iteration = 1;
        // Chunks arrive in order, each entry's together.
        'pages: while let Some(current) = option {
            let values = trx
                .get_range(&current, iteration, true)
                .await
                .map_err(unavailable)?;
            for pair in values.iter() {
                let version = self.version_of(pair.key())?;
                if let Some((last, bytes)) = entries.last_mut()
                    && *last == version
                {
                    bytes.extend_from_slice(pair.value());
                } else if entries.len() == limit {
                    break 'pages;
                } else {
                    entries.push((version, pair.value().to_vec()));
                }
            }
            option = current.next_range(&values);
            iteration += 1;
        }
        Ok(entries)
    }

    async fn wait_past(&self, version: Version) -> Result<Version, StorageError> {
        let key = self.latest_key();
        loop {
            let trx = self.latest_trx()?;
            let latest = self.latest(&trx).await?;
            if latest > version {
                return Ok(latest);
            }
            // The watch fires once the key differs from what this
            // transaction's read version saw.
            let watch = trx.watch(&key);
            trx.commit().await.map_err(|error| unavailable(*error))?;
            watch.await.map_err(unavailable)?;
        }
    }

    async fn truncate_log(&self, below: Version) -> Result<(), StorageError> {
        let mut trx = self.latest_trx()?;
        loop {
            trx.clear_range(&self.log(0), &self.log(below));
            match trx.commit().await {
                Ok(_) => return Ok(()),
                Err(error) => trx = error.on_error().await.map_err(unavailable)?,
            }
        }
    }
}

pub struct FdbTxn {
    store: Fdb,
    trx: Transaction,
    /// Bytes written so far, as the limits count them.
    bytes: usize,
    too_large: bool,
    /// The newest version this transaction logs.
    latest: Option<Version>,
}

impl FdbTxn {
    fn count(&mut self, key: &[u8], value: usize) {
        self.too_large |= key.len() > MAX_KEY || value > MAX_VALUE;
        self.bytes += key.len() + value;
    }
}

impl Txn for FdbTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .trx
            .get(&self.store.tree(key), false)
            .await
            .map_err(unavailable)?
            .map(|value| value.to_vec()))
    }

    async fn scan(&mut self, range: KeyRange, limit: usize) -> Result<Pairs, StorageError> {
        self.store.scan(&self.trx, &range, limit, false).await
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.count(key, value.len());
        self.trx.set(&self.store.tree(key), value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.count(key, 0);
        self.trx.clear(&self.store.tree(key));
    }

    fn clear_range(&mut self, range: KeyRange) {
        self.bytes += range.start.len() + range.end.len();
        self.trx
            .clear_range(&self.store.tree(&range.start), &self.store.tree(&range.end));
    }

    fn append_log(&mut self, version: Version, entry: &[u8]) {
        self.bytes += entry.len() + 18;
        self.latest = self.latest.max(Some(version));
        let chunks = entry.chunks(MAX_VALUE).enumerate();
        if entry.is_empty() {
            self.trx.set(&self.store.chunk(version, 0), entry);
        }
        for (index, chunk) in chunks {
            self.trx
                .set(&self.store.chunk(version, index as u16), chunk);
        }
    }

    async fn commit(self) -> Result<(), CommitError> {
        if self.too_large || self.bytes > MAX_TRANSACTION {
            self.store.forget();
            return Err(CommitError::TooLarge);
        }
        if let Some(latest) = self.latest {
            self.trx
                .set(&self.store.latest_key(), &latest.to_be_bytes());
        }
        match self.trx.commit().await {
            Ok(committed) => {
                // A transaction that wrote nothing reports -1.
                if let Ok(version) = committed.committed_version()
                    && version >= 0
                {
                    self.store.learn(version, Instant::now());
                }
                Ok(())
            }
            Err(error) => {
                self.store.forget();
                Err(commit_error(&error))
            }
        }
    }
}
