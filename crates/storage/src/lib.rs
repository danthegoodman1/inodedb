//! The storage trait InodeDB's backends implement, the procedures the writer's
//! actions run over it, and the memory backend.
//!
//! The contract, which `storage-conformance` states as tests:
//!
//! - Keys and scans order bytes lexicographically.
//! - A snapshot read sees one committed state.
//! - A transaction sees its own writes and never another's uncommitted
//!   ones. Committed transactions are serializable: a commit fails with
//!   `Conflict` when another commit, after this transaction began, wrote a
//!   key it read or one inside a range it scanned.
//! - Log entries read back in version order.
//! - `Unknown` means the commit may or may not have applied.
//! - A commit is durable before it returns.
//! - Keys hold up to 9,000 bytes, values up to 100,000, and a transaction
//!   writes up to 10,000,000, matching FoundationDB on every backend. Its
//!   keys hold 10,000, which leaves room for a store's prefix.

pub mod memory;
pub mod occ;

use inodedb_core::keys::{self, KeyRange, KvRead, KvResult, KvWrite, Pairs, WriteSet};
use inodedb_core::types::Version;
use inodedb_core::writer::CommitOutcome;
use std::future::Future;

pub use memory::Memory;

pub const MAX_KEY: usize = 9_000;
pub const MAX_VALUE: usize = 100_000;
pub const MAX_TRANSACTION: usize = 10_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    Unavailable(String),
    Corrupt(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Unavailable(why) => write!(f, "storage unavailable: {why}"),
            StorageError::Corrupt(why) => write!(f, "storage corrupt: {why}"),
        }
    }
}

impl std::error::Error for StorageError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitError {
    Conflict,
    Unknown,
    TooLarge,
    Unavailable(String),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::Conflict => f.write_str("conflict"),
            CommitError::Unknown => f.write_str("outcome unknown"),
            CommitError::TooLarge => f.write_str("transaction too large"),
            CommitError::Unavailable(why) => write!(f, "storage unavailable: {why}"),
        }
    }
}

impl std::error::Error for CommitError {}

pub trait Storage: Send + Sync + 'static {
    type Txn: Txn;

    /// Runs every read against one committed state.
    fn snapshot_read(
        &self,
        reads: Vec<KvRead>,
    ) -> impl Future<Output = Result<Vec<KvResult>, StorageError>> + Send;

    /// Begins a transaction at the latest committed state.
    fn begin(&self) -> impl Future<Output = Result<Self::Txn, StorageError>> + Send;

    /// Log entries from version `from` on, in version order.
    fn read_log(
        &self,
        from: Version,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(Version, Vec<u8>)>, StorageError>> + Send;

    /// Resolves with the latest logged version once one later than
    /// `version` exists.
    fn wait_past(
        &self,
        version: Version,
    ) -> impl Future<Output = Result<Version, StorageError>> + Send;

    /// Deletes log entries older than `below`.
    fn truncate_log(&self, below: Version)
    -> impl Future<Output = Result<(), StorageError>> + Send;
}

pub trait Txn: Send {
    fn get(
        &mut self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send;

    fn scan(
        &mut self,
        range: KeyRange,
        limit: usize,
    ) -> impl Future<Output = Result<Pairs, StorageError>> + Send;

    fn set(&mut self, key: &[u8], value: &[u8]);

    fn clear(&mut self, key: &[u8]);

    fn clear_range(&mut self, range: KeyRange);

    fn append_log(&mut self, version: Version, entry: &[u8]);

    fn commit(self) -> impl Future<Output = Result<(), CommitError>> + Send;
}

/// Checks a transaction's writes against the size limits.
pub fn check_limits<'a>(
    writes: impl IntoIterator<Item = &'a KvWrite>,
    log: impl IntoIterator<Item = &'a [u8]>,
) -> Result<(), CommitError> {
    let mut total = 0usize;
    for write in writes {
        match write {
            KvWrite::Set(key, value) => {
                if key.len() > MAX_KEY || value.len() > MAX_VALUE {
                    return Err(CommitError::TooLarge);
                }
                total += key.len() + value.len();
            }
            KvWrite::Clear(key) => {
                if key.len() > MAX_KEY {
                    return Err(CommitError::TooLarge);
                }
                total += key.len();
            }
            KvWrite::ClearRange(range) => total += range.start.len() + range.end.len(),
        }
    }
    for entry in log {
        total += entry.len() + 18;
    }
    if total > MAX_TRANSACTION {
        return Err(CommitError::TooLarge);
    }
    Ok(())
}

/// Carries out a writer's `Commit`: checks the head record holds `expect`,
/// then applies the writes in one transaction.
pub async fn commit<S: Storage>(
    storage: &S,
    expect: Option<&[u8]>,
    writes: &WriteSet,
) -> CommitOutcome {
    let Ok(mut txn) = storage.begin().await else {
        return CommitOutcome::Failed;
    };
    let head = match txn.get(&keys::head()).await {
        Ok(head) => head,
        Err(_) => return CommitOutcome::Failed,
    };
    if head.as_deref() != expect {
        return CommitOutcome::Fenced;
    }
    apply(&mut txn, writes);
    match txn.commit().await {
        Ok(()) => CommitOutcome::Committed,
        Err(CommitError::Conflict) => CommitOutcome::Fenced,
        Err(CommitError::Unknown) => CommitOutcome::Unknown,
        Err(CommitError::TooLarge | CommitError::Unavailable(_)) => CommitOutcome::Failed,
    }
}

pub fn apply<T: Txn>(txn: &mut T, writes: &WriteSet) {
    for write in &writes.writes {
        match write {
            KvWrite::Set(key, value) => txn.set(key, value),
            KvWrite::Clear(key) => txn.clear(key),
            KvWrite::ClearRange(range) => txn.clear_range(range.clone()),
        }
    }
    for (version, entry) in &writes.log {
        txn.append_log(*version, entry);
    }
}

/// Every key and value in one snapshot, for checking a whole store. It
/// holds the store in memory, and on FoundationDB it must read within one
/// transaction's five seconds.
pub async fn dump<S: Storage>(storage: &S) -> Result<Pairs, StorageError> {
    let everything = KeyRange {
        start: vec![keys::FORMAT],
        end: vec![keys::FORMAT + 1],
    };
    match storage
        .snapshot_read(vec![KvRead::Scan {
            range: everything,
            limit: usize::MAX,
        }])
        .await?
        .pop()
    {
        Some(KvResult::Scan(pairs)) => Ok(pairs),
        _ => Err(StorageError::Corrupt(
            "a scan answered as something else".into(),
        )),
    }
}

/// Carries out a writer's `Resolve`: reads the head record and writes it
/// back in one transaction, so a commit still in doubt, which read it
/// first, conflicts and never applies. Returns the head it found.
pub async fn resolve<S: Storage>(storage: &S) -> Result<Option<Vec<u8>>, StorageError> {
    loop {
        let mut txn = storage.begin().await?;
        let head = txn.get(&keys::head()).await?;
        match &head {
            Some(bytes) => txn.set(&keys::head(), bytes),
            None => txn.clear(&keys::head()),
        }
        match txn.commit().await {
            Ok(()) => return Ok(head),
            Err(CommitError::Conflict | CommitError::Unknown) => continue,
            Err(CommitError::TooLarge) => {
                return Err(StorageError::Corrupt("head too large".into()));
            }
            Err(CommitError::Unavailable(why)) => return Err(StorageError::Unavailable(why)),
        }
    }
}
