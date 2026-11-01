//! The memory backend: a map under a lock. The simulator drives it through
//! its synchronous methods and injects faults around them; tests and the
//! server use it through `Storage`.

use crate::occ::{ReadSet, Validator, Written};
use crate::{CommitError, Storage, StorageError, Txn, check_limits};
use inodedb_core::keys::{self, KeyRange, KvRead, KvResult, KvWrite, Pairs, WriteSet};
use inodedb_core::types::Version;
use inodedb_core::writer::CommitOutcome;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::watch;

/// A failure the next commits through `Storage` suffer, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The commit applies, but reports an unknown outcome.
    UnknownApplied,
    /// The commit never applies, and reports an unknown outcome.
    UnknownDropped,
    Unavailable,
}

#[derive(Default)]
struct State {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    log: BTreeMap<Version, Vec<u8>>,
    validator: Validator,
    faults: std::collections::VecDeque<Fault>,
}

impl State {
    fn scan(&self, range: &KeyRange, limit: usize) -> Pairs {
        self.data
            .range::<[u8], _>((
                Bound::Included(range.start.as_slice()),
                Bound::Excluded(range.end.as_slice()),
            ))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn read(&self, reads: &[KvRead]) -> Vec<KvResult> {
        reads
            .iter()
            .map(|read| match read {
                KvRead::Get(key) => KvResult::Get(self.data.get(key).cloned()),
                KvRead::Scan { range, limit } => KvResult::Scan(self.scan(range, *limit)),
            })
            .collect()
    }

    fn write(&mut self, writes: &[KvWrite], log: &[(Version, Vec<u8>)]) -> Version {
        let mut written = Vec::with_capacity(writes.len());
        for write in writes {
            match write {
                KvWrite::Set(key, value) => {
                    self.data.insert(key.clone(), value.clone());
                    written.push(Written::Key(key.clone()));
                }
                KvWrite::Clear(key) => {
                    self.data.remove(key);
                    written.push(Written::Key(key.clone()));
                }
                KvWrite::ClearRange(range) => {
                    let doomed: Vec<Vec<u8>> = self
                        .scan(range, usize::MAX)
                        .into_iter()
                        .map(|(key, _)| key)
                        .collect();
                    for key in doomed {
                        self.data.remove(&key);
                    }
                    written.push(Written::Range(range.clone()));
                }
            }
        }
        for (version, entry) in log {
            self.log.insert(*version, entry.clone());
        }
        self.validator.record(written);
        self.latest()
    }

    fn latest(&self) -> Version {
        self.log.keys().next_back().copied().unwrap_or(0)
    }
}

struct Shared {
    state: Mutex<State>,
    latest: watch::Sender<Version>,
}

#[derive(Clone)]
pub struct Memory {
    shared: Arc<Shared>,
}

impl Default for Memory {
    fn default() -> Memory {
        Memory::new()
    }
}

impl Memory {
    pub fn new() -> Memory {
        Memory {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                latest: watch::Sender::new(0),
            }),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.shared
            .state
            .lock()
            .expect("no thread panics holding the state")
    }

    /// Runs reads against the current state.
    pub fn read_now(&self, reads: &[KvRead]) -> Vec<KvResult> {
        self.state().read(reads)
    }

    /// Checks the head record and applies a writer's commit at once.
    pub fn commit_now(&self, expect: Option<&[u8]>, writes: &WriteSet) -> CommitOutcome {
        let mut state = self.state();
        if state.data.get(&keys::head()).map(Vec::as_slice) != expect {
            return CommitOutcome::Fenced;
        }
        if check_limits(
            &writes.writes,
            writes.log.iter().map(|(_, entry)| entry.as_slice()),
        )
        .is_err()
        {
            return CommitOutcome::Failed;
        }
        let latest = state.write(&writes.writes, &writes.log);
        drop(state);
        self.shared.latest.send_replace(latest);
        CommitOutcome::Committed
    }

    /// The head record now.
    pub fn head_now(&self) -> Option<Vec<u8>> {
        self.state().data.get(&keys::head()).cloned()
    }

    /// Every key and value, in order.
    pub fn dump(&self) -> Pairs {
        self.state()
            .data
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    pub fn log_now(&self, from: Version, limit: usize) -> Vec<(Version, Vec<u8>)> {
        self.state()
            .log
            .range(from..)
            .take(limit)
            .map(|(version, entry)| (*version, entry.clone()))
            .collect()
    }

    /// Makes a later commit through `Storage` fail this way.
    pub fn inject(&self, fault: Fault) {
        self.state().faults.push_back(fault);
    }

    pub fn truncate_now(&self, below: Version) {
        let mut state = self.state();
        state.log = state.log.split_off(&below);
    }
}

impl Storage for Memory {
    type Txn = MemoryTxn;

    async fn snapshot_read(&self, reads: Vec<KvRead>) -> Result<Vec<KvResult>, StorageError> {
        Ok(self.read_now(&reads))
    }

    async fn begin(&self) -> Result<MemoryTxn, StorageError> {
        let (id, start) = self.state().validator.begin();
        Ok(MemoryTxn {
            memory: self.clone(),
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
        Ok(self.log_now(from, limit))
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
        self.truncate_now(below);
        Ok(())
    }
}

pub struct MemoryTxn {
    memory: Memory,
    id: u64,
    start: u64,
    reads: ReadSet,
    /// Buffered writes by key: a value, or None for a clear.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ranges: Vec<KeyRange>,
    log: Vec<(Version, Vec<u8>)>,
}

impl MemoryTxn {
    fn buffered(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        if let Some(write) = self.writes.get(key) {
            return Some(write.clone());
        }
        if self.ranges.iter().any(|range| range.contains(key)) {
            return Some(None);
        }
        None
    }
}

impl Drop for MemoryTxn {
    fn drop(&mut self) {
        if let Ok(mut state) = self.memory.shared.state.lock() {
            state.validator.end(self.id);
        }
    }
}

impl Txn for MemoryTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(buffered) = self.buffered(key) {
            return Ok(buffered);
        }
        self.reads.keys.insert(key.to_vec());
        Ok(self.memory.state().data.get(key).cloned())
    }

    async fn scan(&mut self, range: KeyRange, limit: usize) -> Result<Pairs, StorageError> {
        let stored = self.memory.state().scan(&range, usize::MAX);
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
        check_limits(&writes, self.log.iter().map(|(_, entry)| entry.as_slice()))?;
        let mut state = self.memory.state();
        if !state.validator.valid(self.start, &self.reads) {
            return Err(CommitError::Conflict);
        }
        let fault = state.faults.pop_front();
        match fault {
            Some(Fault::UnknownDropped) => return Err(CommitError::Unknown),
            Some(Fault::Unavailable) => return Err(CommitError::Unavailable("injected".into())),
            Some(Fault::UnknownApplied) | None => {}
        }
        let latest = state.write(&writes, &self.log);
        drop(state);
        self.memory.shared.latest.send_replace(latest);
        match fault {
            Some(Fault::UnknownApplied) => Err(CommitError::Unknown),
            _ => Ok(()),
        }
    }
}
