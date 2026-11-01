//! Optimistic validation for backends that apply commits one at a time:
//! a commit conflicts when a commit since its transaction began wrote what
//! it read.

use inodedb_core::keys::KeyRange;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug)]
pub enum Written {
    Key(Vec<u8>),
    Range(KeyRange),
}

#[derive(Clone, Debug, Default)]
pub struct ReadSet {
    pub keys: BTreeSet<Vec<u8>>,
    pub ranges: Vec<KeyRange>,
}

impl ReadSet {
    fn touches(&self, written: &Written) -> bool {
        match written {
            Written::Key(key) => {
                self.keys.contains(key) || self.ranges.iter().any(|range| range.contains(key))
            }
            Written::Range(written) => {
                self.keys.iter().any(|key| written.contains(key))
                    || self
                        .ranges
                        .iter()
                        .any(|range| range.start < written.end && written.start < range.end)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Validator {
    seq: u64,
    recent: VecDeque<(u64, Vec<Written>)>,
    open: BTreeMap<u64, u64>,
    next_txn: u64,
}

impl Validator {
    /// Opens a transaction; returns its id and the sequence it began at.
    pub fn begin(&mut self) -> (u64, u64) {
        let id = self.next_txn;
        self.next_txn += 1;
        self.open.insert(id, self.seq);
        (id, self.seq)
    }

    pub fn end(&mut self, id: u64) {
        self.open.remove(&id);
        let oldest = self.open.values().copied().min().unwrap_or(self.seq);
        while self.recent.front().is_some_and(|(seq, _)| *seq <= oldest) {
            self.recent.pop_front();
        }
    }

    /// Whether a transaction that began at `start` and read `reads` may
    /// commit.
    pub fn valid(&self, start: u64, reads: &ReadSet) -> bool {
        self.recent
            .iter()
            .filter(|(seq, _)| *seq > start)
            .all(|(_, writes)| !writes.iter().any(|written| reads.touches(written)))
    }

    /// Records a commit's writes and returns its sequence number.
    pub fn record(&mut self, writes: Vec<Written>) -> u64 {
        self.seq += 1;
        if !self.open.is_empty() {
            self.recent.push_back((self.seq, writes));
        }
        self.seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_to_a_read_key_conflicts() {
        let mut validator = Validator::default();
        let (first, start) = validator.begin();
        let (_, other_start) = validator.begin();
        validator.record(vec![Written::Key(b"k".to_vec())]);
        let reads = ReadSet {
            keys: BTreeSet::from([b"k".to_vec()]),
            ranges: Vec::new(),
        };
        assert!(!validator.valid(start, &reads));
        let ranges = ReadSet {
            keys: BTreeSet::new(),
            ranges: vec![KeyRange::prefix(b"k".to_vec())],
        };
        assert!(!validator.valid(other_start, &ranges));
        validator.end(first);
        let (_, later) = validator.begin();
        assert!(validator.valid(later, &reads));
    }
}
