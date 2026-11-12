//! Storage keys and the key-value operations the core asks its owner to
//! carry out.
//!
//! Every key starts with a format byte. Numbers are big-endian, so ranges
//! scan in numeric order. The `l` prefix belongs to backends that keep the
//! log as keys; the core never writes under it.

use crate::types::{Ino, Version};

pub const FORMAT: u8 = 0x01;

const INODE: u8 = b'i';
const VALUE: u8 = b'v';
const ENTRY: u8 = b'e';
const BACKPTR: u8 = b'p';
const FREE: u8 = b'f';
const UNDER: u8 = b'u';
const DETACHED: u8 = b'd';
const SYSTEM: u8 = b's';
pub const LOG: u8 = b'l';

/// A half-open range of keys, `start..end`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl KeyRange {
    pub fn prefix(prefix: Vec<u8>) -> KeyRange {
        let end = prefix_end(&prefix);
        KeyRange { start: prefix, end }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && key < self.end.as_slice()
    }
}

/// The smallest key after every key that starts with `prefix`.
pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0xff; prefix.len() + 1]
}

/// One read in a snapshot read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvRead {
    Get(Vec<u8>),
    /// Up to `limit` pairs from `range`, in key order.
    Scan {
        range: KeyRange,
        limit: usize,
    },
}

/// Key/value pairs in key order.
pub type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvResult {
    Get(Option<Vec<u8>>),
    Scan(Pairs),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvWrite {
    Set(Vec<u8>, Vec<u8>),
    Clear(Vec<u8>),
    ClearRange(KeyRange),
}

/// Everything one storage transaction writes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteSet {
    pub writes: Vec<KvWrite>,
    /// Log entries by version, appended in order.
    pub log: Vec<(Version, Vec<u8>)>,
}

/// The longest store prefix the charge for a write leaves room for.
const STORE_PREFIX: usize = 16;
/// What FoundationDB charges for each write beside its keys and value.
const WRITE_CHARGE: usize = 72;
/// A log entry's key within a store: a tag, a version and a chunk's number.
const LOG_KEY: usize = 11;
/// The most of a log entry one value holds.
const LOG_CHUNK: usize = 100_000;

impl WriteSet {
    /// What FoundationDB counts toward a transaction's size for these
    /// writes, in a store whose prefix holds up to 16 bytes. It counts a
    /// key three times over when set, once written and twice in the range
    /// that guards it from conflicts, and four times when cleared, with
    /// about 72 bytes beside for each write (`BENCHMARKS.md`). A write of
    /// a small key therefore costs several times its bytes, and a group of
    /// such writes fits a transaction only by this count.
    pub fn charged(&self) -> usize {
        let writes: usize = self
            .writes
            .iter()
            .map(|write| match write {
                KvWrite::Set(key, value) => {
                    3 * (key.len() + STORE_PREFIX) + value.len() + WRITE_CHARGE
                }
                KvWrite::Clear(key) => 4 * (key.len() + STORE_PREFIX) + WRITE_CHARGE,
                KvWrite::ClearRange(range) => {
                    2 * (range.start.len() + range.end.len() + 2 * STORE_PREFIX) + WRITE_CHARGE
                }
            })
            .sum();
        writes
            + self
                .log
                .iter()
                .map(|(_, entry)| {
                    let chunks = entry.len().div_ceil(LOG_CHUNK).max(1);
                    chunks * (3 * (LOG_KEY + STORE_PREFIX) + WRITE_CHARGE) + entry.len()
                })
                .sum::<usize>()
    }

    /// The bytes of the keys and values written.
    pub fn bytes(&self) -> usize {
        let writes: usize = self
            .writes
            .iter()
            .map(|write| match write {
                KvWrite::Set(key, value) => key.len() + value.len(),
                KvWrite::Clear(key) => key.len(),
                KvWrite::ClearRange(range) => range.start.len() + range.end.len(),
            })
            .sum();
        writes
            + self
                .log
                .iter()
                .map(|(_, entry)| 18 + entry.len())
                .sum::<usize>()
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.push(KvWrite::Set(key, value));
    }

    pub fn clear(&mut self, key: Vec<u8>) {
        self.writes.push(KvWrite::Clear(key));
    }

    pub fn extend(&mut self, other: WriteSet) {
        self.writes.extend(other.writes);
        self.log.extend(other.log);
    }
}

fn key(tag: u8, capacity: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + capacity);
    key.push(FORMAT);
    key.push(tag);
    key
}

pub fn inode(ino: Ino) -> Vec<u8> {
    let mut key = key(INODE, 8);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

pub fn inode_range() -> KeyRange {
    KeyRange::prefix(key(INODE, 0))
}

pub fn value(ino: Ino, version: Version) -> Vec<u8> {
    let mut key = key(VALUE, 16);
    key.extend_from_slice(&ino.to_be_bytes());
    key.extend_from_slice(&version.to_be_bytes());
    key
}

pub fn value_range() -> KeyRange {
    KeyRange::prefix(key(VALUE, 0))
}

pub fn entry(dir: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = entry_prefix(dir);
    key.extend_from_slice(name);
    key
}

pub fn entry_prefix(dir: Ino) -> Vec<u8> {
    let mut key = key(ENTRY, 8 + 16);
    key.extend_from_slice(&dir.to_be_bytes());
    key
}

/// A directory's entries after `after`, or all of them.
pub fn entries_after(dir: Ino, after: Option<&[u8]>) -> KeyRange {
    let prefix = entry_prefix(dir);
    let end = prefix_end(&prefix);
    let start = match after {
        Some(name) => {
            let mut start = entry(dir, name);
            start.push(0);
            start
        }
        None => prefix,
    };
    KeyRange { start, end }
}

pub fn entry_range() -> KeyRange {
    KeyRange::prefix(key(ENTRY, 0))
}

pub fn backptr(ino: Ino, parent: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = backptr_prefix(ino);
    key.extend_from_slice(&parent.to_be_bytes());
    key.extend_from_slice(name);
    key
}

pub fn backptr_prefix(ino: Ino) -> Vec<u8> {
    let mut key = key(BACKPTR, 16 + 16);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

pub fn backptr_range() -> KeyRange {
    KeyRange::prefix(key(BACKPTR, 0))
}

pub fn free(version: Version, ino: Ino, value: Option<Version>) -> Vec<u8> {
    let mut key = key(FREE, 24);
    key.extend_from_slice(&version.to_be_bytes());
    key.extend_from_slice(&ino.to_be_bytes());
    if let Some(value) = value {
        key.extend_from_slice(&value.to_be_bytes());
    }
    key
}

pub fn free_range() -> KeyRange {
    KeyRange::prefix(key(FREE, 0))
}

/// A free-queue entry again, under one of the directories it records above
/// what it frees. Each directory's keys hold its part of the queue in the
/// queue's own order, so a consumer rooted there reads its entries alone.
pub fn free_under(dir: Ino, version: Version, ino: Ino, value: Option<Version>) -> Vec<u8> {
    let mut key = free_under_prefix(dir);
    key.extend_from_slice(&version.to_be_bytes());
    key.extend_from_slice(&ino.to_be_bytes());
    if let Some(value) = value {
        key.extend_from_slice(&value.to_be_bytes());
    }
    key
}

fn free_under_prefix(dir: Ino) -> Vec<u8> {
    let mut key = key(UNDER, 32);
    key.extend_from_slice(&dir.to_be_bytes());
    key
}

pub fn detached(version: Version, ino: Ino) -> Vec<u8> {
    let mut key = key(DETACHED, 16);
    key.extend_from_slice(&version.to_be_bytes());
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

pub fn detached_range() -> KeyRange {
    KeyRange::prefix(key(DETACHED, 0))
}

fn system(sub: u8, capacity: usize) -> Vec<u8> {
    let mut key = key(SYSTEM, 1 + capacity);
    key.push(sub);
    key
}

/// The last committed version and the next inode id.
pub fn head() -> Vec<u8> {
    system(b'h', 0)
}

pub fn request(id: u128, part: u16) -> Vec<u8> {
    let mut key = request_prefix(id);
    key.extend_from_slice(&part.to_be_bytes());
    key
}

pub fn request_prefix(id: u128) -> Vec<u8> {
    let mut key = system(b'r', 18);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

/// The value of a request's expiry key: how many parts its record has, so
/// deleting the record reads nothing. Empty for the one part nearly every
/// record has.
pub fn encode_parts(parts: u16) -> Vec<u8> {
    if parts == 1 {
        Vec::new()
    } else {
        parts.to_be_bytes().to_vec()
    }
}

/// The number of parts an expiry key's value gives.
pub fn decode_parts(value: &[u8]) -> Option<u16> {
    match value {
        [] => Some(1),
        [high, low] => Some(u16::from_be_bytes([*high, *low])),
        _ => None,
    }
}

/// Request records by the time they expire, so expiry scans in order.
pub fn request_expiry(at: u64, id: u128) -> Vec<u8> {
    let mut key = system(b't', 24);
    key.extend_from_slice(&at.to_be_bytes());
    key.extend_from_slice(&id.to_be_bytes());
    key
}

pub fn request_expiry_range() -> KeyRange {
    KeyRange::prefix(system(b't', 0))
}

/// A key's record, by its id.
pub fn key_record(id: u64) -> Vec<u8> {
    let mut key = system(b'k', 8);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

pub fn key_records() -> KeyRange {
    KeyRange::prefix(system(b'k', 0))
}

/// A deleted operator key's tombstone, so its id never mints again.
pub fn key_retired(id: u64) -> Vec<u8> {
    let mut key = system(b'R', 8);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

pub fn retired_keys() -> KeyRange {
    KeyRange::prefix(system(b'R', 0))
}

/// A reader's lease, by its id.
pub fn lease(id: u64) -> Vec<u8> {
    let mut key = system(b'L', 8);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

pub fn leases() -> KeyRange {
    KeyRange::prefix(system(b'L', 0))
}

/// The free-queue entries freed at or before `version`, from `after` on.
pub fn released(after: Option<&[u8]>, version: Version) -> KeyRange {
    let mut end = key(FREE, 8);
    end.extend_from_slice(&(version + 1).to_be_bytes());
    KeyRange {
        start: free_start(after),
        end,
    }
}

/// The whole free queue past `after`.
pub fn free_after(after: Option<&[u8]>) -> KeyRange {
    KeyRange {
        start: free_start(after),
        end: free_range().end,
    }
}

fn free_start(after: Option<&[u8]>) -> Vec<u8> {
    start_after(after, key(FREE, 0))
}

/// The free-queue entries under `dir` freed at or before `version`, from
/// `after` on.
pub fn released_under(dir: Ino, after: Option<&[u8]>, version: Version) -> KeyRange {
    let mut end = free_under_prefix(dir);
    end.extend_from_slice(&(version + 1).to_be_bytes());
    KeyRange {
        start: start_after(after, free_under_prefix(dir)),
        end,
    }
}

/// The whole free queue under `dir` past `after`.
pub fn free_under_after(dir: Ino, after: Option<&[u8]>) -> KeyRange {
    let prefix = free_under_prefix(dir);
    KeyRange {
        end: prefix_end(&prefix),
        start: start_after(after, prefix),
    }
}

/// Where a scan resumes: just past `after`, or at `first`.
fn start_after(after: Option<&[u8]>, first: Vec<u8>) -> Vec<u8> {
    match after {
        Some(key) => {
            let mut next = key.to_vec();
            next.push(0);
            next
        }
        None => first,
    }
}

/// The version from which every free-queue entry has its keys under the
/// directories above it: the store's version when a writer first kept
/// them. Entries freed at or before it may have none.
pub fn free_index() -> Vec<u8> {
    system(b'q', 0)
}

/// The secret that handles' MACs are made under.
pub fn handle_secret() -> Vec<u8> {
    system(b'm', 0)
}

/// The release record: the horizon claims may use, and the next lease id.
pub fn release() -> Vec<u8> {
    system(b'z', 0)
}

pub fn system_range(sub: u8) -> KeyRange {
    KeyRange::prefix(system(sub, 0))
}

pub fn system_key(sub: u8, rest: &[u8]) -> Vec<u8> {
    let mut key = system(sub, rest.len());
    key.extend_from_slice(rest);
    key
}

/// What a key names, for `fsck` and for tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed<'a> {
    Inode(Ino),
    Value(Ino, Version),
    Entry(Ino, &'a [u8]),
    Backptr(Ino, Ino, &'a [u8]),
    Free(Version, Ino, Option<Version>),
    /// A free-queue entry under a directory above what it frees.
    FreeUnder(Ino, Version, Ino, Option<Version>),
    Detached(Version, Ino),
    System(u8, &'a [u8]),
    Log,
}

fn be64(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.get(..8)?.try_into().ok()?))
}

pub fn parse(key: &[u8]) -> Option<Parsed<'_>> {
    let (&format, rest) = key.split_first()?;
    if format != FORMAT {
        return None;
    }
    let (&tag, rest) = rest.split_first()?;
    Some(match tag {
        INODE if rest.len() == 8 => Parsed::Inode(be64(rest)?),
        VALUE if rest.len() == 16 => Parsed::Value(be64(rest)?, be64(&rest[8..])?),
        ENTRY if rest.len() > 8 => Parsed::Entry(be64(rest)?, &rest[8..]),
        BACKPTR if rest.len() > 16 => Parsed::Backptr(be64(rest)?, be64(&rest[8..])?, &rest[16..]),
        FREE if rest.len() == 16 => Parsed::Free(be64(rest)?, be64(&rest[8..])?, None),
        FREE if rest.len() == 24 => {
            Parsed::Free(be64(rest)?, be64(&rest[8..])?, Some(be64(&rest[16..])?))
        }
        UNDER if rest.len() == 24 => {
            Parsed::FreeUnder(be64(rest)?, be64(&rest[8..])?, be64(&rest[16..])?, None)
        }
        UNDER if rest.len() == 32 => Parsed::FreeUnder(
            be64(rest)?,
            be64(&rest[8..])?,
            be64(&rest[16..])?,
            Some(be64(&rest[24..])?),
        ),
        DETACHED if rest.len() == 16 => Parsed::Detached(be64(rest)?, be64(&rest[8..])?),
        SYSTEM if !rest.is_empty() => Parsed::System(rest[0], &rest[1..]),
        LOG => Parsed::Log,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_parse_back() {
        assert_eq!(parse(&inode(7)), Some(Parsed::Inode(7)));
        assert_eq!(parse(&value(7, 9)), Some(Parsed::Value(7, 9)));
        assert_eq!(parse(&entry(3, b"a")), Some(Parsed::Entry(3, b"a")));
        assert_eq!(
            parse(&backptr(5, 3, b"a")),
            Some(Parsed::Backptr(5, 3, b"a"))
        );
        assert_eq!(parse(&free(4, 5, None)), Some(Parsed::Free(4, 5, None)));
        assert_eq!(
            parse(&free(4, 5, Some(2))),
            Some(Parsed::Free(4, 5, Some(2)))
        );
        assert_eq!(parse(&detached(4, 5)), Some(Parsed::Detached(4, 5)));
        assert_eq!(
            parse(&free_under(3, 4, 5, None)),
            Some(Parsed::FreeUnder(3, 4, 5, None))
        );
        assert_eq!(
            parse(&free_under(3, 4, 5, Some(2))),
            Some(Parsed::FreeUnder(3, 4, 5, Some(2)))
        );
    }

    #[test]
    fn a_directorys_part_of_the_queue_scans_in_the_queues_order() {
        let all = free_under_after(3, None);
        assert!(all.contains(&free_under(3, 1, 9, None)));
        assert!(all.contains(&free_under(3, u64::MAX, u64::MAX, Some(u64::MAX))));
        assert!(!all.contains(&free_under(2, 9, 9, None)));
        assert!(!all.contains(&free_under(4, 1, 1, None)));
        // By version, then inode, as the queue itself runs.
        assert!(free_under(3, 4, 9, None) < free_under(3, 5, 1, None));
        assert!(free_under(3, 4, 5, None) < free_under(3, 4, 6, None));
        // Released entries stop at the horizon.
        let released = released_under(3, None, 4);
        assert!(released.contains(&free_under(3, 4, 9, Some(2))));
        assert!(!released.contains(&free_under(3, 5, 1, None)));
        assert!(!released.contains(&free_under(4, 1, 1, None)));
        // A scan resumes just past the last key it read.
        let last = free_under(3, 4, 5, None);
        let rest = released_under(3, Some(&last), 9);
        assert!(!rest.contains(&last));
        assert!(rest.contains(&free_under(3, 4, 5, Some(1))));
        assert!(rest.contains(&free_under(3, 4, 6, None)));
        let rest = free_under_after(3, Some(&last));
        assert!(!rest.contains(&last));
        assert!(rest.contains(&free_under(3, 4, 6, None)));
        assert!(!rest.contains(&free_under(4, 1, 1, None)));
    }

    #[test]
    fn entries_scan_in_name_order_within_their_directory() {
        let range = KeyRange::prefix(entry_prefix(3));
        assert!(range.contains(&entry(3, b"a")));
        assert!(range.contains(&entry(3, &[0xff; 4])));
        assert!(!range.contains(&entry(4, b"a")));
        assert!(!range.contains(&entry(2, b"z")));
        let after = entries_after(3, Some(b"b"));
        assert!(!after.contains(&entry(3, b"b")));
        assert!(after.contains(&entry(3, b"b\0")));
        assert!(after.contains(&entry(3, b"c")));
    }

    #[test]
    fn writes_are_charged_as_foundationdb_charges_them() {
        let charged = |writes: Vec<KvWrite>, log: Vec<(Version, Vec<u8>)>| {
            let set = WriteSet { writes, log };
            (set.bytes(), set.charged())
        };
        // A key three times over with its store prefix, and 72 bytes.
        let small = free_under(3, 4, 5, None);
        assert_eq!(small.len(), 26);
        assert_eq!(
            charged(vec![KvWrite::Set(small.clone(), Vec::new())], Vec::new()),
            (26, 3 * (26 + 16) + 72)
        );
        // Four times over when cleared.
        assert_eq!(
            charged(vec![KvWrite::Clear(small)], Vec::new()),
            (26, 4 * (26 + 16) + 72)
        );
        // A value once.
        assert_eq!(
            charged(vec![KvWrite::Set(inode(7), vec![0; 100])], Vec::new()),
            (110, 3 * (10 + 16) + 100 + 72)
        );
        // A log entry by the chunks that hold it.
        assert_eq!(
            charged(Vec::new(), vec![(9, vec![0; 50])]),
            (68, 3 * (11 + 16) + 72 + 50)
        );
        assert_eq!(
            charged(Vec::new(), vec![(9, vec![0; 250_000])]).1,
            3 * (3 * (11 + 16) + 72) + 250_000
        );
    }

    #[test]
    fn prefix_end_carries() {
        assert_eq!(prefix_end(&[1, 2]), vec![1, 3]);
        assert_eq!(prefix_end(&[1, 0xff]), vec![2]);
        assert_eq!(prefix_end(&[0xff]), vec![0xff, 0xff]);
    }
}
