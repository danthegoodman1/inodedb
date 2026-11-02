//! Reading the tree into a cache at startup, a page of keys at a time: every
//! directory's entries, then every inode record, then every file's
//! back-pointers, then every value, so a tree larger than the cache resolves
//! paths from it. A directory whose entries all fall in one page arrives
//! complete, so the cache answers that a name is absent without a read,
//! and so does an empty directory the entries pass over once an entry has
//! named it; a larger one arrives as known names. Each page is one read
//! like any other, moved to the cache's version through the commits since.

use crate::codec::DecodeError;
use crate::keys::{self, KeyRange, KvRead, Pairs, Parsed};
use crate::types::{Entry, Ino, Inode, Kind, Name};
use crate::view::Fact;
use bytes::Bytes;
use std::collections::BTreeSet;

/// Keys one page reads: a directory of up to this many entries can arrive
/// complete. A page of entries takes a few megabytes.
pub const PAGE: usize = 16_384;
/// Values one page reads, at most 64 MiB of them.
pub const VALUE_PAGE: usize = 1_024;

/// Where the next page starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cursor {
    Entries(Vec<u8>),
    Inodes(Vec<u8>),
    Backptrs(Vec<u8>),
    Values(Vec<u8>),
}

/// What one page read: its facts, the directories its entries name, and
/// where the next page starts, or None once every key is read.
pub struct Page {
    pub facts: Vec<Fact>,
    pub dirs: Vec<Ino>,
    pub next: Option<Cursor>,
}

impl Cursor {
    pub fn start() -> Cursor {
        Cursor::Entries(keys::entry_range().start)
    }

    /// The read for this page.
    pub fn read(&self) -> KvRead {
        let (start, range, limit) = match self {
            Cursor::Entries(start) => (start, keys::entry_range(), PAGE),
            Cursor::Inodes(start) => (start, keys::inode_range(), PAGE),
            Cursor::Backptrs(start) => (start, keys::backptr_range(), PAGE),
            Cursor::Values(start) => (start, keys::value_range(), VALUE_PAGE),
        };
        KvRead::Scan {
            range: KeyRange {
                start: start.clone(),
                end: range.end,
            },
            limit,
        }
    }

    /// What a page holds. `dirs` are the directories the preload knows of,
    /// so a page of entries knows which directories it passed over empty.
    pub fn page(&self, pairs: Pairs, dirs: &BTreeSet<Ino>) -> Result<Page, DecodeError> {
        let full = pairs.len() >= PAGE;
        match self {
            Cursor::Values(_) => values(pairs),
            Cursor::Inodes(_) => {
                let next = match pairs.last() {
                    Some((last, _)) if full => Cursor::Inodes(after(last)),
                    _ => Cursor::Backptrs(keys::backptr_range().start),
                };
                let facts = pairs
                    .iter()
                    .map(|(key, value)| match keys::parse(key) {
                        Some(Parsed::Inode(ino)) => {
                            Ok(Fact::Inode(ino, Some(Inode::from_bytes(value)?)))
                        }
                        _ => Err(DecodeError),
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Page {
                    facts,
                    dirs: Vec::new(),
                    next: Some(next),
                })
            }
            Cursor::Entries(start) => entries(start, pairs, full, dirs),
            Cursor::Backptrs(start) => backptrs(start, pairs, full),
        }
    }
}

/// The least key past `key`.
fn after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// Consecutive pairs grouped by the inode their keys begin with.
fn groups<T>(
    pairs: &Pairs,
    parse: impl Fn(&[u8], &[u8]) -> Result<(Ino, T), DecodeError>,
) -> Result<Vec<(Ino, Vec<T>)>, DecodeError> {
    let mut groups: Vec<(Ino, Vec<T>)> = Vec::new();
    for (key, value) in pairs {
        let (ino, item) = parse(key, value)?;
        match groups.last_mut() {
            Some((last, items)) if *last == ino => items.push(item),
            _ => groups.push((ino, vec![item])),
        }
    }
    Ok(groups)
}

/// A page of entries. Each directory it holds whole arrives complete. When
/// the page fills, its last directory may go on past it: the next page
/// starts at that directory, unless the directory fills the page alone,
/// when it arrives as known names and the next page goes on after them.
fn entries(
    start: &[u8],
    pairs: Pairs,
    full: bool,
    dirs: &BTreeSet<Ino>,
) -> Result<Page, DecodeError> {
    let mut groups = groups(&pairs, |key, value| match keys::parse(key) {
        Some(Parsed::Entry(dir, name)) => {
            Ok((dir, (Name::from(name), Some(Entry::from_bytes(value)?))))
        }
        _ => Err(DecodeError),
    })?;
    let found: BTreeSet<Ino> = groups
        .iter()
        .flat_map(|(_, entries)| entries.iter().flat_map(|(_, entry)| entry))
        .filter(|entry| entry.kind == Kind::Dir)
        .map(|entry| entry.child)
        .collect();
    let (next, last_ends) = match pairs.last() {
        Some((last, _)) if full && groups.len() == 1 => (Cursor::Entries(after(last)), false),
        _ if full => {
            let (dir, _) = groups.pop().ok_or(DecodeError)?;
            (Cursor::Entries(keys::entry_prefix(dir)), true)
        }
        _ => (Cursor::Inodes(keys::inode_range().start), true),
    };
    // A directory this page passed over whole, with no entry in it, is
    // empty as of this page's read.
    let covered = match &next {
        Cursor::Entries(end) => end.clone(),
        _ => keys::entry_range().end,
    };
    let listed: BTreeSet<Ino> = groups.iter().map(|(dir, _)| *dir).collect();
    let dir_at = |key: &[u8], beyond: Ino| match keys::parse(key) {
        Some(Parsed::Entry(dir, _)) => dir,
        _ => beyond,
    };
    let span = dir_at(start, 0)..=dir_at(&covered, Ino::MAX);
    let known: BTreeSet<Ino> = dirs
        .range(span.clone())
        .chain(found.range(span))
        .copied()
        .collect();
    let empty: Vec<Fact> = known
        .iter()
        .filter(|dir| !listed.contains(dir))
        .filter(|dir| {
            let prefix = keys::entry_prefix(**dir);
            start <= prefix.as_slice() && keys::prefix_end(&prefix) <= covered
        })
        .map(|dir| Fact::Listing {
            dir: *dir,
            entries: Vec::new(),
            complete: true,
        })
        .collect();
    let count = groups.len();
    let mut facts: Vec<Fact> = groups
        .into_iter()
        .enumerate()
        .map(|(index, (dir, entries))| {
            // The page's first directory may have begun before it.
            let begun = index > 0 || start <= keys::entry_prefix(dir).as_slice();
            let ends = index + 1 < count || last_ends;
            Fact::Listing {
                dir,
                entries,
                complete: begun && ends,
            }
        })
        .collect();
    facts.extend(empty);
    Ok(Page {
        facts,
        dirs: found.into_iter().collect(),
        next: Some(next),
    })
}

/// A page of back-pointers. Each file's arrive together: when the page
/// fills, the next starts at its last file, unless that file fills the page
/// alone, when the next goes on past it and the file's links wait for a
/// read that needs them.
fn backptrs(start: &[u8], pairs: Pairs, full: bool) -> Result<Page, DecodeError> {
    let mut groups = groups(&pairs, |key, _| match keys::parse(key) {
        Some(Parsed::Backptr(ino, parent, name)) => Ok((ino, (parent, Name::from(name)))),
        _ => Err(DecodeError),
    })?;
    let next = match groups.len() {
        _ if !full => Some(Cursor::Values(keys::value_range().start)),
        1 => {
            let (ino, _) = groups.pop().ok_or(DecodeError)?;
            Some(Cursor::Backptrs(keys::prefix_end(&keys::backptr_prefix(
                ino,
            ))))
        }
        _ => {
            let (ino, _) = groups.pop().ok_or(DecodeError)?;
            Some(Cursor::Backptrs(keys::backptr_prefix(ino)))
        }
    };
    let facts = groups
        .into_iter()
        .enumerate()
        .filter(|(index, (ino, _))| *index > 0 || start <= keys::backptr_prefix(*ino).as_slice())
        .map(|(_, (ino, links))| Fact::Backptrs(ino, links.into_iter().collect::<BTreeSet<_>>()))
        .collect();
    Ok(Page {
        facts,
        dirs: Vec::new(),
        next,
    })
}

/// A page of values. An old value a file's readers may still use comes along
/// with its file's current one, each under its own version.
fn values(pairs: Pairs) -> Result<Page, DecodeError> {
    let next = match pairs.last() {
        Some((last, _)) if pairs.len() >= VALUE_PAGE => Some(Cursor::Values(after(last))),
        _ => None,
    };
    let facts = pairs
        .into_iter()
        .map(|(key, value)| match keys::parse(&key) {
            Some(Parsed::Value(ino, version)) => {
                Ok(Fact::Value(ino, version, Some(Bytes::from(value))))
            }
            _ => Err(DecodeError),
        })
        .collect::<Result<_, _>>()?;
    Ok(Page {
        facts,
        dirs: Vec::new(),
        next,
    })
}
