//! Snapshots of a subscriber's cache. A snapshot pins a version while the
//! cache moves on, and every read through it sees that version.
//!
//! While a snapshot older than a change is open, the subscriber keeps the
//! state each of the change's effects replaced as an undo record under the
//! change's version, whether or not the cache holds the key. A read
//! through a snapshot at V takes a key's earliest undo record newer than
//! V, which holds the state at V; without one, the key has not changed
//! since V, and the current state answers.
//!
//! What leaves the feed's sight, by moving out of the subtree or being
//! removed, may change in storage without the feed showing it. As the
//! cache forgets it, the subscriber keeps what the cache held, as of the
//! change it left in. A snapshot older than that change reads beneath it
//! from undo records up to that change and the kept states alone, and
//! what they lack is lost to the snapshot.

use crate::auth::{KeyId, KeyRecord};
use crate::cache::Cache;
use crate::ops::CommitResult;
use crate::types::{Entry, Ino, Inode, Name, Version};
use crate::view::{Lookup, Read, View, layered_listing};
use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque, btree_map};
use std::ops::Bound;

/// What an undo record or kept name costs beyond its name's bytes, as
/// `tests/examples/footprint.rs` measures it.
const RECORD_BYTES: usize = 160;

/// What the cache held of an inode as it left the feed's sight.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Kept {
    /// Its record, when cached.
    pub inode: Option<Option<Inode>>,
    /// A directory's cached names, and whether they were all of them.
    pub entries: Option<(BTreeMap<Name, Option<Entry>>, bool)>,
}

impl Kept {
    fn bytes(&self) -> usize {
        RECORD_BYTES
            + self.entries.as_ref().map_or(0, |(entries, _)| {
                entries.keys().map(|name| RECORD_BYTES + name.len()).sum()
            })
    }

    /// Takes what it lacks from `other`, returning what it took.
    fn merge(&mut self, other: Kept) -> Kept {
        let mut took = Kept::default();
        if self.inode.is_none() {
            self.inode = other.inode;
            took.inode = other.inode;
        }
        if self.entries.is_none() {
            took.entries.clone_from(&other.entries);
            self.entries = other.entries;
        }
        took
    }

    /// The children its names reach.
    fn children(&self) -> impl Iterator<Item = Ino> + '_ {
        self.entries
            .iter()
            .flat_map(|(entries, _)| entries.values().flatten().map(|entry| entry.child))
    }
}

/// A record's place, for dropping records by version.
#[derive(Debug)]
enum Slot {
    Inode(Ino),
    Entry(Ino, Name),
    Left(Ino),
}

/// Undo records and kept states, for the open snapshots.
#[derive(Debug, Default)]
pub struct Undo {
    inodes: BTreeMap<Ino, BTreeMap<Version, Option<Inode>>>,
    entries: BTreeMap<Ino, BTreeMap<Name, BTreeMap<Version, Option<Entry>>>>,
    /// What left the feed's sight, under the change it left in.
    left: BTreeMap<Ino, BTreeMap<Version, Kept>>,
    /// Each record's place, oldest first, as changes arrive in version
    /// order.
    slots: VecDeque<(Version, Slot)>,
    /// How many entry records and kept names name each child.
    children: BTreeMap<Ino, usize>,
    bytes: usize,
    /// Open snapshots' versions, with how many hold each.
    open: BTreeMap<Version, usize>,
    /// Snapshots the budget ended that their holders have yet to close.
    /// They read nothing, but the lease still holds their versions, since
    /// their holders may still use what they read.
    ended: BTreeMap<Version, usize>,
}

impl Undo {
    /// Whether a snapshot is open, so changes need undo records.
    pub fn recording(&self) -> bool {
        !self.open.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn open(&mut self, at: Version) {
        *self.open.entry(at).or_default() += 1;
    }

    pub fn close(&mut self, at: Version) {
        let closed = |counts: &mut BTreeMap<Version, usize>| match counts.entry(at) {
            btree_map::Entry::Occupied(mut count) => {
                *count.get_mut() -= 1;
                if *count.get() == 0 {
                    count.remove();
                }
                true
            }
            btree_map::Entry::Vacant(_) => false,
        };
        if closed(&mut self.ended) {
            return;
        }
        if closed(&mut self.open) {
            self.trim();
        }
    }

    /// The oldest open snapshot's version.
    pub fn oldest(&self) -> Option<Version> {
        self.open.keys().next().copied()
    }

    /// The oldest version a snapshot not yet closed holds, ended or not,
    /// which its reader's lease must hold.
    pub fn pinned(&self) -> Option<Version> {
        let ended = self.ended.keys().next().copied();
        match (self.oldest(), ended) {
            (Some(open), Some(ended)) => Some(open.min(ended)),
            (open, ended) => open.or(ended),
        }
    }

    /// Whether a snapshot at `at` still reads.
    pub fn holds(&self, at: Version) -> bool {
        self.open.contains_key(&at)
    }

    /// Records the inode state a change at `version` replaced.
    pub fn inode(&mut self, ino: Ino, version: Version, before: Option<Inode>) {
        if let btree_map::Entry::Vacant(slot) = self.inodes.entry(ino).or_default().entry(version) {
            slot.insert(before);
            self.bytes += RECORD_BYTES;
            self.slots.push_back((version, Slot::Inode(ino)));
        }
    }

    /// Records the entry a change at `version` replaced.
    pub fn entry(&mut self, dir: Ino, name: &Name, version: Version, before: Option<Entry>) {
        let records = self
            .entries
            .entry(dir)
            .or_default()
            .entry(name.clone())
            .or_default();
        if let btree_map::Entry::Vacant(slot) = records.entry(version) {
            slot.insert(before);
            if let Some(entry) = before {
                *self.children.entry(entry.child).or_default() += 1;
            }
            self.bytes += RECORD_BYTES + name.len();
            self.slots
                .push_back((version, Slot::Entry(dir, name.clone())));
        }
    }

    /// Records that `ino` left the feed's sight in the change at
    /// `version`, with what the cache held of it.
    pub fn leave(&mut self, ino: Ino, version: Version, kept: Kept) {
        let stored = match self.left.entry(ino).or_default().entry(version) {
            btree_map::Entry::Vacant(slot) => {
                self.bytes += kept.bytes();
                self.slots.push_back((version, Slot::Left(ino)));
                slot.insert(kept).clone()
            }
            btree_map::Entry::Occupied(mut slot) => {
                self.bytes -= slot.get().bytes();
                let took = slot.get_mut().merge(kept);
                self.bytes += slot.get().bytes();
                took
            }
        };
        for child in stored.children() {
            *self.children.entry(child).or_default() += 1;
        }
    }

    /// Whether a record names `ino`, so a snapshot may hand out its
    /// handle.
    pub fn mentions(&self, ino: Ino) -> bool {
        self.inodes.contains_key(&ino)
            || self.entries.contains_key(&ino)
            || self.left.contains_key(&ino)
            || self.children.contains_key(&ino)
    }

    /// How many inodes the records name, about.
    pub fn mentioned(&self) -> usize {
        self.inodes.len() + self.entries.len() + self.left.len() + self.children.len()
    }

    fn unmention(&mut self, child: Ino) {
        if let btree_map::Entry::Occupied(mut count) = self.children.entry(child) {
            *count.get_mut() -= 1;
            if *count.get() == 0 {
                count.remove();
            }
        }
    }

    /// Ends the oldest snapshots until the records fit `budget`.
    pub fn fit(&mut self, budget: usize) {
        while self.bytes > budget
            && let Some((oldest, count)) = self.open.pop_first()
        {
            *self.ended.entry(oldest).or_default() += count;
            self.trim();
        }
    }

    /// Drops the records no open snapshot reads: those at or below the
    /// oldest one's version.
    fn trim(&mut self) {
        let keep = self.oldest().map_or(Version::MAX, |oldest| oldest + 1);
        while self
            .slots
            .front()
            .is_some_and(|(version, _)| *version < keep)
        {
            let (version, slot) = self.slots.pop_front().expect("checked");
            match slot {
                Slot::Inode(ino) => {
                    if let btree_map::Entry::Occupied(mut records) = self.inodes.entry(ino) {
                        records.get_mut().remove(&version);
                        self.bytes -= RECORD_BYTES;
                        if records.get().is_empty() {
                            records.remove();
                        }
                    }
                }
                Slot::Entry(dir, name) => {
                    let mut gone = None;
                    if let btree_map::Entry::Occupied(mut names) = self.entries.entry(dir) {
                        if let Some(records) = names.get_mut().get_mut(&name) {
                            gone = records.remove(&version).flatten();
                            self.bytes -= RECORD_BYTES + name.len();
                            if records.is_empty() {
                                names.get_mut().remove(&name);
                            }
                        }
                        if names.get().is_empty() {
                            names.remove();
                        }
                    }
                    if let Some(entry) = gone {
                        self.unmention(entry.child);
                    }
                }
                Slot::Left(ino) => {
                    let mut gone = None;
                    if let btree_map::Entry::Occupied(mut left) = self.left.entry(ino) {
                        gone = left.get_mut().remove(&version);
                        if left.get().is_empty() {
                            left.remove();
                        }
                    }
                    if let Some(kept) = gone {
                        self.bytes -= kept.bytes();
                        for child in kept.children().collect::<Vec<_>>() {
                            self.unmention(child);
                        }
                    }
                }
            }
        }
    }

    fn inode_at(&self, ino: Ino, at: Version, cut: Option<Version>) -> Option<Option<Inode>> {
        let records = self.inodes.get(&ino)?;
        records
            .range(at + 1..=cut.unwrap_or(Version::MAX))
            .next()
            .map(|(_, before)| *before)
    }

    fn entry_at(
        &self,
        dir: Ino,
        name: &[u8],
        at: Version,
        cut: Option<Version>,
    ) -> Option<Option<Entry>> {
        let records = self.entries.get(&dir)?.get(name)?;
        records
            .range(at + 1..=cut.unwrap_or(Version::MAX))
            .next()
            .map(|(_, before)| *before)
    }

    /// The names of `dir` changed after `at`, from `start`, each with its
    /// state at `at`.
    fn entries_at(
        &self,
        dir: Ino,
        at: Version,
        cut: Option<Version>,
        start: Bound<&[u8]>,
    ) -> Vec<(Name, Option<Entry>)> {
        let Some(names) = self.entries.get(&dir) else {
            return Vec::new();
        };
        let upto = cut.unwrap_or(Version::MAX);
        names
            .range::<[u8], _>((start, Bound::Unbounded))
            .filter_map(|(name, records)| {
                records
                    .range(at + 1..=upto)
                    .next()
                    .map(|(_, before)| (name.clone(), *before))
            })
            .collect()
    }

    /// The first change after `at` that `ino` left the feed's sight in.
    fn left_after(&self, ino: Ino, at: Version) -> Option<Version> {
        self.left
            .get(&ino)?
            .range(at + 1..)
            .next()
            .map(|(version, _)| *version)
    }

    fn kept(&self, ino: Ino, version: Version) -> Option<&Kept> {
        self.left.get(&ino)?.get(&version)
    }
}

/// Why a read through a snapshot has no answer yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Miss {
    /// Reads of the current state that would answer it, once the feed
    /// reaches the versions they reflect.
    Reads(Vec<Read>),
    /// The snapshot ended, or what the read needs left the feed's sight
    /// without the cache holding it.
    TooOld,
}

/// What a read through a snapshot fetched, over the subscriber's cache.
pub(crate) struct Current<'a> {
    pub fetched: &'a Cache,
    pub cache: &'a Cache,
}

impl View for Current<'_> {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>> {
        self.fetched.inode(ino).or_else(|_| self.cache.inode(ino))
    }

    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        self.fetched
            .entry(dir, name)
            .or_else(|_| self.cache.entry(dir, name))
    }

    fn has_entry_outside(&self, dir: Ino, removed: &BTreeSet<Name>) -> Lookup<bool> {
        self.fetched
            .has_entry_outside(dir, removed)
            .or_else(|_| self.cache.has_entry_outside(dir, removed))
    }

    fn backptrs(&self, ino: Ino) -> Lookup<BTreeSet<(Ino, Name)>> {
        self.fetched
            .backptrs(ino)
            .or_else(|_| self.cache.backptrs(ino))
    }

    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        self.fetched
            .value(ino, version)
            .or_else(|_| self.cache.value(ino, version))
    }

    fn request(&self, id: u128) -> Lookup<Option<CommitResult>> {
        self.cache.request(id)
    }

    fn listing(&self, dir: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>> {
        self.fetched
            .listing(dir, start, limit)
            .or_else(|_| self.cache.listing(dir, start, limit))
    }

    fn free_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.cache.free_entry(version, ino)
    }

    fn detach_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.fetched
            .detach_entry(version, ino)
            .or_else(|_| self.cache.detach_entry(version, ino))
    }

    fn key(&self, id: KeyId) -> Option<KeyRecord> {
        self.cache.key(id)
    }

    fn key_retired(&self, id: KeyId) -> bool {
        self.cache.key_retired(id)
    }

    fn keys_rooted_at(&self, dir: Ino) -> Vec<KeyId> {
        self.cache.keys_rooted_at(dir)
    }

    fn keys_minted_by(&self, id: KeyId) -> Vec<KeyId> {
        self.cache.keys_minted_by(id)
    }

    fn key_roots(&self) -> Vec<(KeyId, Ino)> {
        self.cache.key_roots()
    }
}

/// The tree at a snapshot's version: undo records over the current state.
/// Lookups go down from the root, and each inode reached carries the
/// first change after the snapshot that it, or a directory above it on
/// the way down, left the feed's sight in: its cut. Beneath a cut, only
/// undo records up to it and the states kept at it answer.
pub(crate) struct SnapshotView<'a> {
    pub undo: &'a Undo,
    pub current: &'a dyn View,
    pub at: Version,
    cuts: RefCell<BTreeMap<Ino, Version>>,
    lost: Cell<bool>,
}

fn earliest(a: Option<Version>, b: Option<Version>) -> Option<Version> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

impl<'a> SnapshotView<'a> {
    pub fn new(undo: &'a Undo, current: &'a dyn View, at: Version) -> SnapshotView<'a> {
        SnapshotView {
            undo,
            current,
            at,
            cuts: RefCell::new(BTreeMap::new()),
            lost: Cell::new(false),
        }
    }

    /// Whether a lookup found what it needs lost to the snapshot.
    pub fn lost(&self) -> bool {
        self.lost.get()
    }

    fn cut(&self, ino: Ino) -> Option<Version> {
        earliest(
            self.cuts.borrow().get(&ino).copied(),
            self.undo.left_after(ino, self.at),
        )
    }

    /// Notes a child reached beneath a cut.
    fn reach(&self, child: Ino, cut: Option<Version>) {
        if let Some(cut) = cut {
            let mut cuts = self.cuts.borrow_mut();
            let slot = cuts.entry(child).or_insert(cut);
            *slot = (*slot).min(cut);
        }
    }

    fn lose<T>(&self, read: Read) -> Lookup<T> {
        self.lost.set(true);
        Err(read)
    }

    /// A cut directory's names as kept, and whether they were all of them.
    fn kept_entries(
        &self,
        dir: Ino,
        cut: Version,
    ) -> Option<&(BTreeMap<Name, Option<Entry>>, bool)> {
        self.undo.kept(dir, cut)?.entries.as_ref()
    }
}

impl View for SnapshotView<'_> {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>> {
        let cut = self.cut(ino);
        if let Some(before) = self.undo.inode_at(ino, self.at, cut) {
            return Ok(before);
        }
        match cut {
            Some(cut) => match self.undo.kept(ino, cut).and_then(|kept| kept.inode) {
                Some(inode) => Ok(inode),
                None => self.lose(Read::Inode(ino)),
            },
            None => self.current.inode(ino),
        }
    }

    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        let cut = self.cut(dir);
        let found = match self.undo.entry_at(dir, name, self.at, cut) {
            Some(before) => before,
            None => match cut {
                Some(cut) => match self.kept_entries(dir, cut) {
                    Some((names, complete)) => match names.get(name) {
                        Some(entry) => *entry,
                        None if *complete => None,
                        None => return self.lose(Read::Entry(dir, Name::from(name))),
                    },
                    None => return self.lose(Read::Entry(dir, Name::from(name))),
                },
                None => self.current.entry(dir, name)?,
            },
        };
        if let Some(entry) = found {
            self.reach(entry.child, cut);
        }
        Ok(found)
    }

    fn has_entry_outside(&self, dir: Ino, removed: &BTreeSet<Name>) -> Lookup<bool> {
        let page = self.listing(dir, Bound::Unbounded, removed.len() + 1)?;
        Ok(page.iter().any(|(name, _)| !removed.contains(name)))
    }

    fn backptrs(&self, ino: Ino) -> Lookup<BTreeSet<(Ino, Name)>> {
        self.lose(Read::Backptrs(ino))
    }

    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        // A value never changes under its version.
        self.current.value(ino, version)
    }

    fn request(&self, id: u128) -> Lookup<Option<CommitResult>> {
        self.current.request(id)
    }

    fn listing(&self, dir: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>> {
        let cut = self.cut(dir);
        let changes = self.undo.entries_at(dir, self.at, cut, start);
        let page = match cut {
            Some(cut) => {
                let Some((names, true)) = self.kept_entries(dir, cut) else {
                    return self.lose(Read::Entries { dir, limit: None });
                };
                let mut merged: BTreeMap<Name, Entry> = names
                    .range::<[u8], _>((start, Bound::Unbounded))
                    .filter_map(|(name, entry)| entry.map(|entry| (name.clone(), entry)))
                    .collect();
                for (name, before) in changes {
                    match before {
                        Some(entry) => merged.insert(name, entry),
                        None => merged.remove(&name),
                    };
                }
                merged.into_iter().take(limit).collect()
            }
            None => layered_listing(
                self.current,
                dir,
                false,
                changes.iter().map(|(name, before)| (name, *before)),
                start,
                limit,
            )?,
        };
        for (_, entry) in &page {
            self.reach(entry.child, cut);
        }
        Ok(page)
    }

    fn free_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.lose(Read::Free(version, ino))
    }

    fn detach_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.lose(Read::Detached(version, ino))
    }

    fn key(&self, id: KeyId) -> Option<KeyRecord> {
        self.current.key(id)
    }

    fn key_retired(&self, id: KeyId) -> bool {
        self.current.key_retired(id)
    }

    fn keys_rooted_at(&self, dir: Ino) -> Vec<KeyId> {
        self.current.keys_rooted_at(dir)
    }

    fn keys_minted_by(&self, id: KeyId) -> Vec<KeyId> {
        self.current.keys_minted_by(id)
    }

    fn key_roots(&self) -> Vec<(KeyId, Ino)> {
        self.current.key_roots()
    }
}
