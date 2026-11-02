//! What a holder knows of the tree at its current version.

use crate::auth::{KeyId, KeyRecord, Keys};
use crate::delta::{Delta, Freed, Links};
use crate::ops::CommitResult;
use crate::snapshot::Kept;
use crate::types::{Entry, Ino, Inode, Kind, Name, Version};
use crate::view::{Fact, Lookup, Read, View};
use bytes::Bytes;
use std::collections::btree_map;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};

/// How much a cache holds before it evicts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Bytes of directories' entries, inode records and files' links, as
    /// the cache charges them.
    pub tree_bytes: usize,
    /// Bytes of values, as the cache charges them.
    pub value_bytes: usize,
    /// Bytes of a subscriber's undo records and kept states, for its
    /// snapshots.
    pub undo_bytes: usize,
}

impl Default for Budget {
    fn default() -> Budget {
        // About a million cached files beside 256 MiB of values
        // (`BENCHMARKS.md`).
        Budget {
            tree_bytes: 1 << 30,
            value_bytes: 256 << 20,
            undo_bytes: 64 << 20,
        }
    }
}

/// What the cache charges for each record: at least what the allocator
/// holds for it, eviction's bookkeeping included, so a cache within its
/// budget is within that much memory (`BENCHMARKS.md`). A directory's
/// entries cost `DIR_BYTES` and each name `NAME_BYTES` beside its own
/// bytes; an inode's record costs `INODE_BYTES`; a file's links cost
/// `LINKS_BYTES` and each link as a name. A value costs `VALUE_BYTES`
/// beside its bytes, against `value_bytes`.
pub const DIR_BYTES: usize = 570;
pub const NAME_BYTES: usize = 80;
pub const INODE_BYTES: usize = 250;
pub const LINKS_BYTES: usize = 360;
pub const VALUE_BYTES: usize = 200;

fn name_bytes(name: &[u8]) -> usize {
    NAME_BYTES + name.len()
}

fn links_bytes(links: &Links) -> usize {
    LINKS_BYTES
        + links
            .iter()
            .map(|(_, name)| name_bytes(name))
            .sum::<usize>()
}

fn value_bytes(value: &Option<Bytes>) -> usize {
    VALUE_BYTES + value.as_ref().map_or(0, Bytes::len)
}

/// The order the cache last wrote what it holds. Each write takes the
/// next place; a queue keeps places oldest first, and a place the key has
/// since left is stale. Eviction sweeps a hand from the oldest place to
/// the newest, then starts again from the oldest, passing over once each
/// key a read used since the hand last passed it (SIEVE).
#[derive(Debug)]
struct Queue<K> {
    places: VecDeque<(u64, K)>,
    latest: BTreeMap<K, u64>,
    /// Where the hand's sweep goes on from.
    hand: u64,
}

impl<K> Default for Queue<K> {
    fn default() -> Queue<K> {
        Queue {
            places: VecDeque::new(),
            latest: BTreeMap::new(),
            hand: 0,
        }
    }
}

impl<K: Ord + Copy> Queue<K> {
    fn touch(&mut self, place: u64, key: K) {
        self.latest.insert(key, place);
        self.places.push_back((place, key));
        self.compact();
    }

    /// Drops stale places once they outnumber the keys.
    fn compact(&mut self) {
        if self.places.len() > 2 * self.latest.len() + 1024 {
            let latest = &self.latest;
            self.places
                .retain(|(place, key)| latest.get(key) == Some(place));
        }
    }

    /// The next key to evict among those written before `spare`. `used`
    /// says whether a read used a key since the hand last passed it, and
    /// clears that.
    fn next(&mut self, spare: u64, used: impl Fn(&K) -> bool) -> Option<K> {
        let mut from = self.hand;
        loop {
            let start = self.places.partition_point(|(place, _)| *place < from);
            let mut passed = false;
            for &(place, key) in self.places.range(start..) {
                if place >= spare {
                    break;
                }
                if self.latest.get(&key) != Some(&place) {
                    continue;
                }
                if used(&key) {
                    passed = true;
                    continue;
                }
                self.hand = place + 1;
                self.latest.remove(&key);
                self.compact();
                return Some(key);
            }
            if from == 0 && !passed {
                return None;
            }
            from = 0;
        }
    }

    fn forget(&mut self, key: K) {
        self.latest.remove(&key);
    }
}

/// What the tree's queue orders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Record {
    /// A directory's entries.
    Entries(Ino),
    /// An inode's record and, for a file, its links.
    Inode(Ino),
}

#[derive(Default, Debug)]
struct Order {
    tree: Queue<Record>,
    values: Queue<(Ino, Version)>,
    written: u64,
}

/// A cached record, and whether a read used it since eviction's hand last
/// passed it. Reads may run on any thread that shares the cache, so the mark
/// is atomic; it is a hint, so relaxed ordering serves, and a read writes it
/// only when it changes, which keeps the record's cache line shared.
#[derive(Debug)]
struct Held<T> {
    record: T,
    used: AtomicBool,
}

impl<T> Held<T> {
    fn new(record: T) -> Held<T> {
        Held {
            record,
            used: AtomicBool::new(false),
        }
    }

    /// The record, for a read.
    fn read(&self) -> &T {
        if !self.used.load(Ordering::Relaxed) {
            self.used.store(true, Ordering::Relaxed);
        }
        &self.record
    }

    /// Whether a read used it since the last call.
    fn take_used(&self) -> bool {
        self.used.swap(false, Ordering::Relaxed)
    }
}

/// Records `record` under `key`, keeping whether a read used what it
/// replaces, and returns what it replaces.
fn put<K: Ord, T>(held: &mut BTreeMap<K, Held<T>>, key: K, record: T) -> Option<T> {
    match held.entry(key) {
        btree_map::Entry::Vacant(slot) => {
            slot.insert(Held::new(record));
            None
        }
        btree_map::Entry::Occupied(slot) => {
            Some(std::mem::replace(&mut slot.into_mut().record, record))
        }
    }
}

#[derive(Debug)]
struct Dir {
    /// Known names: present entries and, until the directory is complete,
    /// known-absent ones.
    entries: BTreeMap<Name, Option<Entry>>,
    /// Every present entry is in `entries`.
    complete: bool,
    /// What the cache charges for the directory.
    bytes: usize,
}

impl Dir {
    fn new() -> Dir {
        Dir {
            entries: BTreeMap::new(),
            complete: false,
            bytes: DIR_BYTES,
        }
    }

    /// Records what `name` holds. A complete directory keeps no record of
    /// an absent name.
    fn put(&mut self, name: Name, entry: Option<Entry>) {
        if self.complete && entry.is_none() {
            if self.entries.remove(&name).is_some() {
                self.bytes -= name_bytes(&name);
            }
            return;
        }
        let bytes = name_bytes(&name);
        if self.entries.insert(name, entry).is_none() {
            self.bytes += bytes;
        }
    }

    /// Makes the directory complete with no names, ready for its listing.
    fn restart(&mut self) {
        self.entries.clear();
        self.complete = true;
        self.bytes = DIR_BYTES;
    }
}

#[derive(Default, Debug)]
pub struct Cache {
    budget: Budget,
    order: Order,
    /// What the cache charges for its directories, inode records and links.
    tree_bytes: usize,
    value_bytes: usize,
    inodes: BTreeMap<Ino, Held<Option<Inode>>>,
    dirs: BTreeMap<Ino, Held<Dir>>,
    backptrs: BTreeMap<Ino, Held<Links>>,
    values: BTreeMap<(Ino, Version), Held<Option<Bytes>>>,
    requests: BTreeMap<u128, Option<CommitResult>>,
    /// Every request id with a live record, once loaded, so a new id needs
    /// no read.
    request_ids: Option<BTreeSet<u128>>,
    /// Every key, in the writer's cache; keys are never evicted.
    keys: Keys,
    /// Freed inodes' free-queue entries, by inode, with the version that
    /// freed each; they go with their inode's record.
    free: BTreeMap<Ino, (Version, Option<Vec<Ino>>)>,
    /// Detach records, by the subtree's top.
    detach: BTreeMap<Ino, (Version, Option<Vec<Ino>>)>,
}

impl Cache {
    pub fn new() -> Cache {
        Cache::default()
    }

    pub fn with_budget(budget: Budget) -> Cache {
        Cache {
            budget,
            ..Cache::default()
        }
    }

    /// Writes so far; `evict` spares what was written at or after a mark.
    pub fn mark(&self) -> u64 {
        self.order.written
    }

    fn place(&mut self) -> u64 {
        self.order.written += 1;
        self.order.written - 1
    }

    /// Changes a directory's entries, recording it if the cache lacks it.
    fn edit_dir(&mut self, dir: Ino, edit: impl FnOnce(&mut Dir)) {
        let place = self.place();
        self.order.tree.touch(place, Record::Entries(dir));
        let (cached, before) = match self.dirs.entry(dir) {
            btree_map::Entry::Vacant(slot) => (&mut slot.insert(Held::new(Dir::new())).record, 0),
            btree_map::Entry::Occupied(slot) => {
                let cached = &mut slot.into_mut().record;
                let before = cached.bytes;
                (cached, before)
            }
        };
        edit(cached);
        self.tree_bytes += cached.bytes;
        self.tree_bytes -= before;
    }

    fn put_inode(&mut self, ino: Ino, inode: Option<Inode>) {
        let place = self.place();
        self.order.tree.touch(place, Record::Inode(ino));
        if put(&mut self.inodes, ino, inode).is_none() {
            self.tree_bytes += INODE_BYTES;
        }
    }

    fn put_backptrs(&mut self, ino: Ino, links: Links) {
        let place = self.place();
        self.order.tree.touch(place, Record::Inode(ino));
        self.tree_bytes += links_bytes(&links);
        if let Some(old) = put(&mut self.backptrs, ino, links) {
            self.tree_bytes -= links_bytes(&old);
        }
    }

    fn put_value(&mut self, key: (Ino, Version), value: Option<Bytes>) {
        let place = self.place();
        self.order.values.touch(place, key);
        self.value_bytes += value_bytes(&value);
        if let Some(old) = put(&mut self.values, key, value) {
            self.value_bytes -= value_bytes(&old);
        }
    }

    fn remove_dir(&mut self, dir: Ino) -> Option<Dir> {
        self.order.tree.forget(Record::Entries(dir));
        let cached = self.dirs.remove(&dir)?.record;
        self.tree_bytes -= cached.bytes;
        Some(cached)
    }

    /// Drops an inode's record and links, returning the record.
    fn remove_inode(&mut self, ino: Ino) -> Option<Option<Inode>> {
        self.order.tree.forget(Record::Inode(ino));
        if let Some(links) = self.backptrs.remove(&ino) {
            self.tree_bytes -= links_bytes(&links.record);
        }
        let inode = self.inodes.remove(&ino)?;
        self.tree_bytes -= INODE_BYTES;
        Some(inode.record)
    }

    /// Evicts until it fits its budget, sparing what it wrote at or after
    /// `spare`, which reads in progress need. Eviction passes over, once,
    /// what a read used since eviction last passed it.
    pub fn evict(&mut self, spare: u64) {
        while self.tree_bytes > self.budget.tree_bytes {
            let Some(record) = self.order.tree.next(spare, |record| match record {
                Record::Entries(dir) => self.dirs.get(dir).is_some_and(Held::take_used),
                Record::Inode(ino) => {
                    let inode = self.inodes.get(ino).is_some_and(Held::take_used);
                    let links = self.backptrs.get(ino).is_some_and(Held::take_used);
                    inode || links
                }
            }) else {
                break;
            };
            match record {
                Record::Entries(dir) => {
                    self.remove_dir(dir);
                }
                Record::Inode(ino) => {
                    self.remove_inode(ino);
                    self.free.remove(&ino);
                    self.detach.remove(&ino);
                }
            }
        }
        while self.value_bytes > self.budget.value_bytes {
            let Some(key) = self.order.values.next(spare, |key| {
                self.values.get(key).is_some_and(Held::take_used)
            }) else {
                break;
            };
            if let Some(value) = self.values.remove(&key) {
                self.value_bytes -= value_bytes(&value.record);
            }
        }
    }

    /// Records the ids of every live request record.
    pub fn load_request_ids(&mut self, ids: impl IntoIterator<Item = u128>) {
        self.request_ids = Some(ids.into_iter().collect());
    }

    pub fn load_keys(&mut self, keys: Keys) {
        self.keys = keys;
    }

    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    /// Forgets the tree as a cache with no room would, sparing what it
    /// wrote at or after `spare`, and keeping the request ids and keys.
    /// With nothing spared it forgets request records too.
    pub fn forget_tree(&mut self, spare: u64) {
        let budget = std::mem::replace(
            &mut self.budget,
            Budget {
                tree_bytes: 0,
                value_bytes: 0,
                undo_bytes: 0,
            },
        );
        self.evict(spare);
        self.budget = budget;
        if spare == u64::MAX {
            self.requests.clear();
        }
    }

    /// Forgets a directory's entries, returning what it held of them.
    pub fn forget_entries(&mut self, dir: Ino) -> Kept {
        Kept {
            inode: None,
            entries: self
                .remove_dir(dir)
                .map(|cached| (cached.entries, cached.complete)),
        }
    }

    /// Forgets a directory and what the cache reaches beneath it through
    /// cached entries, returning what it held of each.
    pub fn forget_beneath(&mut self, dir: Ino) -> BTreeMap<Ino, Kept> {
        let mut forgot: BTreeMap<Ino, Kept> = BTreeMap::new();
        let mut stack = vec![dir];
        while let Some(dir) = stack.pop() {
            let mut kept = self.forget_entries(dir);
            kept.inode = self.forget_file(dir).inode;
            for entry in kept
                .entries
                .iter()
                .flat_map(|(entries, _)| entries.values().flatten())
            {
                match entry.kind {
                    Kind::Dir => stack.push(entry.child),
                    // A file linked twice beneath is forgotten once.
                    Kind::File => {
                        forgot
                            .entry(entry.child)
                            .or_insert_with(|| self.forget_file(entry.child));
                    }
                }
            }
            forgot.insert(dir, kept);
        }
        forgot
    }

    /// Keeps only what `root` reaches through cached directories' entries,
    /// as after a directory left a subscriber's subtree or entered it: a
    /// record behind a directory the cache lacks may describe a state the
    /// subscriber's feed never showed. Returns what it forgot.
    pub fn keep_reachable(&mut self, root: Ino) -> BTreeMap<Ino, Kept> {
        let mut reached = BTreeSet::from([root]);
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let Some(cached) = self.dirs.get(&dir) else {
                continue;
            };
            for entry in cached.record.entries.values().flatten() {
                if reached.insert(entry.child) && entry.kind == Kind::Dir {
                    stack.push(entry.child);
                }
            }
        }
        let mut forgot: BTreeMap<Ino, Kept> = BTreeMap::new();
        for dir in outside(&self.dirs, &reached) {
            if let Some(cached) = self.remove_dir(dir) {
                forgot.entry(dir).or_default().entries = Some((cached.entries, cached.complete));
            }
        }
        let inodes: BTreeSet<Ino> = outside(&self.inodes, &reached)
            .into_iter()
            .chain(outside(&self.backptrs, &reached))
            .collect();
        for ino in inodes {
            if let Some(inode) = self.remove_inode(ino) {
                forgot.entry(ino).or_default().inode = Some(inode);
            }
        }
        self.free.retain(|ino, _| reached.contains(ino));
        self.detach.retain(|ino, _| reached.contains(ino));
        let order = &mut self.order;
        let mut dropped = 0;
        self.values.retain(|key, value| {
            let keep = reached.contains(&key.0);
            if !keep {
                dropped += value_bytes(&value.record);
                order.values.forget(*key);
            }
            keep
        });
        self.value_bytes -= dropped;
        forgot
    }

    /// Forgets an inode's record and, for a file, its links, returning
    /// what it held of them.
    pub fn forget_file(&mut self, ino: Ino) -> Kept {
        Kept {
            inode: self.remove_inode(ino),
            entries: None,
        }
    }

    pub fn expire_request(&mut self, id: u128) {
        self.requests.remove(&id);
        if let Some(ids) = &mut self.request_ids {
            ids.remove(&id);
        }
    }

    /// Records what storage held, already rebased to the cache's version.
    pub fn fill(&mut self, fact: Fact) {
        match fact {
            Fact::Inode(ino, inode) => {
                self.put_inode(ino, inode);
            }
            Fact::Entry(dir, name, entry) => {
                self.edit_dir(dir, |cached| cached.put(name, entry));
            }
            Fact::Listing {
                dir,
                entries,
                complete,
            } => {
                self.edit_dir(dir, |cached| {
                    if complete {
                        cached.restart();
                    }
                    for (name, entry) in entries {
                        cached.put(name, entry);
                    }
                });
            }
            Fact::Backptrs(ino, links) => {
                self.put_backptrs(ino, links);
            }
            Fact::Value(ino, version, value) => {
                self.put_value((ino, version), value);
            }
            Fact::Request(id, result) => {
                if result.is_some()
                    && let Some(ids) = &mut self.request_ids
                {
                    ids.insert(id);
                }
                self.requests.insert(id, result);
            }
            Fact::Free(version, ino, above) => {
                self.free.insert(ino, (version, above));
            }
            Fact::Detached(version, ino, above) => {
                self.detach.insert(ino, (version, above));
            }
        }
    }

    /// Applies a committed delta's changes.
    pub fn apply(&mut self, delta: &Delta) {
        for ino in &delta.created {
            match delta.inodes.get(ino).and_then(|change| change.after) {
                Some(inode) if inode.kind == Kind::Dir => {
                    self.edit_dir(*ino, Dir::restart);
                }
                _ => {
                    self.put_backptrs(*ino, Links::new());
                }
            }
        }
        for (ino, change) in &delta.inodes {
            self.put_inode(*ino, change.after);
        }
        for (dir, names) in &delta.entries {
            self.edit_dir(*dir, |cached| {
                for (name, change) in names {
                    cached.put(name.clone(), change.after);
                }
            });
        }
        for (ino, change) in &delta.links {
            self.put_backptrs(*ino, change.after.clone().unwrap_or_default());
        }
        // A feed's deltas carry entries without link sets, so each cached
        // file's links follow the names that changed.
        for (dir, names) in &delta.entries {
            for (name, change) in names {
                for ino in linked(change.before)
                    .into_iter()
                    .chain(linked(change.after))
                {
                    if !delta.links.contains_key(&ino)
                        && let Some(links) = self.backptrs.get_mut(&ino)
                    {
                        let before = links_bytes(&links.record);
                        relink(
                            &mut links.record,
                            ino,
                            *dir,
                            name,
                            change.before,
                            change.after,
                        );
                        self.tree_bytes += links_bytes(&links.record);
                        self.tree_bytes -= before;
                    }
                }
            }
        }
        for (key, value) in &delta.values {
            self.put_value(*key, Some(value.clone()));
        }
        // A retry reads its record back from storage, so the cache keeps
        // only that the record exists: the results of large batches are
        // large, and a record lives for minutes.
        if let Some((id, _)) = &delta.request
            && let Some(ids) = &mut self.request_ids
        {
            ids.insert(*id);
        }
        for (id, change) in &delta.keys {
            self.keys.put(*id, change.after);
        }
        for acked in &delta.acked {
            if let Freed::Inode(ino) = acked.freed {
                self.free.remove(&ino);
            }
        }
        // A feed's deltas carry neither, so an inode's change drops what
        // the cache held of its record, which a read fetches again.
        for ino in delta.inodes.keys() {
            self.detach.remove(ino);
        }
        for ino in &delta.detached {
            if let Some(above) = delta.above.get(ino) {
                self.detach
                    .insert(*ino, (delta.version, Some(above.clone())));
            }
        }
        for key in &delta.dropped {
            self.put_value(*key, None);
        }
    }

    /// Inode records cached.
    pub fn inodes(&self) -> usize {
        self.inodes.len()
    }

    /// Directories with entries cached.
    pub fn dirs(&self) -> usize {
        self.dirs.len()
    }

    /// What the cache charges for its directories, inode records and
    /// links.
    pub fn tree_bytes(&self) -> usize {
        self.tree_bytes
    }

    /// What the cache charges for its values.
    pub fn value_bytes(&self) -> usize {
        self.value_bytes
    }

    /// Whether `fact` fits in what the tree budget has left.
    pub fn has_room(&self, fact: &Fact) -> bool {
        if let Fact::Value(_, _, value) = fact {
            return self.value_bytes + value_bytes(value) <= self.budget.value_bytes;
        }
        let bytes = match fact {
            Fact::Listing { entries, .. } => {
                DIR_BYTES
                    + entries
                        .iter()
                        .map(|(name, _)| name_bytes(name))
                        .sum::<usize>()
            }
            Fact::Entry(_, name, _) => name_bytes(name),
            Fact::Inode(..) => INODE_BYTES,
            Fact::Backptrs(_, links) => links_bytes(links),
            _ => 0,
        };
        self.tree_bytes + bytes <= self.budget.tree_bytes
    }

    /// Whether the cache holds an inode's record or a directory's entries.
    pub fn holds(&self, ino: Ino) -> bool {
        self.inodes.contains_key(&ino) || self.dirs.contains_key(&ino)
    }
}

impl View for Cache {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>> {
        self.inodes
            .get(&ino)
            .map(|held| *held.read())
            .ok_or(Read::Inode(ino))
    }

    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        if let Some(held) = self.dirs.get(&dir) {
            let cached = held.read();
            if let Some(entry) = cached.entries.get(name) {
                return Ok(*entry);
            }
            if cached.complete {
                return Ok(None);
            }
        }
        Err(Read::Entry(dir, Name::from(name)))
    }

    fn has_entry_outside(&self, dir: Ino, removed: &BTreeSet<Name>) -> Lookup<bool> {
        if let Some(held) = self.dirs.get(&dir) {
            let cached = held.read();
            if cached
                .entries
                .iter()
                .any(|(name, entry)| entry.is_some() && !removed.contains(name))
            {
                return Ok(true);
            }
            if cached.complete {
                return Ok(false);
            }
        }
        Err(Read::Entries {
            dir,
            limit: Some(removed.len() + 1),
        })
    }

    fn backptrs(&self, ino: Ino) -> Lookup<Links> {
        self.backptrs
            .get(&ino)
            .map(|held| held.read().clone())
            .ok_or(Read::Backptrs(ino))
    }

    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        self.values
            .get(&(ino, version))
            .map(|held| held.read().clone())
            .ok_or(Read::Value(ino, version))
    }

    fn request(&self, id: u128) -> Lookup<Option<CommitResult>> {
        if let Some(result) = self.requests.get(&id) {
            return Ok(result.clone());
        }
        match &self.request_ids {
            Some(ids) if !ids.contains(&id) => Ok(None),
            _ => Err(Read::Request(id)),
        }
    }

    fn listing(&self, dir: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>> {
        match self.dirs.get(&dir).map(Held::read) {
            Some(cached) if cached.complete => Ok(cached
                .entries
                .range::<[u8], _>((start, Bound::Unbounded))
                .filter_map(|(name, entry)| entry.map(|entry| (name.clone(), entry)))
                .take(limit)
                .collect()),
            _ => Err(Read::Entries { dir, limit: None }),
        }
    }

    fn free_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        match self.free.get(&ino) {
            Some((freed, above)) if *freed == version => Ok(above.clone()),
            _ => Err(Read::Free(version, ino)),
        }
    }

    fn detach_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        match self.detach.get(&ino) {
            Some((detached, above)) if *detached == version => Ok(above.clone()),
            _ => Err(Read::Detached(version, ino)),
        }
    }

    fn key(&self, id: KeyId) -> Option<KeyRecord> {
        self.keys.get(id)
    }

    fn key_retired(&self, id: KeyId) -> bool {
        self.keys.retired(id)
    }

    fn keys_rooted_at(&self, dir: Ino) -> Vec<KeyId> {
        self.keys.rooted_at(dir)
    }

    fn keys_minted_by(&self, id: KeyId) -> Vec<KeyId> {
        self.keys.minted_by(id)
    }

    fn key_roots(&self) -> Vec<(KeyId, Ino)> {
        self.keys.roots()
    }
}

/// The keys of `held` outside `reached`.
fn outside<V>(held: &BTreeMap<Ino, V>, reached: &BTreeSet<Ino>) -> Vec<Ino> {
    held.keys()
        .filter(|ino| !reached.contains(*ino))
        .copied()
        .collect()
}

/// The file an entry names, if it names one.
fn linked(entry: Option<Entry>) -> Option<Ino> {
    entry
        .filter(|entry| entry.kind == Kind::File)
        .map(|entry| entry.child)
}

/// Moves file `ino`'s links across a change of `dir/name` from `from` to
/// `to`.
fn relink(
    links: &mut Links,
    ino: Ino,
    dir: Ino,
    name: &Name,
    from: Option<Entry>,
    to: Option<Entry>,
) {
    let (was, is) = (linked(from) == Some(ino), linked(to) == Some(ino));
    if was && !is {
        links.remove(&(dir, name.clone()));
    } else if is && !was {
        links.insert((dir, name.clone()));
    }
}

/// Moves a fact read at version `read_at` to version `to`, given every
/// delta between them in ascending version order. Deltas at or below `to`
/// are committed, so their after-images apply; those above it are not, so
/// their before-images restore the state at `to`.
pub fn rebase(fact: Fact, read_at: Version, to: Version, window: &[&Delta]) -> Option<Fact> {
    if read_at == to {
        return Some(fact);
    }
    let forward = read_at < to;
    let (low, high) = if forward {
        (read_at, to)
    } else {
        (to, read_at)
    };
    let mut deltas: Vec<&Delta> = window
        .iter()
        .copied()
        .filter(|delta| delta.version > low && delta.version <= high)
        .collect();
    if !forward {
        deltas.reverse();
    }
    let side = |change_before: Option<Entry>, change_after: Option<Entry>| {
        if forward { change_after } else { change_before }
    };
    Some(match fact {
        Fact::Inode(ino, mut inode) => {
            for delta in deltas {
                if let Some(change) = delta.inodes.get(&ino) {
                    inode = if forward { change.after } else { change.before };
                } else if !forward && delta.created.contains(&ino) {
                    inode = None;
                }
            }
            Fact::Inode(ino, inode)
        }
        Fact::Entry(dir, name, mut entry) => {
            for delta in deltas {
                if let Some(change) = delta.entries.get(&dir).and_then(|names| names.get(&name)) {
                    entry = side(change.before, change.after);
                } else if !forward && delta.created.contains(&dir) {
                    entry = None;
                }
            }
            Fact::Entry(dir, name, entry)
        }
        Fact::Listing {
            dir,
            entries,
            mut complete,
        } => {
            let mut known: BTreeMap<Name, Option<Entry>> = entries.into_iter().collect();
            for delta in deltas {
                if !forward && delta.created.contains(&dir) {
                    known.clear();
                    complete = true;
                }
                if let Some(names) = delta.entries.get(&dir) {
                    for (name, change) in names {
                        known.insert(name.clone(), side(change.before, change.after));
                    }
                }
            }
            Fact::Listing {
                dir,
                entries: known.into_iter().collect(),
                complete,
            }
        }
        Fact::Backptrs(ino, mut links) => {
            for delta in deltas {
                if let Some(change) = delta.links.get(&ino) {
                    let chosen = if forward {
                        &change.after
                    } else {
                        &change.before
                    };
                    links = chosen.clone().unwrap_or_default();
                } else if !forward && delta.created.contains(&ino) {
                    links = Links::new();
                } else {
                    for (dir, names) in &delta.entries {
                        for (name, change) in names {
                            let (from, to) = if forward {
                                (change.before, change.after)
                            } else {
                                (change.after, change.before)
                            };
                            relink(&mut links, ino, *dir, name, from, to);
                        }
                    }
                }
            }
            Fact::Backptrs(ino, links)
        }
        Fact::Value(ino, version, mut value) => {
            for delta in deltas {
                if let Some(written) = delta.values.get(&(ino, version)) {
                    value = forward.then(|| written.clone());
                }
                if delta.dropped.contains(&(ino, version)) {
                    if !forward {
                        // The bytes it deleted are unknown here.
                        return None;
                    }
                    value = None;
                }
            }
            Fact::Value(ino, version, value)
        }
        Fact::Request(id, mut result) => {
            for delta in deltas {
                if let Some((recorded, written)) = &delta.request
                    && *recorded == id
                {
                    result = forward.then(|| written.clone());
                }
            }
            Fact::Request(id, result)
        }
        Fact::Free(version, ino, mut above) => {
            for delta in deltas {
                if delta.version == version
                    && let Some(recorded) = delta.ancestors.get(&Freed::Inode(ino))
                {
                    above = forward.then(|| recorded.clone());
                }
                if let Some(acked) = delta
                    .acked
                    .iter()
                    .find(|acked| acked.version == version && acked.freed == Freed::Inode(ino))
                {
                    above = (!forward).then(|| acked.ancestors.clone());
                }
            }
            Fact::Free(version, ino, above)
        }
        // A record stands unchanged from the detach that writes it to the
        // freeing that deletes it, both of which change the top's inode;
        // across either, the read goes again.
        Fact::Detached(version, ino, above) => {
            if deltas
                .iter()
                .any(|delta| delta.inodes.contains_key(&ino) || delta.detached.contains(&ino))
            {
                return None;
            }
            Fact::Detached(version, ino, above)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::Change;

    fn name(text: &str) -> Name {
        Name::from(text.as_bytes())
    }

    fn file(child: Ino) -> Option<Entry> {
        Some(Entry {
            child,
            kind: Kind::File,
            version: 1,
        })
    }

    fn listing(dir: Ino) -> Fact {
        Fact::Listing {
            dir,
            entries: vec![(name("x"), file(dir + 100))],
            complete: true,
        }
    }

    /// What one of `listing`'s directories costs.
    const LISTING: usize = DIR_BYTES + NAME_BYTES + 1;

    fn budget(tree_bytes: usize) -> Budget {
        Budget {
            tree_bytes,
            value_bytes: 10,
            undo_bytes: 0,
        }
    }

    fn holds(cache: &Cache, dir: Ino) -> bool {
        cache.dirs.contains_key(&dir)
    }

    /// What the cache should charge, counted afresh.
    fn recount(cache: &Cache) -> usize {
        let dirs: usize = cache
            .dirs
            .values()
            .map(|held| {
                let names: usize = held
                    .record
                    .entries
                    .keys()
                    .map(|name| name_bytes(name))
                    .sum();
                assert_eq!(held.record.bytes, DIR_BYTES + names);
                held.record.bytes
            })
            .sum();
        let links: usize = cache
            .backptrs
            .values()
            .map(|held| links_bytes(&held.record))
            .sum();
        dirs + cache.inodes.len() * INODE_BYTES + links
    }

    #[test]
    fn eviction_drops_what_was_written_longest_ago() {
        let mut cache = Cache::with_budget(budget(2 * LISTING));
        for dir in 1..=3 {
            cache.fill(listing(dir));
        }
        cache.evict(u64::MAX);
        assert!(!holds(&cache, 1) && holds(&cache, 2) && holds(&cache, 3));
        // A write renews a record's place.
        cache.fill(Fact::Entry(2, name("y"), None));
        cache.fill(listing(4));
        cache.evict(u64::MAX);
        assert!(holds(&cache, 2) && !holds(&cache, 3) && holds(&cache, 4));
        // A dropped directory loses its completeness: lookups read again.
        assert_eq!(
            cache.entry(3, b"absent"),
            Err(Read::Entry(3, name("absent")))
        );
        assert_eq!(
            cache.listing(3, Bound::Unbounded, usize::MAX),
            Err(Read::Entries {
                dir: 3,
                limit: None
            })
        );
    }

    #[test]
    fn eviction_passes_over_what_reads_used_once() {
        let mut cache = Cache::with_budget(budget(2 * LISTING));
        for dir in 1..=3 {
            cache.fill(listing(dir));
        }
        assert_eq!(cache.entry(1, b"x"), Ok(file(101)));
        cache.evict(u64::MAX);
        assert!(holds(&cache, 1) && !holds(&cache, 2) && holds(&cache, 3));
        // The hand goes on from where it stopped.
        cache.fill(listing(4));
        cache.evict(u64::MAX);
        assert!(holds(&cache, 1) && !holds(&cache, 3) && holds(&cache, 4));
        // Past what reads used, it comes around to what it passed over.
        assert!(cache.entry(4, b"x").is_ok());
        cache.fill(listing(5));
        assert!(cache.entry(5, b"x").is_ok());
        cache.evict(u64::MAX);
        assert!(!holds(&cache, 1) && holds(&cache, 4) && holds(&cache, 5));
        // A cache with no room drops what reads used too.
        assert_eq!(cache.entry(4, b"x"), Ok(file(104)));
        cache.forget_tree(u64::MAX);
        assert_eq!((cache.dirs(), cache.tree_bytes()), (0, 0));
    }

    #[test]
    fn a_write_keeps_that_a_read_used_its_record() {
        let mut cache = Cache::with_budget(budget(INODE_BYTES));
        cache.fill(Fact::Inode(1, Some(Inode::file(1, 0, 0))));
        assert!(cache.inode(1).is_ok());
        cache.fill(Fact::Inode(1, Some(Inode::file(2, 0, 0))));
        cache.fill(Fact::Inode(2, Some(Inode::file(1, 0, 0))));
        cache.evict(u64::MAX);
        assert!(cache.inode(1).is_ok() && cache.inode(2).is_err());
    }

    #[test]
    fn a_value_is_charged_beside_its_bytes() {
        let mut cache = Cache::with_budget(Budget {
            tree_bytes: 0,
            value_bytes: 2 * VALUE_BYTES,
            undo_bytes: 0,
        });
        for ino in 1..=3 {
            cache.fill(Fact::Value(ino, 1, Some(Bytes::new())));
        }
        cache.evict(u64::MAX);
        assert_eq!(cache.value_bytes(), 2 * VALUE_BYTES);
        assert!(cache.value(1, 1).is_err() && cache.value(3, 1).is_ok());
    }

    #[test]
    fn a_directory_is_charged_for_its_names() {
        let mut cache = Cache::with_budget(budget(DIR_BYTES + 10 * (NAME_BYTES + 2)));
        // Lookups of absent names in a directory the cache lacks in full.
        for index in 0..10 {
            cache.fill(Fact::Entry(1, name(&format!("n{index}")), None));
        }
        cache.evict(u64::MAX);
        assert!(holds(&cache, 1));
        cache.fill(Fact::Entry(1, name("n10"), None));
        cache.evict(u64::MAX);
        assert!(!holds(&cache, 1) && cache.tree_bytes() == 0);
    }

    #[test]
    fn a_complete_directory_keeps_no_absent_names() {
        let mut cache = Cache::new();
        let mut delta = Delta::new(1);
        delta.created.insert(1);
        delta.inodes.insert(
            1,
            Change {
                before: None,
                after: Some(Inode::dir(1, 0, 0)),
            },
        );
        cache.apply(&delta);
        for (version, (before, after)) in [(None, file(9)), (file(9), None)].into_iter().enumerate()
        {
            let mut delta = Delta::new(version as u64 + 2);
            delta
                .entries
                .entry(1)
                .or_default()
                .insert(name("temp"), Change { before, after });
            cache.apply(&delta);
        }
        assert!(cache.dirs[&1].record.entries.is_empty());
        assert_eq!(cache.entry(1, b"temp"), Ok(None));
        assert_eq!(cache.tree_bytes(), recount(&cache));
    }

    #[test]
    fn the_charge_follows_what_the_cache_holds() {
        let mut cache = Cache::new();
        cache.fill(listing(1));
        cache.fill(Fact::Listing {
            dir: 2,
            entries: vec![(name("a"), file(20)), (name("gone"), None)],
            complete: false,
        });
        cache.fill(Fact::Entry(2, name("b"), file(21)));
        cache.fill(Fact::Entry(1, name("x"), None));
        cache.fill(Fact::Inode(20, Some(Inode::file(1, 0, 0))));
        cache.fill(Fact::Inode(20, Some(Inode::file(2, 0, 0))));
        cache.fill(Fact::Backptrs(20, Links::from([(2, name("a"))])));
        assert_eq!(cache.tree_bytes(), recount(&cache));
        // A commit renames 2/a to 2/c, links 21 there too, and creates 3.
        let mut delta = Delta::new(1);
        let names = delta.entries.entry(2).or_default();
        names.insert(
            name("a"),
            Change {
                before: file(20),
                after: None,
            },
        );
        names.insert(
            name("c"),
            Change {
                before: None,
                after: file(20),
            },
        );
        delta.created.insert(3);
        delta.inodes.insert(
            3,
            Change {
                before: None,
                after: Some(Inode::dir(1, 0, 2)),
            },
        );
        delta.links.insert(
            21,
            Change {
                before: None,
                after: Some(Links::from([(2, name("b")), (2, name("d"))])),
            },
        );
        cache.apply(&delta);
        assert_eq!(
            cache.backptrs(20),
            Ok(Links::from([(2, name("c"))])),
            "links follow the rename"
        );
        assert_eq!(cache.tree_bytes(), recount(&cache));
        cache.forget_file(21);
        cache.forget_entries(1);
        assert_eq!(cache.tree_bytes(), recount(&cache));
        cache.keep_reachable(2);
        assert_eq!(cache.tree_bytes(), recount(&cache));
        cache.forget_beneath(2);
        assert_eq!(cache.tree_bytes(), recount(&cache));
        assert_eq!(cache.tree_bytes(), 0);
    }

    #[test]
    fn eviction_spares_what_reads_in_progress_wrote() {
        let mut cache = Cache::with_budget(Budget {
            tree_bytes: 0,
            value_bytes: 0,
            undo_bytes: 0,
        });
        cache.fill(listing(1));
        let mark = cache.mark();
        cache.fill(listing(2));
        cache.fill(Fact::Inode(9, Some(Inode::file(1, 0, 3))));
        cache.fill(Fact::Value(9, 1, Some(Bytes::from_static(b"abc"))));
        cache.evict(mark);
        assert!(!holds(&cache, 1), "written before the mark");
        assert!(holds(&cache, 2) && cache.inode(9).is_ok() && cache.value(9, 1).is_ok());
        cache.evict(u64::MAX);
        assert!(!holds(&cache, 2) && cache.inode(9).is_err() && cache.value(9, 1).is_err());
    }

    #[test]
    fn forgetting_the_tree_spares_reads_in_progress() {
        let mut cache = Cache::new();
        cache.fill(listing(1));
        let mark = cache.mark();
        cache.fill(listing(2));
        cache.forget_tree(mark);
        assert!(!holds(&cache, 1) && holds(&cache, 2));
        cache.forget_tree(u64::MAX);
        assert!(!holds(&cache, 2));
    }
}
