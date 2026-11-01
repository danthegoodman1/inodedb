//! What a holder knows of the tree at its current version.

use crate::auth::{KeyId, KeyRecord, Keys};
use crate::delta::{Delta, Freed, Links};
use crate::ops::CommitResult;
use crate::snapshot::Kept;
use crate::types::{Entry, Ino, Inode, Kind, Name, Version};
use crate::view::{Fact, Lookup, Read, View};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;

/// How much a cache holds before it evicts what it cached longest ago.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Directories with entries cached.
    pub dirs: usize,
    /// Inode records, with each file's links.
    pub inodes: usize,
    /// Bytes of values.
    pub value_bytes: usize,
    /// Bytes of a subscriber's undo records and kept states, for its
    /// snapshots.
    pub undo_bytes: usize,
}

impl Default for Budget {
    fn default() -> Budget {
        // A cached file costs about 830 bytes, so a full cache holds about
        // 0.8 GB of tree beside its values (`BENCHMARKS.md`).
        Budget {
            dirs: 1 << 18,
            inodes: 1 << 20,
            value_bytes: 256 << 20,
            undo_bytes: 64 << 20,
        }
    }
}

/// The order the cache last wrote what it holds. Each write takes the
/// next place; a queue keeps places oldest first, and a place the key has
/// since left is stale.
#[derive(Debug)]
struct Queue<K> {
    places: VecDeque<(u64, K)>,
    latest: BTreeMap<K, u64>,
}

impl<K> Default for Queue<K> {
    fn default() -> Queue<K> {
        Queue {
            places: VecDeque::new(),
            latest: BTreeMap::new(),
        }
    }
}

impl<K: Ord + Copy> Queue<K> {
    fn touch(&mut self, place: u64, key: K) {
        self.latest.insert(key, place);
        self.places.push_back((place, key));
        // Writes to the same keys leave stale places; drop them once they
        // outnumber the keys.
        if self.places.len() > 2 * self.latest.len() + 1024 {
            let latest = &self.latest;
            self.places
                .retain(|(place, key)| latest.get(key) == Some(place));
        }
    }

    /// The key written longest ago, if written before `spare`.
    fn oldest(&mut self, spare: u64) -> Option<K> {
        while let Some(&(place, key)) = self.places.front() {
            if place >= spare {
                return None;
            }
            self.places.pop_front();
            if self.latest.get(&key) == Some(&place) {
                self.latest.remove(&key);
                return Some(key);
            }
        }
        None
    }

    fn forget(&mut self, key: K) {
        self.latest.remove(&key);
    }
}

#[derive(Default, Debug)]
struct Order {
    dirs: Queue<Ino>,
    inodes: Queue<Ino>,
    values: Queue<(Ino, Version)>,
    written: u64,
}

#[derive(Default, Debug)]
struct Dir {
    /// Known names: present entries and known-absent ones.
    entries: BTreeMap<Name, Option<Entry>>,
    /// Every present entry is in `entries`.
    complete: bool,
}

#[derive(Default, Debug)]
pub struct Cache {
    budget: Budget,
    order: Order,
    value_bytes: usize,
    inodes: BTreeMap<Ino, Option<Inode>>,
    dirs: BTreeMap<Ino, Dir>,
    backptrs: BTreeMap<Ino, Links>,
    values: BTreeMap<(Ino, Version), Option<Bytes>>,
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

    fn dir(&mut self, dir: Ino) -> &mut Dir {
        let place = self.place();
        self.order.dirs.touch(place, dir);
        self.dirs.entry(dir).or_default()
    }

    fn put_inode(&mut self, ino: Ino, inode: Option<Inode>) {
        let place = self.place();
        self.order.inodes.touch(place, ino);
        self.inodes.insert(ino, inode);
    }

    fn put_backptrs(&mut self, ino: Ino, links: Links) {
        let place = self.place();
        self.order.inodes.touch(place, ino);
        self.backptrs.insert(ino, links);
    }

    fn put_value(&mut self, key: (Ino, Version), value: Option<Bytes>) {
        let place = self.place();
        self.order.values.touch(place, key);
        let len = value.as_ref().map_or(0, Bytes::len);
        if let Some(old) = self.values.insert(key, value) {
            self.value_bytes -= old.as_ref().map_or(0, Bytes::len);
        }
        self.value_bytes += len;
    }

    /// Evicts what it wrote longest ago until it fits its budget, sparing
    /// what it wrote at or after `spare`, which reads in progress need.
    pub fn evict(&mut self, spare: u64) {
        while self.dirs.len() > self.budget.dirs
            && let Some(dir) = self.order.dirs.oldest(spare)
        {
            self.dirs.remove(&dir);
        }
        while self.inodes.len().max(self.backptrs.len()) > self.budget.inodes
            && let Some(ino) = self.order.inodes.oldest(spare)
        {
            self.inodes.remove(&ino);
            self.backptrs.remove(&ino);
            self.free.remove(&ino);
            self.detach.remove(&ino);
        }
        while self.value_bytes > self.budget.value_bytes
            && let Some(key) = self.order.values.oldest(spare)
        {
            if let Some(value) = self.values.remove(&key) {
                self.value_bytes -= value.as_ref().map_or(0, Bytes::len);
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
                dirs: 0,
                inodes: 0,
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
        self.order.dirs.forget(dir);
        Kept {
            inode: None,
            entries: self
                .dirs
                .remove(&dir)
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
            for entry in cached.entries.values().flatten() {
                if reached.insert(entry.child) && entry.kind == Kind::Dir {
                    stack.push(entry.child);
                }
            }
        }
        let mut forgot: BTreeMap<Ino, Kept> = BTreeMap::new();
        let order = &mut self.order;
        for (dir, cached) in self.dirs.extract_if(.., |dir, _| !reached.contains(dir)) {
            order.dirs.forget(dir);
            forgot.entry(dir).or_default().entries = Some((cached.entries, cached.complete));
        }
        for (ino, inode) in self.inodes.extract_if(.., |ino, _| !reached.contains(ino)) {
            order.inodes.forget(ino);
            forgot.entry(ino).or_default().inode = Some(inode);
        }
        self.backptrs.retain(|ino, _| reached.contains(ino));
        self.free.retain(|ino, _| reached.contains(ino));
        self.detach.retain(|ino, _| reached.contains(ino));
        let mut dropped = 0;
        self.values.retain(|key, value| {
            let keep = reached.contains(&key.0);
            if !keep {
                dropped += value.as_ref().map_or(0, Bytes::len);
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
        self.backptrs.remove(&ino);
        self.order.inodes.forget(ino);
        Kept {
            inode: self.inodes.remove(&ino),
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
                self.dir(dir).entries.insert(name, entry);
            }
            Fact::Listing {
                dir,
                entries,
                complete,
            } => {
                let cached = self.dir(dir);
                if complete {
                    cached.entries.clear();
                    cached.complete = true;
                }
                for (name, entry) in entries {
                    if entry.is_some() || !complete {
                        cached.entries.insert(name, entry);
                    }
                }
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
                    *self.dir(*ino) = Dir {
                        entries: BTreeMap::new(),
                        complete: true,
                    };
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
            let cached = self.dir(*dir);
            for (name, change) in names {
                cached.entries.insert(name.clone(), change.after);
            }
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
                        relink(links, ino, *dir, name, change.before, change.after);
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

    pub fn inodes(&self) -> usize {
        self.inodes.len()
    }

    /// Whether the cache holds an inode's record or a directory's entries.
    pub fn holds(&self, ino: Ino) -> bool {
        self.inodes.contains_key(&ino) || self.dirs.contains_key(&ino)
    }
}

impl View for Cache {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>> {
        self.inodes.get(&ino).copied().ok_or(Read::Inode(ino))
    }

    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        if let Some(cached) = self.dirs.get(&dir) {
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
        if let Some(cached) = self.dirs.get(&dir) {
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
        self.backptrs.get(&ino).cloned().ok_or(Read::Backptrs(ino))
    }

    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        self.values
            .get(&(ino, version))
            .cloned()
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
        match self.dirs.get(&dir) {
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

/// Moves a fact read at version `read_at` to version `to`, given every
/// delta between them in ascending version order. Deltas at or below `to`
/// are committed, so their after-images apply; those above it are not, so
/// their before-images restore the state at `to`.
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

    fn listing(dir: Ino) -> Fact {
        Fact::Listing {
            dir,
            entries: vec![(
                Name::from(&b"x"[..]),
                Some(Entry {
                    child: dir + 100,
                    kind: Kind::File,
                    version: 1,
                }),
            )],
            complete: true,
        }
    }

    fn holds(cache: &Cache, dir: Ino) -> bool {
        cache.entry(dir, b"x").is_ok()
    }

    #[test]
    fn eviction_drops_what_was_written_longest_ago() {
        let mut cache = Cache::with_budget(Budget {
            dirs: 2,
            inodes: 10,
            value_bytes: 10,
            undo_bytes: 0,
        });
        for dir in 1..=3 {
            cache.fill(listing(dir));
        }
        cache.evict(u64::MAX);
        assert!(!holds(&cache, 1) && holds(&cache, 2) && holds(&cache, 3));
        // A write renews a record's place.
        cache.fill(Fact::Entry(2, Name::from(&b"y"[..]), None));
        cache.fill(listing(4));
        cache.evict(u64::MAX);
        assert!(holds(&cache, 2) && !holds(&cache, 3) && holds(&cache, 4));
        // A dropped directory loses its completeness: lookups read again.
        assert_eq!(
            cache.entry(3, b"absent"),
            Err(Read::Entry(3, Name::from(&b"absent"[..])))
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
    fn eviction_spares_what_reads_in_progress_wrote() {
        let mut cache = Cache::with_budget(Budget {
            dirs: 0,
            inodes: 0,
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
