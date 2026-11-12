//! The writer's uncommitted changes: the newest after-image of every key
//! that a batch not yet committed wrote, tagged with that batch's version.
//! Once a group commits, its versions leave the overlay for the cache.

use crate::auth::{KeyId, KeyRecord};
use crate::delta::{Delta, Freed, Links};
use crate::ops::CommitResult;
use crate::types::{Entry, Ino, Inode, Name, Version};
use crate::view::{Lookup, View, layered_listing};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

/// A free-queue entry an uncommitted batch wrote (its ancestors) or
/// acknowledged (None), with that batch's version.
type FreeChange = (Version, Option<Vec<Ino>>);

#[derive(Default, Debug)]
pub struct Overlay {
    inodes: BTreeMap<Ino, (Version, Option<Inode>)>,
    entries: BTreeMap<Ino, BTreeMap<Name, (Version, Option<Entry>)>>,
    links: BTreeMap<Ino, (Version, Links)>,
    created: BTreeMap<Ino, Version>,
    values: BTreeMap<(Ino, Version), Bytes>,
    requests: BTreeMap<u128, (Version, CommitResult)>,
    keys: BTreeMap<KeyId, (Version, Option<KeyRecord>)>,
    /// The keys minted in the overlay, by minter and by root; a lookup
    /// checks each still stands.
    minted: BTreeMap<KeyId, BTreeSet<KeyId>>,
    rooted: BTreeMap<Ino, BTreeSet<KeyId>>,
    /// Freed inodes' entries, by the version that freed each.
    free: BTreeMap<(Version, Ino), FreeChange>,
    /// Detach records, by the version that detached each top.
    detach: BTreeMap<(Version, Ino), FreeChange>,
    dropped: BTreeMap<(Ino, Version), Version>,
    /// Directories uncommitted batches bring to hold or lie above a key's
    /// root, with the latest version that did.
    covers: BTreeMap<Ino, Version>,
    /// The cover the latest uncommitted search found, with its version.
    cover: Option<(Version, BTreeSet<Ino>)>,
}

impl Overlay {
    pub fn add(&mut self, delta: &Delta) {
        let version = delta.version;
        for ino in &delta.created {
            self.created.insert(*ino, version);
        }
        for (ino, change) in &delta.inodes {
            self.inodes.insert(*ino, (version, change.after));
        }
        for (dir, names) in &delta.entries {
            let overlaid = self.entries.entry(*dir).or_default();
            for (name, change) in names {
                overlaid.insert(name.clone(), (version, change.after));
            }
        }
        for (ino, change) in &delta.links {
            self.links
                .insert(*ino, (version, change.after.clone().unwrap_or_default()));
        }
        for (key, value) in &delta.values {
            self.values.insert(*key, value.clone());
        }
        if let Some((id, result)) = &delta.request {
            self.requests.insert(*id, (version, result.clone()));
        }
        for (id, change) in &delta.keys {
            self.keys.insert(*id, (version, change.after));
            if let Some(record) = change.after {
                self.index(*id, &record);
            }
        }
        for (freed, above) in &delta.ancestors {
            if let Freed::Inode(ino) = freed {
                self.free
                    .insert((version, *ino), (version, Some(above.clone())));
            }
        }
        for acked in &delta.acked {
            if let Freed::Inode(ino) = acked.freed {
                self.free.insert((acked.version, ino), (version, None));
            }
        }
        for ino in &delta.detached {
            let above = delta.above.get(ino).cloned();
            self.detach.insert((version, *ino), (version, above));
        }
        for (detached, ino) in &delta.undetached {
            self.detach.insert((*detached, *ino), (version, None));
        }
        for key in &delta.dropped {
            self.values.remove(key);
            self.dropped.insert(*key, version);
        }
        for dir in &delta.covers {
            self.covers.insert(*dir, version);
        }
        if let Some(cover) = &delta.cover {
            self.cover = Some((version, cover.clone()));
        }
    }

    fn index(&mut self, id: KeyId, record: &KeyRecord) {
        if let Some(minter) = record.minted_by {
            self.minted.entry(minter).or_default().insert(id);
        }
        self.rooted.entry(record.root).or_default().insert(id);
    }

    /// The keys in `index` that the overlay still holds.
    fn standing<'a>(
        &'a self,
        index: Option<&'a BTreeSet<KeyId>>,
    ) -> impl Iterator<Item = KeyId> + 'a {
        index
            .into_iter()
            .flatten()
            .copied()
            .filter(|id| matches!(self.keys.get(id), Some((_, Some(_)))))
    }

    /// `base`'s keys less those the overlay changed, with `own`.
    fn layered(&self, base: Vec<KeyId>, own: impl Iterator<Item = KeyId>) -> Vec<KeyId> {
        let mut ids: BTreeSet<KeyId> = base
            .into_iter()
            .filter(|id| !self.keys.contains_key(id))
            .collect();
        ids.extend(own);
        ids.into_iter().collect()
    }

    /// Drops what versions up to `committed` wrote, now that the cache holds
    /// it. Keys a later version wrote stay.
    pub fn prune(&mut self, committed: Version) {
        self.inodes.retain(|_, (version, _)| *version > committed);
        for names in self.entries.values_mut() {
            names.retain(|_, (version, _)| *version > committed);
        }
        self.entries.retain(|_, names| !names.is_empty());
        self.links.retain(|_, (version, _)| *version > committed);
        self.created.retain(|_, version| *version > committed);
        self.values.retain(|(_, version), _| *version > committed);
        self.requests.retain(|_, (version, _)| *version > committed);
        self.keys.retain(|_, (version, _)| *version > committed);
        self.minted.clear();
        self.rooted.clear();
        let standing: Vec<(KeyId, KeyRecord)> = self
            .keys
            .iter()
            .filter_map(|(id, (_, record))| record.map(|record| (*id, record)))
            .collect();
        for (id, record) in standing {
            self.index(id, &record);
        }
        self.free.retain(|_, (version, _)| *version > committed);
        self.detach.retain(|_, (version, _)| *version > committed);
        self.dropped.retain(|_, version| *version > committed);
        self.covers.retain(|_, version| *version > committed);
        if self
            .cover
            .as_ref()
            .is_some_and(|(version, _)| *version <= committed)
        {
            self.cover = None;
        }
    }

    pub fn clear(&mut self) {
        *self = Overlay::default();
    }

    pub fn is_empty(&self) -> bool {
        self.inodes.is_empty()
            && self.entries.is_empty()
            && self.links.is_empty()
            && self.created.is_empty()
            && self.values.is_empty()
            && self.requests.is_empty()
            && self.keys.is_empty()
            && self.free.is_empty()
            && self.dropped.is_empty()
    }
}

/// The writer's view for running batches: the overlay over the cache.
pub struct Overlaid<'a> {
    pub overlay: &'a Overlay,
    pub base: &'a dyn View,
}

impl View for Overlaid<'_> {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>> {
        match self.overlay.inodes.get(&ino) {
            Some((_, inode)) => Ok(*inode),
            None if self.overlay.created.contains_key(&ino) => Ok(None),
            None => self.base.inode(ino),
        }
    }

    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        if let Some((_, entry)) = self
            .overlay
            .entries
            .get(&dir)
            .and_then(|names| names.get(name))
        {
            return Ok(*entry);
        }
        if self.overlay.created.contains_key(&dir) {
            return Ok(None);
        }
        self.base.entry(dir, name)
    }

    fn has_entry_outside(&self, dir: Ino, removed: &BTreeSet<Name>) -> Lookup<bool> {
        let mut also = None;
        if let Some(names) = self.overlay.entries.get(&dir) {
            for (name, (_, entry)) in names {
                if removed.contains(name) {
                    continue;
                }
                if entry.is_some() {
                    return Ok(true);
                }
                also.get_or_insert_with(|| removed.clone())
                    .insert(name.clone());
            }
        }
        if self.overlay.created.contains_key(&dir) {
            return Ok(false);
        }
        self.base
            .has_entry_outside(dir, also.as_ref().unwrap_or(removed))
    }

    fn backptrs(&self, ino: Ino) -> Lookup<Links> {
        match self.overlay.links.get(&ino) {
            Some((_, links)) => Ok(links.clone()),
            None if self.overlay.created.contains_key(&ino) => Ok(Links::new()),
            None => self.base.backptrs(ino),
        }
    }

    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        match self.overlay.values.get(&(ino, version)) {
            Some(value) => Ok(Some(value.clone())),
            None if self.overlay.dropped.contains_key(&(ino, version)) => Ok(None),
            None => self.base.value(ino, version),
        }
    }

    fn request(&self, id: u128) -> Lookup<Option<CommitResult>> {
        match self.overlay.requests.get(&id) {
            Some((_, result)) => Ok(Some(result.clone())),
            None => self.base.request(id),
        }
    }

    fn listing(&self, dir: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>> {
        let changes = self
            .overlay
            .entries
            .get(&dir)
            .into_iter()
            .flat_map(|names| names.range::<[u8], _>((start, Bound::Unbounded)))
            .map(|(name, (_, entry))| (name, *entry));
        layered_listing(
            self.base,
            dir,
            self.overlay.created.contains_key(&dir),
            changes,
            start,
            limit,
        )
    }

    fn free_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        match self.overlay.free.get(&(version, ino)) {
            Some((_, above)) => Ok(above.clone()),
            None => self.base.free_entry(version, ino),
        }
    }

    fn detach_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        match self.overlay.detach.get(&(version, ino)) {
            Some((_, above)) => Ok(above.clone()),
            None => self.base.detach_entry(version, ino),
        }
    }

    fn key(&self, id: KeyId) -> Option<KeyRecord> {
        match self.overlay.keys.get(&id) {
            Some((_, record)) => *record,
            None => self.base.key(id),
        }
    }

    fn key_retired(&self, id: KeyId) -> bool {
        match self.overlay.keys.get(&id) {
            Some((_, record)) => record.is_none(),
            None => self.base.key_retired(id),
        }
    }

    fn keys_rooted_at(&self, dir: Ino) -> Vec<KeyId> {
        self.overlay.layered(
            self.base.keys_rooted_at(dir),
            self.overlay.standing(self.overlay.rooted.get(&dir)),
        )
    }

    fn keys_minted_by(&self, id: KeyId) -> Vec<KeyId> {
        self.overlay.layered(
            self.base.keys_minted_by(id),
            self.overlay.standing(self.overlay.minted.get(&id)),
        )
    }

    fn key_roots(&self) -> BTreeSet<Ino> {
        let mut roots = self.base.key_roots();
        for (id, (_, record)) in &self.overlay.keys {
            match record {
                Some(record) => {
                    roots.insert(record.root);
                }
                // The root of a key the overlay deleted may hold no other.
                None => {
                    if let Some(deleted) = self.base.key(*id)
                        && self.keys_rooted_at(deleted.root).is_empty()
                    {
                        roots.remove(&deleted.root);
                    }
                }
            }
        }
        roots
    }

    fn covers(&self, dir: Ino) -> Option<bool> {
        if self.overlay.covers.contains_key(&dir) {
            return Some(true);
        }
        // A search's cover holds what every batch before it covered; the
        // batches after it add theirs above.
        match &self.overlay.cover {
            Some((_, cover)) => Some(cover.contains(&dir)),
            None => self.base.covers(dir),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Grant, Keys};
    use crate::cache::Cache;

    fn record(root: Ino) -> KeyRecord {
        KeyRecord {
            hash: [7; 32],
            root,
            grant: Grant::Read,
            expires: 0,
            minted_by: None,
        }
    }

    #[test]
    fn key_roots_follow_the_keys_uncommitted_batches_mint_and_delete() {
        let mut keys = Keys::default();
        keys.put(1, Some(record(5)));
        keys.put(2, Some(record(5)));
        keys.put(3, Some(record(6)));
        let mut cache = Cache::new();
        cache.load_keys(keys);
        // One of the two keys at 5 goes, the only key at 6 goes, 7 gains
        // one, and 8 gains one that a later batch deletes.
        let mut first = Delta::new(9);
        first.set_key(1, Some(record(5)), None);
        first.set_key(3, Some(record(6)), None);
        first.set_key(4, None, Some(record(7)));
        first.set_key(10, None, Some(record(8)));
        let mut second = Delta::new(10);
        second.set_key(10, Some(record(8)), None);
        let mut overlay = Overlay::default();
        overlay.add(&first);
        overlay.add(&second);
        let view = Overlaid {
            overlay: &overlay,
            base: &cache,
        };
        assert_eq!(view.key_roots(), BTreeSet::from([5, 7]));
    }
}
