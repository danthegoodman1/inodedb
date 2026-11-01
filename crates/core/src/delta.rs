//! A batch's changes, with the state each change replaced.

use crate::auth::{self, KeyId, KeyRecord};
use crate::codec::{Dec, DecodeError, Enc};
use crate::keys::{self, WriteSet};
use crate::ops::CommitResult;
use crate::types::{Entry, Ino, Inode, Name, Version};
use crate::view::{Lookup, View, layered_listing};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change<T> {
    pub before: Option<T>,
    pub after: Option<T>,
}

/// What the free queue gains.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Freed {
    Inode(Ino),
    /// A value that `Put` replaced: the file and the value's version.
    Value(Ino, Version),
}

pub type Links = BTreeSet<(Ino, Name)>;

/// A free-queue entry a consumer acknowledged: it goes, with the records it
/// kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acked {
    /// The version that freed it.
    pub version: Version,
    pub freed: Freed,
    /// The directories it recorded above what it freed.
    pub ancestors: Vec<Ino>,
}

#[derive(Clone, Debug, Default)]
pub struct Delta {
    /// The version the changes commit at.
    pub version: Version,
    pub inodes: BTreeMap<Ino, Change<Inode>>,
    pub entries: BTreeMap<Ino, BTreeMap<Name, Change<Entry>>>,
    /// Files whose links changed, with their link sets before and after.
    pub links: BTreeMap<Ino, Change<Links>>,
    /// Inodes this delta created: directories start empty, files unlinked.
    pub created: BTreeSet<Ino>,
    pub values: BTreeMap<(Ino, Version), Bytes>,
    pub freed: Vec<Freed>,
    /// The directories above each freed record as the commit found them,
    /// so a consumer claims only what lay in its subtree.
    pub ancestors: BTreeMap<Freed, Vec<Ino>>,
    pub detached: Vec<Ino>,
    /// The directories above each detached subtree's top as it detached,
    /// which its freed records name in place of what lies above it later.
    pub above: BTreeMap<Ino, Vec<Ino>>,
    /// Detach records of subtrees freed in full: the version that detached
    /// each, and its top.
    pub undetached: Vec<(Version, Ino)>,
    pub acked: Vec<Acked>,
    /// Values deleted with their acknowledged entries.
    pub dropped: BTreeSet<(Ino, Version)>,
    pub request: Option<(u128, CommitResult)>,
    pub keys: BTreeMap<KeyId, Change<KeyRecord>>,
    /// Leases opened, whose records commit before the opening is answered.
    pub leases: BTreeMap<crate::lease::LeaseId, Option<crate::lease::StoredLease>>,
}

impl Delta {
    pub fn new(version: Version) -> Delta {
        Delta {
            version,
            ..Delta::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inodes.is_empty()
            && self.entries.is_empty()
            && self.links.is_empty()
            && self.values.is_empty()
            && self.freed.is_empty()
            && self.detached.is_empty()
            && self.keys.is_empty()
            && self.acked.is_empty()
            && self.leases.is_empty()
    }

    pub fn inode(&self, base: &dyn View, ino: Ino) -> Lookup<Option<Inode>> {
        match self.inodes.get(&ino) {
            Some(change) => Ok(change.after),
            None if self.created.contains(&ino) => Ok(None),
            None => base.inode(ino),
        }
    }

    pub fn entry(&self, base: &dyn View, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        if let Some(change) = self.entries.get(&dir).and_then(|names| names.get(name)) {
            return Ok(change.after);
        }
        if self.created.contains(&dir) {
            return Ok(None);
        }
        base.entry(dir, name)
    }

    pub fn has_entry_outside(
        &self,
        base: &dyn View,
        dir: Ino,
        removed: &BTreeSet<Name>,
    ) -> Lookup<bool> {
        let mut also = None;
        if let Some(names) = self.entries.get(&dir) {
            for (name, change) in names {
                if removed.contains(name) {
                    continue;
                }
                if change.after.is_some() {
                    return Ok(true);
                }
                also.get_or_insert_with(|| removed.clone())
                    .insert(name.clone());
            }
        }
        if self.created.contains(&dir) {
            return Ok(false);
        }
        base.has_entry_outside(dir, also.as_ref().unwrap_or(removed))
    }

    pub fn backptrs(&self, base: &dyn View, ino: Ino) -> Lookup<Links> {
        match self.links.get(&ino) {
            Some(change) => Ok(change.after.clone().unwrap_or_default()),
            None if self.created.contains(&ino) => Ok(Links::new()),
            None => base.backptrs(ino),
        }
    }

    pub fn value(&self, base: &dyn View, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        match self.values.get(&(ino, version)) {
            Some(value) => Ok(Some(value.clone())),
            None if self.dropped.contains(&(ino, version)) => Ok(None),
            None => base.value(ino, version),
        }
    }

    /// The ancestors a freed inode's entry records, if the entry stands.
    pub fn free_entry(
        &self,
        base: &dyn View,
        version: Version,
        ino: Ino,
    ) -> Lookup<Option<Vec<Ino>>> {
        if version == self.version
            && let Some(above) = self.ancestors.get(&Freed::Inode(ino))
        {
            return Ok(Some(above.clone()));
        }
        if self
            .acked
            .iter()
            .any(|acked| acked.version == version && acked.freed == Freed::Inode(ino))
        {
            return Ok(None);
        }
        base.free_entry(version, ino)
    }

    /// The ancestors a detached top's record names, if the record stands.
    pub fn detach_entry(
        &self,
        base: &dyn View,
        version: Version,
        ino: Ino,
    ) -> Lookup<Option<Vec<Ino>>> {
        if version == self.version
            && self.detached.contains(&ino)
            && let Some(above) = self.above.get(&ino)
        {
            return Ok(Some(above.clone()));
        }
        if self.undetached.contains(&(version, ino)) {
            return Ok(None);
        }
        base.detach_entry(version, ino)
    }

    pub fn request(&self, base: &dyn View, id: u128) -> Lookup<Option<CommitResult>> {
        match &self.request {
            Some((recorded, result)) if *recorded == id => Ok(Some(result.clone())),
            _ => base.request(id),
        }
    }

    pub fn listing(
        &self,
        base: &dyn View,
        dir: Ino,
        start: Bound<&[u8]>,
        limit: usize,
    ) -> Lookup<Vec<(Name, Entry)>> {
        let changes = self
            .entries
            .get(&dir)
            .into_iter()
            .flat_map(|names| names.range::<[u8], _>((start, Bound::Unbounded)))
            .map(|(name, change)| (name, change.after));
        layered_listing(
            base,
            dir,
            self.created.contains(&dir),
            changes,
            start,
            limit,
        )
    }

    pub fn key(&self, base: &dyn View, id: KeyId) -> Option<KeyRecord> {
        match self.keys.get(&id) {
            Some(change) => change.after,
            None => base.key(id),
        }
    }

    fn key_layer(&self) -> impl Iterator<Item = (KeyId, Option<&KeyRecord>)> {
        self.keys
            .iter()
            .map(|(id, change)| (*id, change.after.as_ref()))
    }

    pub fn keys_rooted_at(&self, base: &dyn View, dir: Ino) -> Vec<KeyId> {
        auth::layered(base.keys_rooted_at(dir), self.key_layer(), |record| {
            record.root == dir
        })
    }

    pub fn keys_minted_by(&self, base: &dyn View, id: KeyId) -> Vec<KeyId> {
        auth::layered(base.keys_minted_by(id), self.key_layer(), |record| {
            record.minted_by == Some(id)
        })
    }

    pub fn key_roots(&self, base: &dyn View) -> Vec<(KeyId, Ino)> {
        let mut roots: BTreeMap<KeyId, Ino> = base.key_roots().into_iter().collect();
        for (id, change) in &self.keys {
            match change.after {
                Some(record) => roots.insert(*id, record.root),
                None => roots.remove(id),
            };
        }
        roots.into_iter().collect()
    }

    /// Records a key's new state; `before` is its state ahead of this delta.
    pub fn set_key(&mut self, id: KeyId, before: Option<KeyRecord>, after: Option<KeyRecord>) {
        self.keys
            .entry(id)
            .or_insert(Change {
                before,
                after: None,
            })
            .after = after;
    }

    /// Records an inode's new state; `before` is its state ahead of this
    /// delta, which the caller has read.
    pub fn set_inode(&mut self, ino: Ino, before: Option<Inode>, after: Inode) {
        self.inodes
            .entry(ino)
            .or_insert(Change {
                before,
                after: None,
            })
            .after = Some(after);
    }

    pub fn set_entry(
        &mut self,
        dir: Ino,
        name: &[u8],
        before: Option<Entry>,
        after: Option<Entry>,
    ) {
        let names = self.entries.entry(dir).or_default();
        match names.get_mut(name) {
            Some(change) => change.after = after,
            None => {
                names.insert(Name::from(name), Change { before, after });
            }
        }
    }

    pub fn set_links(&mut self, ino: Ino, before: Option<Links>, after: Links) {
        self.links
            .entry(ino)
            .or_insert(Change {
                before,
                after: None,
            })
            .after = Some(after);
    }

    /// Every key this delta writes, with its value.
    pub fn write_set(&self) -> WriteSet {
        let mut writes = WriteSet::default();
        for (ino, change) in &self.inodes {
            match (&change.before, &change.after) {
                (before, Some(after)) if before.as_ref() != Some(after) => {
                    writes.set(keys::inode(*ino), after.to_bytes())
                }
                (Some(_), None) => writes.clear(keys::inode(*ino)),
                _ => {}
            }
        }
        for (dir, names) in &self.entries {
            for (name, change) in names {
                match (&change.before, &change.after) {
                    (before, Some(after)) if before.as_ref() != Some(after) => {
                        writes.set(keys::entry(*dir, name), after.to_bytes())
                    }
                    (Some(_), None) => writes.clear(keys::entry(*dir, name)),
                    _ => {}
                }
            }
        }
        for (ino, change) in &self.links {
            let empty = Links::new();
            let before = change.before.as_ref().unwrap_or(&empty);
            let after = change.after.as_ref().unwrap_or(&empty);
            for (parent, name) in before.difference(after) {
                writes.clear(keys::backptr(*ino, *parent, name));
            }
            for (parent, name) in after.difference(before) {
                writes.set(keys::backptr(*ino, *parent, name), Vec::new());
            }
        }
        for ((ino, version), value) in &self.values {
            writes.set(keys::value(*ino, *version), value.to_vec());
        }
        for freed in &self.freed {
            let key = match *freed {
                Freed::Inode(ino) => keys::free(self.version, ino, None),
                Freed::Value(ino, value) => keys::free(self.version, ino, Some(value)),
            };
            let above = self.ancestors.get(freed).map_or(&[][..], Vec::as_slice);
            writes.set(key, encode_ancestors(above));
        }
        for ino in &self.detached {
            let above = self.above.get(ino).map_or(&[][..], Vec::as_slice);
            writes.set(keys::detached(self.version, *ino), encode_ancestors(above));
        }
        for (version, ino) in &self.undetached {
            writes.clear(keys::detached(*version, *ino));
        }
        for (id, change) in &self.keys {
            match change.after {
                Some(record) => writes.set(keys::key_record(*id), record.encode()),
                None => {
                    writes.clear(keys::key_record(*id));
                    writes.set(keys::key_retired(*id), Vec::new());
                }
            }
        }
        for acked in &self.acked {
            let key = match acked.freed {
                Freed::Inode(ino) => keys::free(acked.version, ino, None),
                Freed::Value(ino, value) => keys::free(acked.version, ino, Some(value)),
            };
            writes.clear(key);
        }
        for (ino, version) in &self.dropped {
            writes.clear(keys::value(*ino, *version));
        }
        for (lease, stored) in &self.leases {
            match stored {
                Some(stored) => writes.set(keys::lease(*lease), stored.encode()),
                None => writes.clear(keys::lease(*lease)),
            }
        }
        writes
    }
}

/// A free-queue entry's value: the directories above what it frees.
pub fn encode_ancestors(dirs: &[Ino]) -> Vec<u8> {
    let mut enc = Enc::with_capacity(1 + 4 * dirs.len());
    enc.varint(dirs.len() as u64);
    for dir in dirs {
        enc.varint(*dir);
    }
    enc.finish()
}

pub fn decode_ancestors(bytes: &[u8]) -> Result<Vec<Ino>, DecodeError> {
    let mut dec = Dec::new(bytes);
    let count = dec.varint()?;
    if count > bytes.len() as u64 {
        return Err(DecodeError);
    }
    let dirs = (0..count).map(|_| dec.varint()).collect::<Result<_, _>>()?;
    dec.end()?;
    Ok(dirs)
}

/// A view after a delta's changes.
pub struct After<'a> {
    pub delta: &'a Delta,
    pub base: &'a dyn View,
}

impl View for After<'_> {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>> {
        self.delta.inode(self.base, ino)
    }

    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>> {
        self.delta.entry(self.base, dir, name)
    }

    fn has_entry_outside(&self, dir: Ino, removed: &BTreeSet<Name>) -> Lookup<bool> {
        self.delta.has_entry_outside(self.base, dir, removed)
    }

    fn backptrs(&self, ino: Ino) -> Lookup<Links> {
        self.delta.backptrs(self.base, ino)
    }

    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>> {
        self.delta.value(self.base, ino, version)
    }

    fn request(&self, id: u128) -> Lookup<Option<CommitResult>> {
        self.delta.request(self.base, id)
    }

    fn listing(&self, dir: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>> {
        self.delta.listing(self.base, dir, start, limit)
    }

    fn free_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.delta.free_entry(self.base, version, ino)
    }

    fn detach_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>> {
        self.delta.detach_entry(self.base, version, ino)
    }

    fn key(&self, id: KeyId) -> Option<KeyRecord> {
        self.delta.key(self.base, id)
    }

    fn key_retired(&self, id: KeyId) -> bool {
        match self.delta.keys.get(&id) {
            Some(change) => change.after.is_none(),
            None => self.base.key_retired(id),
        }
    }

    fn keys_rooted_at(&self, dir: Ino) -> Vec<KeyId> {
        self.delta.keys_rooted_at(self.base, dir)
    }

    fn keys_minted_by(&self, id: KeyId) -> Vec<KeyId> {
        self.delta.keys_minted_by(self.base, id)
    }

    fn key_roots(&self) -> Vec<(KeyId, Ino)> {
        self.delta.key_roots(self.base)
    }
}
