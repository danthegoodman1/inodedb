//! Views of the tree, and the reads that fill in what a view lacks.

use crate::auth::{KeyId, KeyRecord};
use crate::codec::{Dec, DecodeError};
use crate::keys::{self, KeyRange, KvRead, KvResult};
use crate::ops::CommitResult;
use crate::types::{Entry, Ino, Inode, Name, Version};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

/// Something a view lacks, named by what the core needs.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Read {
    Inode(Ino),
    Entry(Ino, Name),
    /// A directory's first `limit` entries, or all of them.
    Entries {
        dir: Ino,
        limit: Option<usize>,
    },
    /// Every link to a file.
    Backptrs(Ino),
    Value(Ino, Version),
    Request(u128),
    /// A freed inode's free-queue entry, under the version that freed it.
    Free(Version, Ino),
    /// A detached subtree's top's detach record, under the version that
    /// detached it.
    Detached(Version, Ino),
}

impl Read {
    pub fn kv(&self) -> KvRead {
        match self {
            Read::Inode(ino) => KvRead::Get(keys::inode(*ino)),
            Read::Entry(dir, name) => KvRead::Get(keys::entry(*dir, name)),
            Read::Entries { dir, limit } => KvRead::Scan {
                range: KeyRange::prefix(keys::entry_prefix(*dir)),
                limit: limit.unwrap_or(usize::MAX),
            },
            Read::Backptrs(ino) => KvRead::Scan {
                range: KeyRange::prefix(keys::backptr_prefix(*ino)),
                limit: usize::MAX,
            },
            Read::Value(ino, version) => KvRead::Get(keys::value(*ino, *version)),
            Read::Request(id) => KvRead::Scan {
                range: KeyRange::prefix(keys::request_prefix(*id)),
                limit: usize::MAX,
            },
            Read::Free(version, ino) => KvRead::Get(keys::free(*version, *ino, None)),
            Read::Detached(version, ino) => KvRead::Get(keys::detached(*version, *ino)),
        }
    }

    /// What the storage result says.
    pub fn fact(&self, result: KvResult) -> Result<Fact, DecodeError> {
        Ok(match (self, result) {
            (Read::Inode(ino), KvResult::Get(value)) => Fact::Inode(
                *ino,
                value.map(|bytes| Inode::from_bytes(&bytes)).transpose()?,
            ),
            (Read::Entry(dir, name), KvResult::Get(value)) => Fact::Entry(
                *dir,
                name.clone(),
                value.map(|bytes| Entry::from_bytes(&bytes)).transpose()?,
            ),
            (Read::Entries { dir, limit }, KvResult::Scan(pairs)) => {
                let complete = limit.is_none_or(|limit| pairs.len() < limit);
                let prefix = keys::entry_prefix(*dir).len();
                let mut entries = Vec::with_capacity(pairs.len());
                for (key, value) in pairs {
                    let name = key.get(prefix..).ok_or(DecodeError)?;
                    entries.push((Name::from(name), Some(Entry::from_bytes(&value)?)));
                }
                Fact::Listing {
                    dir: *dir,
                    entries,
                    complete,
                }
            }
            (Read::Backptrs(ino), KvResult::Scan(pairs)) => {
                let prefix = keys::backptr_prefix(*ino).len();
                let mut links = BTreeSet::new();
                for (key, _) in pairs {
                    let rest = key.get(prefix..).ok_or(DecodeError)?;
                    let mut dec = Dec::new(rest);
                    let parent = dec.be64()?;
                    links.insert((parent, Name::from(dec.rest())));
                }
                Fact::Backptrs(*ino, links)
            }
            (Read::Value(ino, version), KvResult::Get(value)) => {
                Fact::Value(*ino, *version, value.map(Bytes::from))
            }
            (Read::Request(id), KvResult::Scan(pairs)) => {
                if pairs.is_empty() {
                    Fact::Request(*id, None)
                } else {
                    let mut bytes = Vec::new();
                    for (_, part) in pairs {
                        bytes.extend_from_slice(&part);
                    }
                    Fact::Request(*id, Some(CommitResult::decode(&bytes)?))
                }
            }
            (Read::Free(version, ino), KvResult::Get(value)) => Fact::Free(
                *version,
                *ino,
                value
                    .map(|bytes| crate::delta::decode_ancestors(&bytes))
                    .transpose()?,
            ),
            (Read::Detached(version, ino), KvResult::Get(value)) => Fact::Detached(
                *version,
                *ino,
                value
                    .map(|bytes| crate::delta::decode_ancestors(&bytes))
                    .transpose()?,
            ),
            _ => return Err(DecodeError),
        })
    }
}

/// What storage held for one read, at some version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fact {
    Inode(Ino, Option<Inode>),
    Entry(Ino, Name, Option<Entry>),
    /// Names in a directory, present or known absent. A complete listing
    /// holds every present entry.
    Listing {
        dir: Ino,
        entries: Vec<(Name, Option<Entry>)>,
        complete: bool,
    },
    Backptrs(Ino, BTreeSet<(Ino, Name)>),
    Value(Ino, Version, Option<Bytes>),
    Request(u128, Option<CommitResult>),
    Free(Version, Ino, Option<Vec<Ino>>),
    Detached(Version, Ino, Option<Vec<Ino>>),
}

/// A known answer, or the read that would supply it.
pub type Lookup<T> = Result<T, Read>;

/// A view of the tree at one version. Views answer what they know and name
/// the read for what they lack.
pub trait View {
    fn inode(&self, ino: Ino) -> Lookup<Option<Inode>>;
    fn entry(&self, dir: Ino, name: &[u8]) -> Lookup<Option<Entry>>;
    /// Whether the directory has an entry whose name `removed` lacks.
    fn has_entry_outside(&self, dir: Ino, removed: &BTreeSet<Name>) -> Lookup<bool>;
    /// Every `(parent, name)` that links to a file.
    fn backptrs(&self, ino: Ino) -> Lookup<BTreeSet<(Ino, Name)>>;
    fn value(&self, ino: Ino, version: Version) -> Lookup<Option<Bytes>>;
    fn request(&self, id: u128) -> Lookup<Option<CommitResult>>;
    /// Up to `limit` of a directory's present entries in name order, from
    /// `start`, when the view holds them all.
    fn listing(&self, dir: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>>;
    /// The directories a freed inode's free-queue entry records above it,
    /// while the entry stands.
    fn free_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>>;
    /// The directories a detached subtree's top's detach record names above
    /// it, while the record stands.
    fn detach_entry(&self, version: Version, ino: Ino) -> Lookup<Option<Vec<Ino>>>;
    /// A key's record. The writer's views hold every key.
    fn key(&self, id: KeyId) -> Option<KeyRecord>;
    /// Whether a tombstone keeps this id from minting again: a deleted
    /// operator key held it. A layer of changes counts every key it
    /// deletes.
    fn key_retired(&self, id: KeyId) -> bool;
    fn keys_rooted_at(&self, dir: Ino) -> Vec<KeyId>;
    fn keys_minted_by(&self, id: KeyId) -> Vec<KeyId>;
    /// Every key, with its root.
    fn key_roots(&self) -> Vec<(KeyId, Ino)>;
}

/// A ranged listing of a layer of changes over `base`. `changes` are the
/// layer's entries for the directory from `start`, in name order; `created`
/// says the layer made the directory, so the base has none of its entries.
pub fn layered_listing<'a>(
    base: &dyn View,
    dir: Ino,
    created: bool,
    changes: impl Iterator<Item = (&'a Name, Option<Entry>)>,
    start: Bound<&[u8]>,
    limit: usize,
) -> Lookup<Vec<(Name, Entry)>> {
    let changes: Vec<(&Name, Option<Entry>)> = changes.collect();
    let (mut merged, last): (BTreeMap<Name, Entry>, Option<Name>) = if created {
        (BTreeMap::new(), None)
    } else {
        // Each removal may hide one of the base's entries.
        let removals = changes.iter().filter(|(_, entry)| entry.is_none()).count();
        let want = limit.saturating_add(removals);
        let page = base.listing(dir, start, want)?;
        // A full page may leave base entries unseen past its last name, so
        // changes beyond it wait for a later page.
        let last = if page.len() == want {
            page.last().map(|(name, _)| name.clone())
        } else {
            None
        };
        (page.into_iter().collect(), last)
    };
    for (name, entry) in changes {
        if last.as_ref().is_some_and(|last| name > last) {
            break;
        }
        match entry {
            Some(entry) => merged.insert(name.clone(), entry),
            None => merged.remove(name),
        };
    }
    Ok(merged.into_iter().take(limit).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Kind;

    /// A base holding one complete directory.
    struct Base(BTreeMap<Name, Entry>);

    impl View for Base {
        fn inode(&self, _: Ino) -> Lookup<Option<Inode>> {
            unreachable!()
        }
        fn entry(&self, _: Ino, _: &[u8]) -> Lookup<Option<Entry>> {
            unreachable!()
        }
        fn has_entry_outside(&self, _: Ino, _: &BTreeSet<Name>) -> Lookup<bool> {
            unreachable!()
        }
        fn backptrs(&self, _: Ino) -> Lookup<BTreeSet<(Ino, Name)>> {
            unreachable!()
        }
        fn value(&self, _: Ino, _: Version) -> Lookup<Option<Bytes>> {
            unreachable!()
        }
        fn request(&self, _: u128) -> Lookup<Option<CommitResult>> {
            unreachable!()
        }
        fn listing(&self, _: Ino, start: Bound<&[u8]>, limit: usize) -> Lookup<Vec<(Name, Entry)>> {
            Ok(self
                .0
                .range::<[u8], _>((start, Bound::Unbounded))
                .take(limit)
                .map(|(name, entry)| (name.clone(), *entry))
                .collect())
        }
        fn free_entry(&self, _: Version, _: Ino) -> Lookup<Option<Vec<Ino>>> {
            unreachable!()
        }
        fn detach_entry(&self, _: Version, _: Ino) -> Lookup<Option<Vec<Ino>>> {
            unreachable!()
        }
        fn key(&self, _: KeyId) -> Option<KeyRecord> {
            unreachable!()
        }
        fn key_retired(&self, _: KeyId) -> bool {
            unreachable!()
        }
        fn keys_rooted_at(&self, _: Ino) -> Vec<KeyId> {
            unreachable!()
        }
        fn keys_minted_by(&self, _: KeyId) -> Vec<KeyId> {
            unreachable!()
        }
        fn key_roots(&self) -> Vec<(KeyId, Ino)> {
            unreachable!()
        }
    }

    fn entry(child: Ino) -> Entry {
        Entry {
            child,
            kind: Kind::File,
            version: 1,
        }
    }

    /// Every small base and layer over names `a` to `e`, listed from every
    /// start with every limit, against merging the whole directory.
    #[test]
    fn layered_listings_match_merging_everything() {
        let names: Vec<Name> = [b"a", b"b", b"c", b"d", b"e"]
            .iter()
            .map(|name| Name::from(&name[..]))
            .collect();
        for base_mask in 0..32u32 {
            // Each name in the layer is untouched, removed, or written.
            for layer_code in 0..3u32.pow(5) {
                let base: BTreeMap<Name, Entry> = names
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| base_mask & (1 << index) != 0)
                    .map(|(index, name)| (name.clone(), entry(index as Ino)))
                    .collect();
                let mut layer: BTreeMap<Name, Option<Entry>> = BTreeMap::new();
                let mut code = layer_code;
                for (index, name) in names.iter().enumerate() {
                    match code % 3 {
                        1 => {
                            layer.insert(name.clone(), None);
                        }
                        2 => {
                            layer.insert(name.clone(), Some(entry(10 + index as Ino)));
                        }
                        _ => {}
                    }
                    code /= 3;
                }
                let mut whole = base.clone();
                for (name, change) in &layer {
                    match change {
                        Some(entry) => whole.insert(name.clone(), *entry),
                        None => whole.remove(name),
                    };
                }
                let view = Base(base);
                let starts: Vec<Bound<&[u8]>> = std::iter::once(Bound::Unbounded)
                    .chain(names.iter().map(|name| Bound::Included(&name[..])))
                    .chain(names.iter().map(|name| Bound::Excluded(&name[..])))
                    .collect();
                for start in starts {
                    for limit in 0..=6 {
                        let expected: Vec<(Name, Entry)> = whole
                            .range::<[u8], _>((start, Bound::Unbounded))
                            .take(limit)
                            .map(|(name, entry)| (name.clone(), *entry))
                            .collect();
                        let changes = layer
                            .range::<[u8], _>((start, Bound::Unbounded))
                            .map(|(name, change)| (name, *change));
                        let got = layered_listing(&view, 1, false, changes, start, limit)
                            .expect("the base holds everything");
                        assert_eq!(
                            got, expected,
                            "base {base_mask:05b} layer {layer_code} {start:?} limit {limit}"
                        );
                    }
                }
            }
        }
    }
}
