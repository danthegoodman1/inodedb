//! Keys: which subtree a request reaches and what it may do there.
//!
//! A key's record holds a hash of its secret, the directory it is rooted
//! at, its grant, when it expires, and the key that minted it. Keys are
//! few, so the writer holds every record from startup on, and a commit
//! that frees or detaches a key's root revokes the key in the same commit.

use crate::codec::{Dec, DecodeError, Enc};
use crate::types::{Error, Ino, Millis, Path};
use crate::view::View;
use std::collections::{BTreeMap, BTreeSet};

pub type KeyId = u64;

/// The most keys a store holds, so revoking any of them, with what it
/// minted, fits one commit.
pub const MAX_KEYS: usize = 100_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Grant {
    /// `Stat`, `Read`, `List`, `Watch` and leases.
    Read,
    /// Adds `Commit` and the free queue.
    Write,
    /// Adds minting and revoking keys.
    Admin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRecord {
    /// SHA-256 of the secret.
    pub hash: [u8; 32],
    pub root: Ino,
    pub grant: Grant,
    /// When the key stops working; zero never.
    pub expires: Millis,
    pub minted_by: Option<KeyId>,
}

impl KeyRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(56);
        self.encode_into(&mut enc);
        enc.finish()
    }

    pub fn encode_into(&self, enc: &mut Enc) {
        enc.raw(&self.hash)
            .varint(self.root)
            .u8(self.grant as u8)
            .varint(self.expires);
        match self.minted_by {
            Some(id) => enc.u8(1).be64(id),
            None => enc.u8(0),
        };
    }

    pub fn decode(bytes: &[u8]) -> Result<KeyRecord, DecodeError> {
        let mut dec = Dec::new(bytes);
        let record = KeyRecord::decode_from(&mut dec)?;
        dec.end()?;
        Ok(record)
    }

    pub fn decode_from(dec: &mut Dec) -> Result<KeyRecord, DecodeError> {
        let hash = dec.take(32)?.try_into().map_err(|_| DecodeError)?;
        let root = dec.varint()?;
        let grant = match dec.u8()? {
            0 => Grant::Read,
            1 => Grant::Write,
            2 => Grant::Admin,
            _ => return Err(DecodeError),
        };
        let expires = dec.varint()?;
        let minted_by = match dec.u8()? {
            0 => None,
            1 => Some(dec.be64()?),
            _ => return Err(DecodeError),
        };
        Ok(KeyRecord {
            hash,
            root,
            grant,
            expires,
            minted_by,
        })
    }

    pub fn live(&self, now: Millis) -> bool {
        self.expires == 0 || now < self.expires
    }
}

/// Who sends a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Caller {
    /// A key, with the hash of the secret presented for it.
    Key { id: KeyId, hash: [u8; 32] },
    /// The operator, with every grant, rooted at a directory: bootstrap
    /// tools and tests. The server builds callers from requests' keys
    /// alone.
    Trusted(Ino),
}

/// Why a request was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denied {
    /// No key matches the id and secret: it never existed, or was revoked.
    Unknown,
    Expired,
    /// The key lacks the grant the request needs.
    Grant,
}

/// What a caller reaches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scope {
    pub root: Ino,
    pub grant: Grant,
    /// Zero never expires.
    pub expires: Millis,
    /// None for a trusted caller.
    pub key: Option<KeyId>,
}

/// The scope of a caller holding `need`, under the keys `view` holds.
pub fn authorize(
    view: &dyn View,
    caller: &Caller,
    now: Millis,
    need: Grant,
) -> Result<Scope, Denied> {
    match *caller {
        Caller::Trusted(root) => Ok(Scope {
            root,
            grant: Grant::Admin,
            expires: 0,
            key: None,
        }),
        Caller::Key { id, hash } => {
            let record = view.key(id).ok_or(Denied::Unknown)?;
            if !same(&record.hash, &hash) {
                return Err(Denied::Unknown);
            }
            if !record.live(now) {
                return Err(Denied::Expired);
            }
            if record.grant < need {
                return Err(Denied::Grant);
            }
            Ok(Scope {
                root: record.root,
                grant: record.grant,
                expires: record.expires,
                key: Some(id),
            })
        }
    }
}

/// Compares in time independent of where the inputs differ.
fn same(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// A change to the keys, which needs `Admin`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyOp {
    /// A key rooted at a directory under the caller's root. Minting needs
    /// `Admin`, so any grant is within the caller's. It expires `ttl` after
    /// it is minted, or with the caller's key if that comes first; a zero
    /// `ttl` expires with the caller's key.
    Mint {
        id: KeyId,
        hash: [u8; 32],
        root: Path,
        grant: Grant,
        ttl: Millis,
    },
    /// Revokes the key and every key minted from it. A key may revoke
    /// itself and the keys minted from it.
    Revoke { id: KeyId },
}

/// Why a key op failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyError {
    /// The root path didn't resolve to a directory.
    Fs(Error),
    /// The id is taken, or belonged to a revoked key.
    Exists,
    /// The store holds `MAX_KEYS` keys.
    TooMany,
    /// No key the caller may revoke has the id.
    NotFound,
}

/// Every key record, indexed by root and by minter, and the ids of keys
/// revoked, which never mint again.
#[derive(Clone, Debug, Default)]
pub struct Keys {
    records: BTreeMap<KeyId, KeyRecord>,
    by_root: BTreeMap<Ino, BTreeSet<KeyId>>,
    by_minter: BTreeMap<KeyId, BTreeSet<KeyId>>,
    retired: BTreeSet<KeyId>,
}

impl Keys {
    pub fn get(&self, id: KeyId) -> Option<KeyRecord> {
        self.records.get(&id).copied()
    }

    pub fn put(&mut self, id: KeyId, record: Option<KeyRecord>) {
        if let Some(old) = self.records.remove(&id) {
            unindex(&mut self.by_root, old.root, id);
            if let Some(minter) = old.minted_by {
                unindex(&mut self.by_minter, minter, id);
            }
            if record.is_none() {
                self.retired.insert(id);
            }
        }
        if let Some(record) = record {
            self.by_root.entry(record.root).or_default().insert(id);
            if let Some(minter) = record.minted_by {
                self.by_minter.entry(minter).or_default().insert(id);
            }
            self.records.insert(id, record);
        }
    }

    /// Marks an id revoked, as its tombstone says.
    pub fn retire(&mut self, id: KeyId) {
        self.retired.insert(id);
    }

    pub fn retired(&self, id: KeyId) -> bool {
        self.retired.contains(&id)
    }

    pub fn rooted_at(&self, dir: Ino) -> Vec<KeyId> {
        self.by_root
            .get(&dir)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn minted_by(&self, id: KeyId) -> Vec<KeyId> {
        self.by_minter
            .get(&id)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn roots(&self) -> Vec<(KeyId, Ino)> {
        self.records
            .iter()
            .map(|(id, record)| (*id, record.root))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (KeyId, &KeyRecord)> {
        self.records.iter().map(|(id, record)| (*id, record))
    }
}

fn unindex<K: Ord>(index: &mut BTreeMap<K, BTreeSet<KeyId>>, at: K, id: KeyId) {
    if let Some(ids) = index.get_mut(&at) {
        ids.remove(&id);
        if ids.is_empty() {
            index.remove(&at);
        }
    }
}

/// The keys a layer of changes leaves matching `matches`: `base`'s,
/// less those the layer changed, plus the layer's records that match.
pub fn layered<'a>(
    base: Vec<KeyId>,
    layer: impl Iterator<Item = (KeyId, Option<&'a KeyRecord>)>,
    matches: impl Fn(&KeyRecord) -> bool,
) -> Vec<KeyId> {
    let mut changed = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for (id, record) in layer {
        changed.insert(id);
        if record.is_some_and(&matches) {
            ids.insert(id);
        }
    }
    ids.extend(base.into_iter().filter(|id| !changed.contains(id)));
    ids.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(root: Ino, minted_by: Option<KeyId>) -> KeyRecord {
        KeyRecord {
            hash: [7; 32],
            root,
            grant: Grant::Write,
            expires: 99,
            minted_by,
        }
    }

    #[test]
    fn records_round_trip() {
        for record in [record(5, None), record(1, Some(u64::MAX))] {
            assert_eq!(KeyRecord::decode(&record.encode()), Ok(record));
        }
    }

    #[test]
    fn indexes_follow_puts() {
        let mut keys = Keys::default();
        keys.put(1, Some(record(5, None)));
        keys.put(2, Some(record(5, Some(1))));
        keys.put(3, Some(record(6, Some(1))));
        assert_eq!(keys.rooted_at(5), vec![1, 2]);
        assert_eq!(keys.minted_by(1), vec![2, 3]);
        keys.put(2, None);
        assert_eq!(keys.rooted_at(5), vec![1]);
        assert_eq!(keys.minted_by(1), vec![3]);
    }

    #[test]
    fn a_layer_hides_what_it_revoked_and_adds_what_it_minted() {
        let minted = record(5, None);
        let ids = layered(
            vec![1, 2],
            [(2, None), (3, Some(&minted))].into_iter(),
            |record| record.root == 5,
        );
        assert_eq!(ids, vec![1, 3]);
    }
}
