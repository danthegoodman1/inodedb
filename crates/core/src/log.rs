//! Log entries: a commit's effects after path resolution, each with the
//! state it replaced.

use crate::auth::{KeyId, KeyRecord};
use crate::codec::{Dec, DecodeError, Enc};
use crate::delta::{Delta, Freed};
use crate::types::{Entry, Ino, Inode, Millis, Name, Version};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryEffect {
    pub dir: Ino,
    pub name: Name,
    pub before: Option<Entry>,
    pub after: Option<Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodeEffect {
    pub ino: Ino,
    pub before: Option<Inode>,
    pub after: Option<Inode>,
    /// For files, every directory holding the file before or after.
    pub holders: Vec<Ino>,
}

/// A directory's ancestors, starting with itself and ending at the root,
/// before the commit and after it. A directory the commit made has no
/// ancestors before it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Chains {
    pub before: Vec<Ino>,
    pub after: Vec<Ino>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LogEntry {
    pub version: Version,
    pub time: Millis,
    pub entries: Vec<EntryEffect>,
    pub inodes: Vec<InodeEffect>,
    pub freed: Vec<Freed>,
    pub detached: Vec<Ino>,
    /// The chains of every directory the effects name: each holding a
    /// changed entry, each changed directory, and each holder of a
    /// changed file. They route effects to subtrees as of this commit.
    pub chains: BTreeMap<Ino, Chains>,
    /// Keys minted, and keys revoked (None), so followers of the log know
    /// every key and streams end when their key goes.
    pub keys: Vec<(KeyId, Option<KeyRecord>)>,
    /// Free-queue entries acknowledged, which go with what they kept, so
    /// followers of the log forget them.
    pub acked: Vec<Freed>,
}

impl LogEntry {
    pub fn from_delta(
        delta: &Delta,
        time: Millis,
        holders: &BTreeMap<Ino, BTreeSet<Ino>>,
        chains: BTreeMap<Ino, Chains>,
    ) -> LogEntry {
        let mut entries = Vec::new();
        for (dir, names) in &delta.entries {
            for (name, change) in names {
                if change.before != change.after {
                    entries.push(EntryEffect {
                        dir: *dir,
                        name: name.clone(),
                        before: change.before,
                        after: change.after,
                    });
                }
            }
        }
        let inodes = delta
            .inodes
            .iter()
            .filter(|(_, change)| change.before != change.after)
            .map(|(ino, change)| InodeEffect {
                ino: *ino,
                before: change.before,
                after: change.after,
                holders: holders
                    .get(ino)
                    .map(|dirs| dirs.iter().copied().collect())
                    .unwrap_or_default(),
            })
            .collect();
        LogEntry {
            version: delta.version,
            time,
            entries,
            inodes,
            freed: delta.freed.clone(),
            detached: delta.detached.clone(),
            chains,
            keys: delta
                .keys
                .iter()
                .map(|(id, change)| (*id, change.after))
                .collect(),
            acked: delta.acked.iter().map(|acked| acked.freed).collect(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(64 + 32 * (self.entries.len() + self.inodes.len()));
        enc.varint(self.version).varint(self.time);
        enc.varint(self.entries.len() as u64);
        for effect in &self.entries {
            enc.varint(effect.dir).bytes(&effect.name);
            for side in [&effect.before, &effect.after] {
                match side {
                    Some(entry) => {
                        enc.u8(1);
                        entry.encode(&mut enc);
                    }
                    None => {
                        enc.u8(0);
                    }
                }
            }
        }
        enc.varint(self.inodes.len() as u64);
        for effect in &self.inodes {
            enc.varint(effect.ino);
            for side in [&effect.before, &effect.after] {
                match side {
                    Some(inode) => {
                        enc.u8(1);
                        inode.encode(&mut enc);
                    }
                    None => {
                        enc.u8(0);
                    }
                }
            }
            enc.varint(effect.holders.len() as u64);
            for holder in &effect.holders {
                enc.varint(*holder);
            }
        }
        encode_freed(&mut enc, &self.freed);
        enc.varint(self.detached.len() as u64);
        for ino in &self.detached {
            enc.varint(*ino);
        }
        enc.varint(self.chains.len() as u64);
        for (dir, chains) in &self.chains {
            // Most commits move nothing, so one chain usually serves both.
            let same = chains.before == chains.after;
            enc.varint(*dir).u8(u8::from(same));
            let sides = if same {
                &[&chains.after][..]
            } else {
                &[&chains.after, &chains.before][..]
            };
            for chain in sides {
                enc.varint(chain.len() as u64);
                for ino in chain.iter() {
                    enc.varint(*ino);
                }
            }
        }
        enc.varint(self.keys.len() as u64);
        for (id, record) in &self.keys {
            enc.be64(*id);
            match record {
                Some(record) => {
                    enc.u8(1);
                    record.encode_into(&mut enc);
                }
                None => {
                    enc.u8(0);
                }
            }
        }
        encode_freed(&mut enc, &self.acked);
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<LogEntry, DecodeError> {
        let mut dec = Dec::new(bytes);
        let version = dec.varint()?;
        let time = dec.varint()?;
        let count = bounded(dec.varint()?, bytes.len())?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let dir = dec.varint()?;
            let name = Name::from(dec.bytes()?);
            let mut sides = [None, None];
            for side in &mut sides {
                if dec.u8()? == 1 {
                    *side = Some(Entry::decode(&mut dec)?);
                }
            }
            entries.push(EntryEffect {
                dir,
                name,
                before: sides[0],
                after: sides[1],
            });
        }
        let count = bounded(dec.varint()?, bytes.len())?;
        let mut inodes = Vec::with_capacity(count);
        for _ in 0..count {
            let ino = dec.varint()?;
            let mut sides = [None, None];
            for side in &mut sides {
                if dec.u8()? == 1 {
                    *side = Some(Inode::decode(&mut dec)?);
                }
            }
            let holders_len = bounded(dec.varint()?, bytes.len())?;
            let mut holders = Vec::with_capacity(holders_len);
            for _ in 0..holders_len {
                holders.push(dec.varint()?);
            }
            inodes.push(InodeEffect {
                ino,
                before: sides[0],
                after: sides[1],
                holders,
            });
        }
        let freed = decode_freed(&mut dec, bytes.len())?;
        let count = bounded(dec.varint()?, bytes.len())?;
        let mut detached = Vec::with_capacity(count);
        for _ in 0..count {
            detached.push(dec.varint()?);
        }
        let count = bounded(dec.varint()?, bytes.len())?;
        let mut chains = BTreeMap::new();
        let chain = |dec: &mut Dec| -> Result<Vec<Ino>, DecodeError> {
            let len = bounded(dec.varint()?, bytes.len())?;
            (0..len).map(|_| dec.varint()).collect()
        };
        for _ in 0..count {
            let dir = dec.varint()?;
            let same = match dec.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError),
            };
            let after = chain(&mut dec)?;
            let before = if same {
                after.clone()
            } else {
                chain(&mut dec)?
            };
            chains.insert(dir, Chains { before, after });
        }
        let count = bounded(dec.varint()?, bytes.len())?;
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            let id = dec.be64()?;
            let record = match dec.u8()? {
                0 => None,
                1 => Some(KeyRecord::decode_from(&mut dec)?),
                _ => return Err(DecodeError),
            };
            keys.push((id, record));
        }
        let acked = decode_freed(&mut dec, bytes.len())?;
        dec.end()?;
        Ok(LogEntry {
            version,
            time,
            entries,
            inodes,
            freed,
            detached,
            chains,
            keys,
            acked,
        })
    }
}

fn encode_freed(enc: &mut Enc, freed: &[Freed]) {
    enc.varint(freed.len() as u64);
    for freed in freed {
        match *freed {
            Freed::Inode(ino) => enc.u8(0).varint(ino),
            Freed::Value(ino, version) => enc.u8(1).varint(ino).varint(version),
        };
    }
}

fn decode_freed(dec: &mut Dec, len: usize) -> Result<Vec<Freed>, DecodeError> {
    let count = bounded(dec.varint()?, len)?;
    let mut freed = Vec::with_capacity(count);
    for _ in 0..count {
        freed.push(match dec.u8()? {
            0 => Freed::Inode(dec.varint()?),
            1 => Freed::Value(dec.varint()?, dec.varint()?),
            _ => return Err(DecodeError),
        });
    }
    Ok(freed)
}

/// A count no larger than the bytes that could hold it.
fn bounded(count: u64, len: usize) -> Result<usize, DecodeError> {
    let count = usize::try_from(count).map_err(|_| DecodeError)?;
    if count > len {
        return Err(DecodeError);
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Grant;
    use crate::types::Kind;

    #[test]
    fn entries_round_trip() {
        let entry = LogEntry {
            version: 7,
            time: 1_000,
            entries: vec![EntryEffect {
                dir: 1,
                name: Name::from(&b"a"[..]),
                before: None,
                after: Some(Entry {
                    child: 9,
                    kind: Kind::File,
                    version: 7,
                }),
            }],
            inodes: vec![InodeEffect {
                ino: 9,
                before: None,
                after: Some(Inode::file(7, 1_000, 3)),
                holders: vec![1],
            }],
            freed: vec![Freed::Value(4, 2), Freed::Inode(5)],
            detached: vec![6],
            chains: BTreeMap::from([
                (
                    1,
                    Chains {
                        before: vec![1],
                        after: vec![1],
                    },
                ),
                (
                    8,
                    Chains {
                        before: vec![8, 3, 1],
                        after: vec![8, 4, 1],
                    },
                ),
                (
                    10,
                    Chains {
                        before: Vec::new(),
                        after: vec![10, 1],
                    },
                ),
            ]),
            keys: vec![
                (
                    3,
                    Some(KeyRecord {
                        hash: [9; 32],
                        root: 8,
                        grant: Grant::Read,
                        expires: 0,
                        minted_by: Some(2),
                    }),
                ),
                (4, None),
            ],
            acked: vec![Freed::Inode(2), Freed::Value(4, 1)],
        };
        assert_eq!(LogEntry::decode(&entry.encode()), Ok(entry));
    }
}
