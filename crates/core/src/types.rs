//! Inodes, entries, names and paths.

use crate::codec::{Dec, DecodeError, Enc};

/// An inode's id. Ids are never reused.
pub type Ino = u64;

/// A commit version, from the writer's counter. Version 0 precedes every
/// commit.
pub type Version = u64;

/// Milliseconds since the Unix epoch.
pub type Millis = u64;

/// A directory entry's name: 1 to 255 bytes, neither `/` nor NUL, and
/// neither `.` nor `..`.
pub type Name = Box<[u8]>;

/// The tree's root directory.
pub const ROOT: Ino = 1;

pub const MAX_NAME: usize = 255;
pub const MAX_PATH: usize = 4096;
pub const MAX_VALUE: usize = 64 * 1024;
pub const MAX_OPS: usize = 10_000;
/// Everything a batch writes, counted as its encoded keys and values.
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Dir,
    File,
}

/// Where an inode stands. A detached directory sits under a `RemoveAll`
/// awaiting the background job; a freed inode waits in the free queue until
/// its entry is acknowledged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Life {
    Live,
    Detached(Version),
    Freed(Version),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Inode {
    pub kind: Kind,
    /// The commit version of the record's last change.
    pub version: Version,
    pub mtime: Millis,
    pub mode: u32,
    /// Files: the number of entries naming the file.
    pub nlink: u32,
    /// Files: the current value's length.
    pub size: u64,
    /// Files: the version under which the current value is stored.
    pub value: Version,
    /// Directories: the directory holding this one's entry. The root is its
    /// own parent.
    pub parent: Ino,
    pub life: Life,
}

impl Inode {
    pub fn dir(version: Version, mtime: Millis, parent: Ino) -> Inode {
        Inode {
            kind: Kind::Dir,
            version,
            mtime,
            mode: 0,
            nlink: 1,
            size: 0,
            value: 0,
            parent,
            life: Life::Live,
        }
    }

    pub fn file(version: Version, mtime: Millis, size: u64) -> Inode {
        Inode {
            kind: Kind::File,
            version,
            mtime,
            mode: 0,
            nlink: 1,
            size,
            value: version,
            parent: 0,
            life: Life::Live,
        }
    }

    pub fn is_dir(&self) -> bool {
        self.kind == Kind::Dir
    }

    pub fn is_live(&self) -> bool {
        self.life == Life::Live
    }

    pub fn encode(&self, enc: &mut Enc) {
        enc.u8(match self.kind {
            Kind::Dir => 0,
            Kind::File => 1,
        });
        enc.varint(self.version)
            .varint(self.mtime)
            .varint(u64::from(self.mode));
        match self.life {
            Life::Live => enc.u8(0),
            Life::Detached(at) => enc.u8(1).varint(at),
            Life::Freed(at) => enc.u8(2).varint(at),
        };
        match self.kind {
            Kind::Dir => enc.varint(self.parent),
            Kind::File => enc
                .varint(u64::from(self.nlink))
                .varint(self.size)
                .varint(self.value),
        };
    }

    pub fn decode(dec: &mut Dec<'_>) -> Result<Inode, DecodeError> {
        let kind = match dec.u8()? {
            0 => Kind::Dir,
            1 => Kind::File,
            _ => return Err(DecodeError),
        };
        let version = dec.varint()?;
        let mtime = dec.varint()?;
        let mode = dec.varint32()?;
        let life = match dec.u8()? {
            0 => Life::Live,
            1 => Life::Detached(dec.varint()?),
            2 => Life::Freed(dec.varint()?),
            _ => return Err(DecodeError),
        };
        let mut inode = Inode {
            kind,
            version,
            mtime,
            mode,
            nlink: 1,
            size: 0,
            value: 0,
            parent: 0,
            life,
        };
        match kind {
            Kind::Dir => inode.parent = dec.varint()?,
            Kind::File => {
                inode.nlink = dec.varint32()?;
                inode.size = dec.varint()?;
                inode.value = dec.varint()?;
            }
        }
        Ok(inode)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(24);
        self.encode(&mut enc);
        enc.finish()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Inode, DecodeError> {
        let mut dec = Dec::new(bytes);
        let inode = Inode::decode(&mut dec)?;
        dec.end()?;
        Ok(inode)
    }
}

/// A directory entry's target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    pub child: Ino,
    pub kind: Kind,
    /// The version that created the entry.
    pub version: Version,
}

impl Entry {
    pub fn encode(&self, enc: &mut Enc) {
        enc.varint(self.child)
            .u8(match self.kind {
                Kind::Dir => 0,
                Kind::File => 1,
            })
            .varint(self.version);
    }

    pub fn decode(dec: &mut Dec<'_>) -> Result<Entry, DecodeError> {
        let child = dec.varint()?;
        let kind = match dec.u8()? {
            0 => Kind::Dir,
            1 => Kind::File,
            _ => return Err(DecodeError),
        };
        let version = dec.varint()?;
        Ok(Entry {
            child,
            kind,
            version,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(12);
        self.encode(&mut enc);
        enc.finish()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Entry, DecodeError> {
        let mut dec = Dec::new(bytes);
        let entry = Entry::decode(&mut dec)?;
        dec.end()?;
        Ok(entry)
    }
}

pub fn valid_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && name != b"."
        && name != b".."
        && !name.iter().any(|&byte| byte == b'/' || byte == 0)
}

/// A path's names, resolved from a key's root. A leading `/` names that
/// root; empty components, `.` and `..` are malformed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Path {
    pub names: Vec<Name>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BadPath;

impl Path {
    pub fn root() -> Path {
        Path::default()
    }

    pub fn parse(text: &[u8]) -> Result<Path, BadPath> {
        if text.len() > MAX_PATH {
            return Err(BadPath);
        }
        let text = text.strip_prefix(b"/").unwrap_or(text);
        if text.is_empty() {
            return Ok(Path::root());
        }
        let mut names = Vec::new();
        for name in text.split(|&byte| byte == b'/') {
            if !valid_name(name) {
                return Err(BadPath);
            }
            names.push(Name::from(name));
        }
        Ok(Path { names })
    }

    pub fn is_root(&self) -> bool {
        self.names.is_empty()
    }

    /// The parent's names and the last name, or None for the root.
    pub fn split_last(&self) -> Option<(&[Name], &[u8])> {
        let (last, parent) = self.names.split_last()?;
        Some((parent, last))
    }

    pub fn display(&self) -> String {
        let mut text = String::new();
        for name in &self.names {
            text.push('/');
            text.push_str(&String::from_utf8_lossy(name));
        }
        if text.is_empty() {
            text.push('/');
        }
        text
    }
}

impl std::str::FromStr for Path {
    type Err = BadPath;

    fn from_str(text: &str) -> Result<Path, BadPath> {
        Path::parse(text.as_bytes())
    }
}

/// Why an op failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Error {
    NotFound,
    Exists,
    NotDir,
    IsDir,
    NotEmpty,
    Invalid,
    TooLarge,
}

impl Error {
    pub fn code(self) -> u8 {
        match self {
            Error::NotFound => 1,
            Error::Exists => 2,
            Error::NotDir => 3,
            Error::IsDir => 4,
            Error::NotEmpty => 5,
            Error::Invalid => 6,
            Error::TooLarge => 7,
        }
    }

    pub fn from_code(code: u8) -> Option<Error> {
        Some(match code {
            1 => Error::NotFound,
            2 => Error::Exists,
            3 => Error::NotDir,
            4 => Error::IsDir,
            5 => Error::NotEmpty,
            6 => Error::Invalid,
            7 => Error::TooLarge,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_rules() {
        assert!(valid_name(b"a"));
        assert!(valid_name(&[b'x'; 255]));
        assert!(!valid_name(&[b'x'; 256]));
        assert!(!valid_name(b""));
        assert!(!valid_name(b"."));
        assert!(!valid_name(b".."));
        assert!(!valid_name(b"a/b"));
        assert!(!valid_name(b"a\0b"));
        assert!(valid_name(b"..."));
    }

    #[test]
    fn paths_parse() {
        assert_eq!(Path::parse(b"").unwrap(), Path::root());
        assert_eq!(Path::parse(b"/").unwrap(), Path::root());
        assert_eq!(Path::parse(b"/a/b").unwrap().names.len(), 2);
        assert_eq!(Path::parse(b"a/b").unwrap(), Path::parse(b"/a/b").unwrap());
        assert!(Path::parse(b"a//b").is_err());
        assert!(Path::parse(b"a/./b").is_err());
        assert!(Path::parse(b"a/../b").is_err());
        assert!(Path::parse(b"a/").is_err());
        assert!(Path::parse(&vec![b'a'; MAX_PATH + 1]).is_err());
    }

    #[test]
    fn records_round_trip() {
        let mut dir = Inode::dir(9, 1_000, 4);
        dir.life = Life::Detached(12);
        assert_eq!(Inode::from_bytes(&dir.to_bytes()), Ok(dir));
        let mut file = Inode::file(3, 77, 4096);
        file.nlink = 2;
        file.mode = 0o644;
        file.life = Life::Freed(5);
        assert_eq!(Inode::from_bytes(&file.to_bytes()), Ok(file));
        let entry = Entry {
            child: 42,
            kind: Kind::File,
            version: 8,
        };
        assert_eq!(Entry::from_bytes(&entry.to_bytes()), Ok(entry));
    }
}
