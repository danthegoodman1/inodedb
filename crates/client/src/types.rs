//! What the server answers, as Rust types.

use bytes::Bytes;
use inodedb_proto as pb;

pub type Version = u64;

/// Why a path or op failed, as the matching `errno` would say.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsError {
    NotFound,
    Exists,
    NotDir,
    IsDir,
    NotEmpty,
    Invalid,
    TooLarge,
}

impl FsError {
    pub(crate) fn from_pb(code: i32) -> FsError {
        match pb::Error::try_from(code) {
            Ok(pb::Error::NotFound) => FsError::NotFound,
            Ok(pb::Error::Exists) => FsError::Exists,
            Ok(pb::Error::NotDir) => FsError::NotDir,
            Ok(pb::Error::IsDir) => FsError::IsDir,
            Ok(pb::Error::NotEmpty) => FsError::NotEmpty,
            Ok(pb::Error::TooLarge) => FsError::TooLarge,
            _ => FsError::Invalid,
        }
    }
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FsError::NotFound => "no such file or directory",
            FsError::Exists => "file exists",
            FsError::NotDir => "not a directory",
            FsError::IsDir => "is a directory",
            FsError::NotEmpty => "directory not empty",
            FsError::Invalid => "invalid argument",
            FsError::TooLarge => "value too large",
        })
    }
}

/// Why the server refused a whole batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// An op or predicate the server can't read, or an `Applied`
    /// predicate naming an op at or after its own.
    Malformed,
    /// The batch holds more than 10,000 ops.
    TooManyOps,
    /// The batch writes more than 8 MiB.
    TooLarge,
}

#[derive(Debug)]
pub enum Error {
    /// The path or handle failed as a filesystem call would.
    Fs(FsError),
    /// The server refused the batch as a whole.
    Rejected(Rejection),
    /// The server refused the request, or retries ran out.
    Status(tonic::Status),
    /// The lease ran out; the reader drops its handles and reloads.
    LeaseExpired,
    /// The snapshot ended: its undo records outgrew the cache's budget,
    /// the cache started over, or what the read needs left the feed's
    /// sight without the cache holding it.
    SnapshotTooOld,
    /// A watch asked for a version the log no longer holds.
    Compacted,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Fs(error) => error.fmt(f),
            Error::Rejected(rejection) => write!(f, "batch rejected: {rejection:?}"),
            Error::Status(status) => write!(f, "{}: {}", status.code(), status.message()),
            Error::LeaseExpired => write!(f, "the lease ran out"),
            Error::SnapshotTooOld => write!(f, "the snapshot is too old"),
            Error::Compacted => write!(f, "the log no longer holds that version"),
        }
    }
}

impl std::error::Error for Error {}

impl From<FsError> for Error {
    fn from(error: FsError) -> Error {
        Error::Fs(error)
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Error {
        Error::Status(status)
    }
}

/// An inode, as the key that received it may address it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handle {
    pub ino: u64,
    mac: Bytes,
}

impl Handle {
    pub(crate) fn from_pb(handle: Option<pb::Handle>) -> Handle {
        let handle = handle.unwrap_or_default();
        Handle {
            ino: handle.ino,
            mac: handle.mac,
        }
    }

    pub(crate) fn to_pb(&self) -> pb::Handle {
        pb::Handle {
            ino: self.ino,
            mac: self.mac.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Dir,
    File,
}

impl Kind {
    fn from_pb(kind: i32) -> Kind {
        match pb::Kind::try_from(kind) {
            Ok(pb::Kind::File) => Kind::File,
            _ => Kind::Dir,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Life {
    Live,
    /// Unreachable by path; freed once no lease holds it.
    Detached,
    Freed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inode {
    pub handle: Handle,
    pub kind: Kind,
    pub version: Version,
    pub mtime: u64,
    pub mode: u32,
    /// Files: the number of entries naming the file.
    pub nlink: u32,
    /// Files: the value's length.
    pub size: u64,
    /// The version a file's current value is stored under.
    pub value_version: Version,
    pub life: Life,
}

impl Inode {
    pub(crate) fn from_pb(mut inode: pb::Inode) -> Inode {
        let handle = Handle::from_pb(inode.handle.take());
        Inode::under(handle, inode)
    }

    /// An inode the server sent without its handle, inside a message that
    /// carries it.
    pub(crate) fn under(handle: Handle, inode: pb::Inode) -> Inode {
        Inode {
            handle,
            kind: Kind::from_pb(inode.kind),
            version: inode.version,
            mtime: inode.mtime,
            mode: inode.mode,
            nlink: inode.nlink,
            size: inode.size,
            value_version: inode.value_version,
            life: match pb::Life::try_from(inode.life) {
                Ok(pb::Life::Detached) => Life::Detached,
                Ok(pb::Life::Freed) => Life::Freed,
                _ => Life::Live,
            },
        }
    }

    pub fn is_dir(&self) -> bool {
        self.kind == Kind::Dir
    }
}

/// A file's value with its inode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value {
    pub inode: Inode,
    pub value: Bytes,
}

/// One directory entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub name: Bytes,
    pub kind: Kind,
    pub handle: Handle,
    /// The version that created the entry.
    pub entry_version: Version,
    pub inode: Option<Inode>,
    pub value: Option<Bytes>,
}

impl Entry {
    pub(crate) fn from_pb(listed: pb::Listed) -> Entry {
        let handle = Handle::from_pb(listed.handle);
        Entry {
            name: listed.name,
            kind: Kind::from_pb(listed.kind),
            inode: listed
                .inode
                .map(|inode| Inode::under(handle.clone(), inode)),
            handle,
            entry_version: listed.entry_version,
            value: listed.value,
        }
    }

    pub fn name_lossy(&self) -> String {
        String::from_utf8_lossy(&self.name).into_owned()
    }
}

/// A directory's entries in name order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listing {
    /// Every entry reflects at least this version. Pages after the first
    /// may reflect later ones, as `readdir` may.
    pub version: Version,
    /// The newest page's version; equal to `version` when the listing
    /// reflects one version.
    pub newest: Version,
    /// The directory listed. Every page lists the directory the first
    /// page did, wherever renames take it.
    pub dir: Handle,
    pub entries: Vec<Entry>,
}

/// Why an op was skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    /// The batch's `when` predicate at this index failed.
    When(usize),
    /// The op's own predicate at this index failed.
    Cond(usize),
    Error(FsError),
    /// Another op in an all-or-nothing batch was skipped.
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The op applied; ops that create or name an inode return its handle.
    Applied(Option<Handle>),
    Skipped(Skip),
}

impl Outcome {
    pub(crate) fn from_pb(outcome: pb::Outcome) -> Outcome {
        match outcome.kind {
            Some(pb::outcome::Kind::Applied(applied)) => {
                Outcome::Applied(applied.handle.map(|handle| Handle::from_pb(Some(handle))))
            }
            Some(pb::outcome::Kind::Skipped(skipped)) => Outcome::Skipped(match skipped.reason {
                Some(pb::skipped::Reason::When(index)) => Skip::When(index as usize),
                Some(pb::skipped::Reason::Cond(index)) => Skip::Cond(index as usize),
                Some(pb::skipped::Reason::Error(code)) => Skip::Error(FsError::from_pb(code)),
                Some(pb::skipped::Reason::Aborted(_)) | None => Skip::Aborted,
            }),
            None => Outcome::Skipped(Skip::Aborted),
        }
    }

    pub fn applied(&self) -> bool {
        matches!(self, Outcome::Applied(_))
    }
}

/// A batch's result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    /// The commit's version, or the version a batch that changed nothing saw.
    pub version: Version,
    /// The batch changed something and took a version.
    pub committed: bool,
    pub outcomes: Vec<Outcome>,
}

impl Commit {
    pub fn applied(&self, op: crate::OpRef) -> bool {
        self.outcomes.get(op.index()).is_some_and(Outcome::applied)
    }

    pub fn all_applied(&self) -> bool {
        self.outcomes.iter().all(Outcome::applied)
    }

    /// The handle an applied op returned.
    pub fn handle(&self, op: crate::OpRef) -> Option<&Handle> {
        match self.outcomes.get(op.index()) {
            Some(Outcome::Applied(handle)) => handle.as_ref(),
            _ => None,
        }
    }
}

/// What a read names: a path from the key's root, or a handle.
#[derive(Clone, Debug)]
pub enum Target {
    Path(Bytes),
    Handle(Handle),
}

impl Target {
    /// The target as a protocol message, for callers that speak gRPC
    /// directly.
    pub fn into_pb(self) -> pb::Target {
        self.to_pb()
    }

    pub(crate) fn to_pb(&self) -> pb::Target {
        pb::Target {
            kind: Some(match self {
                Target::Path(path) => pb::target::Kind::Path(path.clone()),
                Target::Handle(handle) => pb::target::Kind::Handle(handle.to_pb()),
            }),
        }
    }
}

impl From<&str> for Target {
    fn from(path: &str) -> Target {
        Target::Path(Bytes::copy_from_slice(path.as_bytes()))
    }
}

impl From<&String> for Target {
    fn from(path: &String) -> Target {
        Target::from(path.as_str())
    }
}

impl From<String> for Target {
    fn from(path: String) -> Target {
        Target::Path(Bytes::from(path))
    }
}

impl From<&[u8]> for Target {
    fn from(path: &[u8]) -> Target {
        Target::Path(Bytes::copy_from_slice(path))
    }
}

impl From<Bytes> for Target {
    fn from(path: Bytes) -> Target {
        Target::Path(path)
    }
}

impl From<Handle> for Target {
    fn from(handle: Handle) -> Target {
        Target::Handle(handle)
    }
}

impl From<&Handle> for Target {
    fn from(handle: &Handle) -> Target {
        Target::Handle(handle.clone())
    }
}

/// A whole store's check, as of one version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checked {
    pub version: Version,
    /// How many keys the check read.
    pub keys: u64,
    /// Each invariant the store breaks; none for a sound store.
    pub problems: Vec<String>,
}

/// What one commit changed inside a watched subtree. A change with no
/// effects reports progress: nothing in the subtree changed up to
/// `version`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changed {
    pub version: Version,
    /// The writer's clock when it committed, in milliseconds.
    pub time: u64,
    pub entries: Vec<EntryChanged>,
    pub inodes: Vec<InodeChanged>,
    /// Directories renamed into the subtree, whose contents the watcher has
    /// never seen.
    pub entered: Vec<Handle>,
}

/// A name a commit added, replaced or removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryChanged {
    pub dir: Handle,
    pub name: Bytes,
    pub before: Option<Entry>,
    pub after: Option<Entry>,
}

/// An inode a commit created, changed or freed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodeChanged {
    pub before: Option<Inode>,
    pub after: Option<Inode>,
}

impl Changed {
    pub(crate) fn from_pb(change: pb::Change) -> Changed {
        Changed {
            version: change.version,
            time: change.time,
            entries: change
                .entries
                .into_iter()
                .map(|effect| EntryChanged {
                    dir: Handle::from_pb(effect.dir),
                    name: effect.name,
                    before: effect.before.map(Entry::from_pb),
                    after: effect.after.map(Entry::from_pb),
                })
                .collect(),
            inodes: change
                .inodes
                .into_iter()
                .map(|effect| {
                    let handle = Handle::from_pb(effect.handle);
                    InodeChanged {
                        before: effect
                            .before
                            .map(|inode| Inode::under(handle.clone(), inode)),
                        after: effect.after.map(|inode| Inode::under(handle, inode)),
                    }
                })
                .collect(),
            entered: change
                .entered
                .into_iter()
                .map(|handle| Handle::from_pb(Some(handle)))
                .collect(),
        }
    }
}
