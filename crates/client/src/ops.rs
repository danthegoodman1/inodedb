//! Ops and conditions, built as values and pushed onto a batch.

use bytes::Bytes;
use inodedb_proto as pb;

/// A predicate on the state the batch's earlier applied ops left.
#[derive(Clone, Debug)]
pub struct Cond(pub(crate) pb::Pred);

fn bytes(path: impl AsRef<[u8]>) -> Bytes {
    Bytes::copy_from_slice(path.as_ref())
}

impl Cond {
    fn of(kind: pb::pred::Kind) -> Cond {
        Cond(pb::Pred { kind: Some(kind) })
    }

    pub fn exists(path: impl AsRef<[u8]>) -> Cond {
        Cond::of(pb::pred::Kind::Exists(bytes(path)))
    }

    pub fn absent(path: impl AsRef<[u8]>) -> Cond {
        Cond::of(pb::pred::Kind::Absent(bytes(path)))
    }

    pub fn is_file(path: impl AsRef<[u8]>) -> Cond {
        Cond::of(pb::pred::Kind::IsFile(bytes(path)))
    }

    pub fn is_dir(path: impl AsRef<[u8]>) -> Cond {
        Cond::of(pb::pred::Kind::IsDir(bytes(path)))
    }

    /// The path is an empty directory.
    pub fn empty(path: impl AsRef<[u8]>) -> Cond {
        Cond::of(pb::pred::Kind::Empty(bytes(path)))
    }

    /// The path's inode carries this version.
    pub fn version_is(path: impl AsRef<[u8]>, version: u64) -> Cond {
        Cond::of(pb::pred::Kind::VersionIs(pb::VersionIs {
            path: bytes(path),
            version,
        }))
    }

    /// An earlier op in the batch applied.
    pub fn applied(op: OpRef) -> Cond {
        Cond::of(pb::pred::Kind::Applied(op.0))
    }
}

/// An op's position in its batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpRef(pub(crate) u32);

impl OpRef {
    /// The op at `index`, for batches built without `push`'s return.
    pub fn from_index(index: usize) -> OpRef {
        OpRef(index as u32)
    }

    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// One op with its condition. Modifiers that make no sense for the op's
/// kind leave it unchanged.
#[derive(Clone, Debug)]
pub struct Op(pub(crate) pb::Op);

impl Op {
    fn of(kind: pb::op::Kind) -> Op {
        Op(pb::Op {
            kind: Some(kind),
            cond: Vec::new(),
        })
    }

    pub fn mkdir(path: impl AsRef<[u8]>) -> Op {
        Op::of(pb::op::Kind::Mkdir(pb::Mkdir {
            path: bytes(path),
            parents: false,
            if_absent: false,
        }))
    }

    /// Upserts a file's value.
    pub fn put(path: impl AsRef<[u8]>, value: impl Into<Bytes>) -> Op {
        Op::of(pb::op::Kind::Put(pb::Put {
            path: bytes(path),
            value: value.into(),
            mode: pb::PutMode::Upsert as i32,
            mtime: None,
        }))
    }

    /// A hard link at `to` to the file at `from`.
    pub fn link(from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Op {
        Op::of(pb::op::Kind::Link(pb::Link {
            from: bytes(from),
            to: bytes(to),
        }))
    }

    pub fn unlink(path: impl AsRef<[u8]>) -> Op {
        Op::of(pb::op::Kind::Unlink(pb::Unlink {
            path: bytes(path),
            if_exists: false,
        }))
    }

    pub fn rmdir(path: impl AsRef<[u8]>) -> Op {
        Op::of(pb::op::Kind::Rmdir(pb::Rmdir {
            path: bytes(path),
            if_exists: false,
        }))
    }

    /// Removes a file or a whole subtree.
    pub fn remove_all(path: impl AsRef<[u8]>) -> Op {
        Op::of(pb::op::Kind::RemoveAll(pb::RemoveAll {
            path: bytes(path),
            if_exists: false,
        }))
    }

    /// Renames, replacing what `to` names as `rename(2)` does.
    pub fn rename(from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Op {
        Op::of(pb::op::Kind::Rename(pb::Rename {
            from: bytes(from),
            to: bytes(to),
            flag: pb::RenameFlag::ReplaceTarget as i32,
        }))
    }

    pub fn set_mtime(path: impl AsRef<[u8]>, mtime: u64) -> Op {
        Op::of(pb::op::Kind::SetAttr(pb::SetAttr {
            path: bytes(path),
            mtime: Some(mtime),
            mode: None,
        }))
    }

    pub fn set_mode(path: impl AsRef<[u8]>, mode: u32) -> Op {
        Op::of(pb::op::Kind::SetAttr(pb::SetAttr {
            path: bytes(path),
            mtime: None,
            mode: Some(mode),
        }))
    }

    /// `mkdir -p`: creates missing parents and succeeds when the directory
    /// exists.
    pub fn parents(mut self) -> Op {
        if let Some(pb::op::Kind::Mkdir(mkdir)) = &mut self.0.kind {
            mkdir.parents = true;
            mkdir.if_absent = true;
        }
        self
    }

    /// Succeeds when the directory already exists.
    pub fn if_absent(mut self) -> Op {
        if let Some(pb::op::Kind::Mkdir(mkdir)) = &mut self.0.kind {
            mkdir.if_absent = true;
        }
        self
    }

    /// Succeeds when nothing is there to remove.
    pub fn if_exists(mut self) -> Op {
        match &mut self.0.kind {
            Some(pb::op::Kind::Unlink(op)) => op.if_exists = true,
            Some(pb::op::Kind::Rmdir(op)) => op.if_exists = true,
            Some(pb::op::Kind::RemoveAll(op)) => op.if_exists = true,
            _ => {}
        }
        self
    }

    fn put_mode(mut self, mode: pb::PutMode) -> Op {
        if let Some(pb::op::Kind::Put(put)) = &mut self.0.kind {
            put.mode = mode as i32;
        }
        self
    }

    /// The put fails when the name exists.
    pub fn create(self) -> Op {
        self.put_mode(pb::PutMode::Create)
    }

    /// The put fails unless a file is there.
    pub fn replace(self) -> Op {
        self.put_mode(pb::PutMode::Replace)
    }

    /// The put's file takes this mtime instead of the commit's time.
    pub fn mtime(mut self, mtime: u64) -> Op {
        if let Some(pb::op::Kind::Put(put)) = &mut self.0.kind {
            put.mtime = Some(mtime);
        }
        self
    }

    fn rename_flag(mut self, flag: pb::RenameFlag) -> Op {
        if let Some(pb::op::Kind::Rename(rename)) = &mut self.0.kind {
            rename.flag = flag as i32;
        }
        self
    }

    /// The rename fails when `to` exists.
    pub fn no_replace(self) -> Op {
        self.rename_flag(pb::RenameFlag::NoReplace)
    }

    /// The rename swaps `from` and `to`, which must both exist.
    pub fn exchange(self) -> Op {
        self.rename_flag(pb::RenameFlag::Exchange)
    }

    /// The op runs only when `cond` holds; conditions join with AND.
    pub fn if_(mut self, cond: Cond) -> Op {
        self.0.cond.push(cond.0);
        self
    }

    /// The op runs only when an earlier op applied.
    pub fn if_applied(self, op: OpRef) -> Op {
        self.if_(Cond::applied(op))
    }

    /// The op as a protocol message, for callers that speak gRPC directly.
    pub fn into_pb(self) -> pb::Op {
        self.0
    }
}
