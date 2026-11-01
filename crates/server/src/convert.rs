//! Between the protocol's messages and the core's types.

use crate::auth::Scope;
use inodedb_core::ops::{
    Batch, CommitResult, Op, OpSpec, Outcome, Pred, PredSource, PutMode, RenameFlag, Skip,
};
use inodedb_core::query::{Listed, Query, QueryResult, Target};
use inodedb_core::types::{Error, Inode, Kind, Life, Path};
use inodedb_proto as pb;
use tonic::Status;

pub fn path(bytes: &[u8]) -> Result<Path, Status> {
    Path::parse(bytes).map_err(|_| {
        Status::invalid_argument(format!(
            "malformed path {:?}",
            String::from_utf8_lossy(bytes)
        ))
    })
}

fn pred(pred: pb::Pred) -> Result<Pred, Status> {
    use pb::pred::Kind as P;
    Ok(
        match pred
            .kind
            .ok_or_else(|| Status::invalid_argument("empty predicate"))?
        {
            P::Exists(p) => Pred::Exists(path(&p)?),
            P::Absent(p) => Pred::Absent(path(&p)?),
            P::IsFile(p) => Pred::IsFile(path(&p)?),
            P::IsDir(p) => Pred::IsDir(path(&p)?),
            P::Empty(p) => Pred::Empty(path(&p)?),
            P::VersionIs(v) => Pred::VersionIs(path(&v.path)?, v.version),
            P::Applied(index) => Pred::Applied(index as usize),
        },
    )
}

fn op(op: pb::Op) -> Result<OpSpec, Status> {
    use pb::op::Kind as K;
    let cond = op.cond.into_iter().map(pred).collect::<Result<_, _>>()?;
    let op = match op
        .kind
        .ok_or_else(|| Status::invalid_argument("empty op"))?
    {
        K::Mkdir(m) => Op::Mkdir {
            path: path(&m.path)?,
            parents: m.parents,
            if_absent: m.if_absent,
        },
        K::Put(p) => Op::Put {
            path: path(&p.path)?,
            value: p.value,
            mode: match pb::PutMode::try_from(p.mode)
                .map_err(|_| Status::invalid_argument("put mode"))?
            {
                pb::PutMode::Upsert => PutMode::Upsert,
                pb::PutMode::Create => PutMode::Create,
                pb::PutMode::Replace => PutMode::Replace,
            },
            mtime: p.mtime,
        },
        K::Link(l) => Op::Link {
            from: path(&l.from)?,
            to: path(&l.to)?,
        },
        K::Unlink(u) => Op::Unlink {
            path: path(&u.path)?,
            if_exists: u.if_exists,
        },
        K::Rmdir(r) => Op::Rmdir {
            path: path(&r.path)?,
            if_exists: r.if_exists,
        },
        K::RemoveAll(r) => Op::RemoveAll {
            path: path(&r.path)?,
            if_exists: r.if_exists,
        },
        K::Rename(r) => Op::Rename {
            from: path(&r.from)?,
            to: path(&r.to)?,
            flag: match pb::RenameFlag::try_from(r.flag)
                .map_err(|_| Status::invalid_argument("rename flag"))?
            {
                pb::RenameFlag::ReplaceTarget => RenameFlag::Replace,
                pb::RenameFlag::NoReplace => RenameFlag::NoReplace,
                pb::RenameFlag::Exchange => RenameFlag::Exchange,
            },
        },
        K::SetAttr(s) => Op::SetAttr {
            path: path(&s.path)?,
            mtime: s.mtime,
            mode: s.mode,
        },
    };
    Ok(OpSpec { op, cond })
}

pub fn batch(request: pb::CommitRequest) -> Result<Batch, Status> {
    let request_id = match request.request_id.len() {
        0 => 0,
        16 => u128::from_be_bytes(request.request_id[..].try_into().expect("16 bytes")),
        _ => return Err(Status::invalid_argument("request ids are 16 bytes")),
    };
    Ok(Batch {
        ops: request.ops.into_iter().map(op).collect::<Result<_, _>>()?,
        when: request
            .when
            .into_iter()
            .map(pred)
            .collect::<Result<_, _>>()?,
        all_or_nothing: request.all_or_nothing,
        request_id,
    })
}

pub fn error(error: Error) -> pb::Error {
    match error {
        Error::NotFound => pb::Error::NotFound,
        Error::Exists => pb::Error::Exists,
        Error::NotDir => pb::Error::NotDir,
        Error::IsDir => pb::Error::IsDir,
        Error::NotEmpty => pb::Error::NotEmpty,
        Error::Invalid => pb::Error::Invalid,
        Error::TooLarge => pb::Error::TooLarge,
    }
}

pub fn commit_response(result: CommitResult, scope: &Scope) -> pb::CommitResponse {
    let outcomes = result
        .outcomes
        .iter()
        .map(|outcome| pb::Outcome {
            kind: Some(match outcome {
                Outcome::Applied { ino } => pb::outcome::Kind::Applied(pb::Applied {
                    handle: ino.map(|ino| scope.handle(ino)),
                }),
                Outcome::Skipped(skip) => pb::outcome::Kind::Skipped(pb::Skipped {
                    reason: Some(match skip {
                        Skip::Pred(PredSource::When(index)) => {
                            pb::skipped::Reason::When(*index as u32)
                        }
                        Skip::Pred(PredSource::Cond(index)) => {
                            pb::skipped::Reason::Cond(*index as u32)
                        }
                        Skip::Error(e) => pb::skipped::Reason::Error(error(*e) as i32),
                        Skip::Aborted => pb::skipped::Reason::Aborted(true),
                    }),
                }),
            }),
        })
        .collect();
    pb::CommitResponse {
        version: result.version,
        committed: result.committed,
        outcomes,
    }
}

pub fn target(target: Option<pb::Target>, scope: &Scope) -> Result<Target, Status> {
    match target.and_then(|target| target.kind) {
        Some(pb::target::Kind::Path(p)) => Ok(Target::Path(path(&p)?)),
        Some(pb::target::Kind::Handle(handle)) => Ok(Target::Ino(scope.check(&handle)?)),
        None => Err(Status::invalid_argument("empty target")),
    }
}

pub fn kind(kind: Kind) -> pb::Kind {
    match kind {
        Kind::Dir => pb::Kind::Dir,
        Kind::File => pb::Kind::File,
    }
}

pub fn inode(ino: u64, inode: &Inode, scope: &Scope) -> pb::Inode {
    inode_with(scope.handle(ino), inode)
}

/// An inode under a handle already made for it, since each handle costs a
/// MAC.
fn inode_with(handle: pb::Handle, inode: &Inode) -> pb::Inode {
    pb::Inode {
        handle: Some(handle),
        kind: kind(inode.kind) as i32,
        version: inode.version,
        mtime: inode.mtime,
        mode: inode.mode,
        nlink: inode.nlink,
        size: inode.size,
        value_version: inode.value,
        life: match inode.life {
            Life::Live => pb::Life::Live,
            Life::Detached(_) => pb::Life::Detached,
            Life::Freed(_) => pb::Life::Freed,
        } as i32,
    }
}

pub fn stat_result(result: &QueryResult, scope: &Scope) -> pb::StatResult {
    pb::StatResult {
        kind: Some(match result {
            QueryResult::Stat { ino, inode: found } => {
                pb::stat_result::Kind::Inode(inode(*ino, found, scope))
            }
            QueryResult::Failed(e) => pb::stat_result::Kind::Error(error(*e) as i32),
            other => unreachable!("a stat answered {other:?}"),
        }),
    }
}

pub fn read_result(result: &QueryResult, scope: &Scope) -> pb::ReadResult {
    pb::ReadResult {
        kind: Some(match result {
            QueryResult::Read {
                ino,
                inode: found,
                value,
            } => pb::read_result::Kind::Value(pb::Value {
                inode: Some(inode(*ino, found, scope)),
                value: value.clone(),
            }),
            QueryResult::Failed(e) => pb::read_result::Kind::Error(error(*e) as i32),
            other => unreachable!("a read answered {other:?}"),
        }),
    }
}

pub fn listed(listed: Listed, scope: &Scope) -> pb::Listed {
    let handle = scope.handle(listed.entry.child);
    pb::Listed {
        name: listed.name.into_vec().into(),
        kind: kind(listed.entry.kind) as i32,
        entry_version: listed.entry.version,
        inode: listed
            .inode
            .as_ref()
            .map(|found| inode_with(handle.clone(), found)),
        handle: Some(handle),
        value: listed.value,
    }
}

pub fn list_query(
    request: &pb::ListRequest,
    target: Target,
    after: Option<Vec<u8>>,
    limit: usize,
    budget: usize,
) -> Result<Query, Status> {
    Ok(Query::List {
        target,
        after: after.map(Into::into),
        prefix: request.prefix.as_ref().map(|prefix| prefix.to_vec().into()),
        limit,
        budget,
        attrs: request.attrs,
        values: request.values,
    })
}

fn change_entry(name: &[u8], entry: &inodedb_core::types::Entry, scope: &Scope) -> pb::Listed {
    pb::Listed {
        name: name.to_vec().into(),
        kind: kind(entry.kind) as i32,
        handle: Some(scope.handle(entry.child)),
        entry_version: entry.version,
        inode: None,
        value: None,
    }
}

pub fn change(change: &inodedb_core::feed::Change, scope: &Scope) -> pb::Change {
    pb::Change {
        version: change.version,
        time: change.time,
        entries: change
            .entries
            .iter()
            .map(|effect| pb::EntryChange {
                dir: Some(scope.handle(effect.dir)),
                name: effect.name.to_vec().into(),
                before: effect
                    .before
                    .as_ref()
                    .map(|entry| change_entry(&effect.name, entry, scope)),
                after: effect
                    .after
                    .as_ref()
                    .map(|entry| change_entry(&effect.name, entry, scope)),
            })
            .collect(),
        inodes: change
            .inodes
            .iter()
            .map(|effect| {
                let handle = scope.handle(effect.ino);
                pb::InodeChange {
                    before: effect
                        .before
                        .as_ref()
                        .map(|found| inode_with(handle.clone(), found)),
                    after: effect
                        .after
                        .as_ref()
                        .map(|found| inode_with(handle.clone(), found)),
                    handle: Some(handle),
                    holders: effect
                        .holders
                        .iter()
                        .map(|dir| scope.handle(*dir))
                        .collect(),
                }
            })
            .collect(),
        entered: change
            .entered
            .iter()
            .map(|dir| scope.handle(*dir))
            .collect(),
        compacted: false,
    }
}

/// A claimed free-queue entry, with the inode and value it kept as reads
/// by handle found them.
pub fn freed(
    entry: &inodedb_core::lease::FreeEntry,
    kept: &QueryResult,
    scope: &Scope,
) -> pb::Freed {
    let (ino, value_version) = match entry.freed {
        inodedb_core::delta::Freed::Inode(ino) => (ino, None),
        inodedb_core::delta::Freed::Value(ino, version) => (ino, Some(version)),
    };
    let (found, value) = match kept {
        QueryResult::Kept { inode, value } => (*inode, value.clone()),
        _ => (None, None),
    };
    pb::Freed {
        version: entry.version,
        handle: Some(scope.handle(ino)),
        value_version,
        inode: found.map(|found| inode(ino, &found, scope)),
        value,
    }
}
