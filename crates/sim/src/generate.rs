//! Random batches and queries over a small namespace, so that names
//! collide, conditions fail and skips cascade often.

use crate::prng::Prng;
use inodedb_core::Bytes;
use inodedb_core::ops::{Batch, Op, OpSpec, Pred, PutMode, RenameFlag};
use inodedb_core::query::{Query, Target};
use inodedb_core::types::{MAX_VALUE, Name, Path, Version};

pub struct Shape {
    pub names: usize,
    pub depth: usize,
    pub cond_percent: u64,
    pub atomic_percent: u64,
    pub when_percent: u64,
}

const NAMES: [&[u8]; 5] = [b"a", b"b", b"c", b"d", b"e"];

pub fn path(prng: &mut Prng, shape: &Shape) -> Path {
    let depth = prng.range(0..=shape.depth as u64) as usize;
    let depth = if depth == 0 && prng.percent(80) {
        1
    } else {
        depth
    };
    Path {
        names: (0..depth)
            .map(|_| Name::from(NAMES[prng.index(shape.names)]))
            .collect(),
    }
}

fn value(prng: &mut Prng) -> Bytes {
    if prng.percent(1) {
        return Bytes::from(vec![7u8; MAX_VALUE + 1]);
    }
    let len = prng.below(24) as usize;
    Bytes::from((0..len).map(|_| prng.below(256) as u8).collect::<Vec<u8>>())
}

fn op(prng: &mut Prng, shape: &Shape) -> Op {
    match prng.below(100) {
        0..=19 => Op::Mkdir {
            path: path(prng, shape),
            parents: prng.percent(30),
            if_absent: prng.percent(40),
        },
        20..=44 => Op::Put {
            path: path(prng, shape),
            value: value(prng),
            mode: *prng.pick(&[
                PutMode::Create,
                PutMode::Replace,
                PutMode::Upsert,
                PutMode::Upsert,
            ]),
            mtime: prng.percent(20).then(|| prng.below(1_000_000)),
        },
        45..=51 => Op::Link {
            from: path(prng, shape),
            to: path(prng, shape),
        },
        52..=61 => Op::Unlink {
            path: path(prng, shape),
            if_exists: prng.percent(40),
        },
        62..=68 => Op::Rmdir {
            path: path(prng, shape),
            if_exists: prng.percent(40),
        },
        69..=72 => Op::RemoveAll {
            path: path(prng, shape),
            if_exists: prng.percent(40),
        },
        73..=92 => Op::Rename {
            from: path(prng, shape),
            to: path(prng, shape),
            flag: *prng.pick(&[
                RenameFlag::Replace,
                RenameFlag::NoReplace,
                RenameFlag::Exchange,
            ]),
        },
        _ => Op::SetAttr {
            path: path(prng, shape),
            mtime: prng.percent(70).then(|| prng.below(1_000_000)),
            mode: prng.percent(50).then(|| prng.below(0o1000) as u32),
        },
    }
}

fn pred(prng: &mut Prng, shape: &Shape, index: Option<usize>, versions: &[Version]) -> Pred {
    match prng.below(7) {
        0 => Pred::Exists(path(prng, shape)),
        1 => Pred::Absent(path(prng, shape)),
        2 => Pred::IsFile(path(prng, shape)),
        3 => Pred::IsDir(path(prng, shape)),
        4 => Pred::Empty(path(prng, shape)),
        5 => {
            let version = if versions.is_empty() || prng.percent(20) {
                prng.below(50)
            } else {
                *prng.pick(versions)
            };
            Pred::VersionIs(path(prng, shape), version)
        }
        _ => match index {
            Some(index) if index > 0 => Pred::Applied(prng.index(index)),
            _ => Pred::Exists(Path::root()),
        },
    }
}

/// A batch; `versions` are versions seen lately, for `VersionIs`.
pub fn batch(prng: &mut Prng, shape: &Shape, versions: &[Version], request_id: u128) -> Batch {
    let len = if prng.percent(10) {
        prng.range(10..=40)
    } else {
        prng.range(1..=6)
    } as usize;
    let mut ops = Vec::with_capacity(len);
    for index in 0..len {
        let mut spec = OpSpec::new(op(prng, shape));
        if prng.percent(shape.cond_percent) {
            for _ in 0..prng.range(1..=2) {
                spec.cond.push(pred(prng, shape, Some(index), versions));
            }
        }
        ops.push(spec);
    }
    if prng.percent(1) && len > 0 {
        // Malformed: an op names itself.
        let last = len - 1;
        ops[last].cond.push(Pred::Applied(last));
    }
    let when = if prng.percent(shape.when_percent) {
        vec![pred(prng, shape, None, versions)]
    } else {
        Vec::new()
    };
    Batch {
        ops,
        when,
        all_or_nothing: prng.percent(shape.atomic_percent),
        request_id,
    }
}

pub fn query(prng: &mut Prng, shape: &Shape) -> Query {
    if prng.percent(15) {
        return Query::Many(
            (0..prng.range(2..=4))
                .map(|_| single_query(prng, shape))
                .collect(),
        );
    }
    single_query(prng, shape)
}

fn single_query(prng: &mut Prng, shape: &Shape) -> Query {
    match prng.below(3) {
        0 => Query::Stat(Target::Path(path(prng, shape))),
        1 => Query::Read {
            target: Target::Path(path(prng, shape)),
            version: None,
        },
        _ => Query::List {
            target: Target::Path({
                let mut listed = path(prng, shape);
                if prng.percent(50) {
                    listed.names.truncate(listed.names.len().saturating_sub(1));
                }
                listed
            }),
            after: prng
                .percent(20)
                .then(|| Name::from(NAMES[prng.index(shape.names)])),
            prefix: prng
                .percent(10)
                .then(|| Name::from(NAMES[prng.index(shape.names)])),
            limit: prng.range(1..=8) as usize,
            budget: if prng.percent(30) {
                prng.range(0..=600) as usize
            } else {
                usize::MAX
            },
            attrs: prng.percent(60),
            values: prng.percent(40),
        },
    }
}
