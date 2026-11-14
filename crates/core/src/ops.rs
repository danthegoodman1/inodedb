//! Batches: ops, conditions and outcomes.

use crate::codec::{Dec, DecodeError, Enc};
use crate::types::{Error, Ino, MAX_OPS, Millis, Path, Version};
use bytes::Bytes;
use xxhash_rust::xxh3::Xxh3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutMode {
    Create,
    Replace,
    Upsert,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenameFlag {
    Replace,
    NoReplace,
    Exchange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Mkdir {
        path: Path,
        parents: bool,
        if_absent: bool,
    },
    Put {
        path: Path,
        value: Bytes,
        mode: PutMode,
        mtime: Option<Millis>,
    },
    Link {
        from: Path,
        to: Path,
    },
    Unlink {
        path: Path,
        if_exists: bool,
    },
    Rmdir {
        path: Path,
        if_exists: bool,
    },
    RemoveAll {
        path: Path,
        if_exists: bool,
    },
    Rename {
        from: Path,
        to: Path,
        flag: RenameFlag,
    },
    SetAttr {
        path: Path,
        mtime: Option<Millis>,
        mode: Option<u32>,
    },
}

/// One predicate in a condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pred {
    Exists(Path),
    Absent(Path),
    IsFile(Path),
    IsDir(Path),
    /// A directory with no entries.
    Empty(Path),
    VersionIs(Path, Version),
    /// An earlier op in the batch applied.
    Applied(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpSpec {
    pub op: Op,
    /// Predicates that must all hold for the op to apply.
    pub cond: Vec<Pred>,
}

impl OpSpec {
    pub fn new(op: Op) -> OpSpec {
        OpSpec {
            op,
            cond: Vec::new(),
        }
    }

    pub fn when(mut self, pred: Pred) -> OpSpec {
        self.cond.push(pred);
        self
    }
}

impl From<Op> for OpSpec {
    fn from(op: Op) -> OpSpec {
        OpSpec::new(op)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Batch {
    pub ops: Vec<OpSpec>,
    /// Predicates joined to every op's condition.
    pub when: Vec<Pred>,
    /// Any skipped op makes the batch apply nothing.
    pub all_or_nothing: bool,
    pub request_id: u128,
}

/// Problems with a batch as a whole, which reject it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchError {
    /// An op names an `Applied` index at or after itself.
    Malformed,
    TooManyOps,
    TooLarge,
    /// The request id answered a different batch: the caller reused it.
    Reused,
}

impl Batch {
    /// A digest of what the batch asks for, leaving out its request id. A
    /// request record keeps it, so a retry under the id can be told from a
    /// different batch. Writers compare digests across restarts and
    /// takeovers, so this encoding stays fixed.
    pub fn digest(&self) -> u128 {
        let mut d = Digester(Xxh3::new());
        d.u64(self.ops.len() as u64);
        for spec in &self.ops {
            d.op(&spec.op);
            d.preds(&spec.cond);
        }
        d.preds(&self.when);
        d.u8(u8::from(self.all_or_nothing));
        d.0.digest128()
    }

    pub fn validate(&self) -> Result<(), BatchError> {
        if self.ops.len() > MAX_OPS {
            return Err(BatchError::TooManyOps);
        }
        if self
            .when
            .iter()
            .any(|pred| matches!(pred, Pred::Applied(_)))
        {
            return Err(BatchError::Malformed);
        }
        for (index, spec) in self.ops.iter().enumerate() {
            for pred in &spec.cond {
                if let Pred::Applied(earlier) = pred
                    && *earlier >= index
                {
                    return Err(BatchError::Malformed);
                }
            }
        }
        Ok(())
    }
}

/// Feeds a batch to the hash field by field, each with its tag or length,
/// so two batches hash alike only when they ask for the same thing.
struct Digester(Xxh3);

impl Digester {
    fn u8(&mut self, value: u8) {
        self.0.update(&[value]);
    }

    fn u64(&mut self, value: u64) {
        self.0.update(&value.to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.update(value);
    }

    fn flag(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn optional(&mut self, value: Option<u64>) {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
        }
    }

    fn path(&mut self, path: &Path) {
        self.u64(path.names.len() as u64);
        for name in &path.names {
            self.bytes(name);
        }
    }

    fn op(&mut self, op: &Op) {
        match op {
            Op::Mkdir {
                path,
                parents,
                if_absent,
            } => {
                self.u8(0);
                self.path(path);
                self.flag(*parents);
                self.flag(*if_absent);
            }
            Op::Put {
                path,
                value,
                mode,
                mtime,
            } => {
                self.u8(1);
                self.path(path);
                self.bytes(value);
                self.u8(match mode {
                    PutMode::Create => 0,
                    PutMode::Replace => 1,
                    PutMode::Upsert => 2,
                });
                self.optional(*mtime);
            }
            Op::Link { from, to } => {
                self.u8(2);
                self.path(from);
                self.path(to);
            }
            Op::Unlink { path, if_exists } => {
                self.u8(3);
                self.path(path);
                self.flag(*if_exists);
            }
            Op::Rmdir { path, if_exists } => {
                self.u8(4);
                self.path(path);
                self.flag(*if_exists);
            }
            Op::RemoveAll { path, if_exists } => {
                self.u8(5);
                self.path(path);
                self.flag(*if_exists);
            }
            Op::Rename { from, to, flag } => {
                self.u8(6);
                self.path(from);
                self.path(to);
                self.u8(match flag {
                    RenameFlag::Replace => 0,
                    RenameFlag::NoReplace => 1,
                    RenameFlag::Exchange => 2,
                });
            }
            Op::SetAttr { path, mtime, mode } => {
                self.u8(7);
                self.path(path);
                self.optional(*mtime);
                self.optional(mode.map(u64::from));
            }
        }
    }

    fn preds(&mut self, preds: &[Pred]) {
        self.u64(preds.len() as u64);
        for pred in preds {
            match pred {
                Pred::Exists(path) => {
                    self.u8(0);
                    self.path(path);
                }
                Pred::Absent(path) => {
                    self.u8(1);
                    self.path(path);
                }
                Pred::IsFile(path) => {
                    self.u8(2);
                    self.path(path);
                }
                Pred::IsDir(path) => {
                    self.u8(3);
                    self.path(path);
                }
                Pred::Empty(path) => {
                    self.u8(4);
                    self.path(path);
                }
                Pred::VersionIs(path, version) => {
                    self.u8(5);
                    self.path(path);
                    self.u64(*version);
                }
                Pred::Applied(index) => {
                    self.u8(6);
                    self.u64(*index as u64);
                }
            }
        }
    }
}

/// Which predicate stopped an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredSource {
    /// The batch's `when`, by index.
    When(usize),
    /// The op's own condition, by index.
    Cond(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    Pred(PredSource),
    Error(Error),
    /// Another op in an `all_or_nothing` batch was skipped.
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The op applied. `ino` is the inode it created or touched; an op that
    /// succeeded without change, such as `Unlink` with `IfExists` on an
    /// absent name, has none.
    Applied {
        ino: Option<Ino>,
    },
    Skipped(Skip),
}

impl Outcome {
    pub fn applied(&self) -> bool {
        matches!(self, Outcome::Applied { .. })
    }

    fn encode(&self, enc: &mut Enc) {
        match self {
            Outcome::Applied { ino: None } => {
                enc.u8(0);
            }
            Outcome::Applied { ino: Some(ino) } => {
                enc.u8(1).varint(*ino);
            }
            Outcome::Skipped(Skip::Pred(PredSource::When(index))) => {
                enc.u8(2).varint(*index as u64);
            }
            Outcome::Skipped(Skip::Pred(PredSource::Cond(index))) => {
                enc.u8(3).varint(*index as u64);
            }
            Outcome::Skipped(Skip::Error(error)) => {
                enc.u8(4).u8(error.code());
            }
            Outcome::Skipped(Skip::Aborted) => {
                enc.u8(5);
            }
        }
    }

    fn decode(dec: &mut Dec<'_>) -> Result<Outcome, DecodeError> {
        Ok(match dec.u8()? {
            0 => Outcome::Applied { ino: None },
            1 => Outcome::Applied {
                ino: Some(dec.varint()?),
            },
            2 => Outcome::Skipped(Skip::Pred(PredSource::When(dec.varint()? as usize))),
            3 => Outcome::Skipped(Skip::Pred(PredSource::Cond(dec.varint()? as usize))),
            4 => Outcome::Skipped(Skip::Error(Error::from_code(dec.u8()?).ok_or(DecodeError)?)),
            5 => Outcome::Skipped(Skip::Aborted),
            _ => return Err(DecodeError),
        })
    }
}

/// A batch's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitResult {
    /// The commit's version when the batch applied anything; otherwise the
    /// version of the state it was evaluated against.
    pub version: Version,
    pub committed: bool,
    pub outcomes: Vec<Outcome>,
}

impl CommitResult {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(8 + 3 * self.outcomes.len());
        enc.varint(self.version)
            .u8(u8::from(self.committed))
            .varint(self.outcomes.len() as u64);
        for outcome in &self.outcomes {
            outcome.encode(&mut enc);
        }
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<CommitResult, DecodeError> {
        let mut dec = Dec::new(bytes);
        let result = CommitResult::read(&mut dec)?;
        dec.end()?;
        Ok(result)
    }

    fn read(dec: &mut Dec<'_>) -> Result<CommitResult, DecodeError> {
        let version = dec.varint()?;
        let committed = dec.u8()? != 0;
        let count = dec.varint()? as usize;
        if count > MAX_OPS {
            return Err(DecodeError);
        }
        let mut outcomes = Vec::with_capacity(count);
        for _ in 0..count {
            outcomes.push(Outcome::decode(dec)?);
        }
        Ok(CommitResult {
            version,
            committed,
            outcomes,
        })
    }
}

/// A request record: a batch's answer, with the digest of the batch that
/// earned it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded {
    pub result: CommitResult,
    /// The batch's [`Batch::digest`]. A record written before records kept
    /// digests has none and answers any batch under its id.
    pub digest: Option<u128>,
}

impl Recorded {
    /// Whether the record answers a batch with this digest, as a retry of
    /// the batch that earned it.
    pub fn answers(&self, digest: u128) -> bool {
        self.digest.is_none_or(|kept| kept == digest)
    }

    /// The answer's encoding, then the digest's 16 bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = self.result.encode();
        if let Some(digest) = self.digest {
            bytes.extend_from_slice(&digest.to_be_bytes());
        }
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Recorded, DecodeError> {
        let mut dec = Dec::new(bytes);
        let result = CommitResult::read(&mut dec)?;
        let digest = if dec.is_empty() {
            None
        } else {
            let bytes = dec.take(16)?;
            Some(u128::from_be_bytes(bytes.try_into().expect("16 bytes")))
        };
        dec.end()?;
        Ok(Recorded { result, digest })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(text: &str) -> Path {
        text.parse().unwrap()
    }

    #[test]
    fn applied_must_name_an_earlier_op() {
        let mut batch = Batch {
            ops: vec![
                Op::Mkdir {
                    path: path("a"),
                    parents: false,
                    if_absent: false,
                }
                .into(),
            ],
            ..Batch::default()
        };
        batch.ops[0].cond.push(Pred::Applied(0));
        assert_eq!(batch.validate(), Err(BatchError::Malformed));
        batch.ops[0].cond.clear();
        batch.when.push(Pred::Applied(0));
        assert_eq!(batch.validate(), Err(BatchError::Malformed));
    }

    #[test]
    fn results_round_trip() {
        let result = CommitResult {
            version: 12,
            committed: true,
            outcomes: vec![
                Outcome::Applied { ino: Some(9) },
                Outcome::Applied { ino: None },
                Outcome::Skipped(Skip::Pred(PredSource::When(0))),
                Outcome::Skipped(Skip::Pred(PredSource::Cond(2))),
                Outcome::Skipped(Skip::Error(Error::NotEmpty)),
                Outcome::Skipped(Skip::Aborted),
            ],
        };
        assert_eq!(CommitResult::decode(&result.encode()), Ok(result));
    }

    #[test]
    fn a_digest_tells_batches_apart() {
        let put = |path_text: &str, value: &'static [u8], mode, mtime| {
            OpSpec::new(Op::Put {
                path: path(path_text),
                value: Bytes::from_static(value),
                mode,
                mtime,
            })
        };
        let batch = |ops: Vec<OpSpec>| Batch {
            ops,
            ..Batch::default()
        };
        let base = batch(vec![put("a", b"1", PutMode::Create, None)]);
        let retry = Batch {
            request_id: 9,
            ..base.clone()
        };
        assert_eq!(base.digest(), retry.digest(), "the request id stays out");
        let others = [
            batch(vec![put("b", b"1", PutMode::Create, None)]),
            batch(vec![put("a", b"2", PutMode::Create, None)]),
            batch(vec![put("a", b"1", PutMode::Upsert, None)]),
            batch(vec![put("a", b"1", PutMode::Create, Some(0))]),
            batch(vec![put("ab", b"", PutMode::Create, None)]),
            batch(vec![put("a/b", b"", PutMode::Create, None)]),
            batch(vec![
                put("a", b"1", PutMode::Create, None),
                put("b", b"1", PutMode::Create, None),
            ]),
            batch(vec![
                put("b", b"1", PutMode::Create, None),
                put("a", b"1", PutMode::Create, None),
            ]),
            batch(vec![
                put("a", b"1", PutMode::Create, None).when(Pred::Exists(path("d"))),
            ]),
            Batch {
                when: vec![Pred::Exists(path("d"))],
                ..base.clone()
            },
            Batch {
                all_or_nothing: true,
                ..base.clone()
            },
            batch(vec![OpSpec::new(Op::Mkdir {
                path: path("a"),
                parents: false,
                if_absent: false,
            })]),
        ];
        let mut digests: Vec<u128> = others.iter().map(Batch::digest).collect();
        digests.push(base.digest());
        let count = digests.len();
        digests.sort_unstable();
        digests.dedup();
        assert_eq!(digests.len(), count, "each batch has its own digest");
    }

    #[test]
    fn records_keep_their_batchs_digest() {
        let result = CommitResult {
            version: 12,
            committed: true,
            outcomes: vec![Outcome::Applied { ino: Some(9) }],
        };
        let recorded = Recorded {
            result: result.clone(),
            digest: Some(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210),
        };
        assert_eq!(Recorded::decode(&recorded.encode()), Ok(recorded.clone()));
        assert!(recorded.answers(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210));
        assert!(!recorded.answers(1));
        let mut cut = recorded.encode();
        cut.pop();
        assert_eq!(Recorded::decode(&cut), Err(DecodeError));
        // A record without a digest answers any batch.
        let bare = Recorded::decode(&result.encode()).expect("decodes");
        assert_eq!(bare.digest, None);
        assert!(bare.answers(1) && bare.answers(2));
    }
}
