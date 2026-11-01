//! Batches: ops, conditions and outcomes.

use crate::codec::{Dec, DecodeError, Enc};
use crate::types::{Error, Ino, MAX_OPS, Millis, Path, Version};
use bytes::Bytes;

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
}

impl Batch {
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
        let version = dec.varint()?;
        let committed = dec.u8()? != 0;
        let count = dec.varint()? as usize;
        if count > MAX_OPS {
            return Err(DecodeError);
        }
        let mut outcomes = Vec::with_capacity(count);
        for _ in 0..count {
            outcomes.push(Outcome::decode(&mut dec)?);
        }
        dec.end()?;
        Ok(CommitResult {
            version,
            committed,
            outcomes,
        })
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
}
