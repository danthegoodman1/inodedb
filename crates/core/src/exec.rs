//! Running a batch against a view.
//!
//! Each op runs against the state the applied ops before it left, checks
//! everything that could stop it before changing anything, and either
//! applies or is skipped. A lookup the view can't answer names a read;
//! execution then keeps going to collect the reads later ops need too, and
//! the caller runs the batch again once they arrive.

use crate::auth::{KeyError, KeyId, KeyOp, KeyRecord, Scope};
use crate::delta::{Acked, After, Change, Delta, Freed, Links};
use crate::lease::FreeEntry;
use crate::log::Chains;
use crate::ops::Pred;
use crate::ops::{Batch, Op, OpSpec, Outcome, PredSource, PutMode, RenameFlag, Skip};
use crate::types::{
    Entry, Error, Ino, Inode, Kind, Life, MAX_VALUE, Millis, Name, Path, ROOT, Version,
};
use crate::view::{Lookup, Read, View};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

/// The most reads one run asks for.
const MAX_READS: usize = 1024;
/// The deepest ancestry a cycle check follows.
const MAX_DEPTH: usize = 1 << 16;

pub struct Input<'a> {
    pub batch: &'a Batch,
    /// The caller's key root, which paths resolve from.
    pub root: Ino,
    pub now: Millis,
    /// The version the batch commits at if it changes anything.
    pub version: Version,
    pub next_ino: Ino,
}

#[derive(Debug)]
pub struct Executed {
    pub delta: Delta,
    pub outcomes: Vec<Outcome>,
    /// Whether the batch changed anything. A batch that changes nothing
    /// writes nothing and takes no version.
    pub committed: bool,
    pub next_ino: Ino,
    /// The directories holding each file whose record changed, before and
    /// after, for routing the changefeed.
    pub holders: BTreeMap<Ino, BTreeSet<Ino>>,
    /// The ancestor chains of every directory the changefeed routes the
    /// batch's effects by.
    pub chains: BTreeMap<Ino, Chains>,
}

enum Halt {
    Need(Read),
    Fail(Error),
}

impl From<Read> for Halt {
    fn from(read: Read) -> Halt {
        Halt::Need(read)
    }
}

type Step<T> = Result<T, Halt>;

fn fail<T>(error: Error) -> Step<T> {
    Err(Halt::Fail(error))
}

struct Exec<'a> {
    base: &'a dyn View,
    delta: Delta,
    root: Ino,
    now: Millis,
    version: Version,
    next_ino: Ino,
    /// While freeing a detached subtree: its top, and the directories
    /// above the top as it detached.
    reaping: Option<(Ino, Vec<Ino>)>,
}

pub fn execute(input: Input<'_>, base: &dyn View) -> Result<Executed, Vec<Read>> {
    let batch = input.batch;
    let mut exec = Exec {
        base,
        delta: Delta::new(input.version),
        root: input.root,
        now: input.now,
        version: input.version,
        next_ino: input.next_ino,
        reaping: None,
    };
    let mut outcomes: Vec<Outcome> = Vec::with_capacity(batch.ops.len());
    let mut needs = BTreeSet::new();
    let mut first_skip = None;
    for (index, spec) in batch.ops.iter().enumerate() {
        let outcome = match exec.run(spec, &batch.when, &outcomes) {
            Ok(outcome) => outcome,
            Err(read) => {
                if needs.len() < MAX_READS {
                    needs.insert(read);
                }
                Outcome::Skipped(Skip::Aborted)
            }
        };
        if !outcome.applied() && first_skip.is_none() && needs.is_empty() {
            first_skip = Some(index);
        }
        outcomes.push(outcome);
        if batch.all_or_nothing && first_skip.is_some() {
            break;
        }
    }
    if needs.is_empty() {
        exec.finish_holders(&mut needs);
    }
    if needs.is_empty()
        && let Err(read) = exec.revoke_orphans()
    {
        needs.insert(read);
    }
    if !needs.is_empty() {
        return Err(needs.into_iter().collect());
    }
    if batch.all_or_nothing
        && let Some(skipped) = first_skip
    {
        let reason = outcomes[skipped];
        let outcomes = (0..batch.ops.len())
            .map(|index| {
                if index == skipped {
                    reason
                } else {
                    Outcome::Skipped(Skip::Aborted)
                }
            })
            .collect();
        return Ok(Executed {
            delta: Delta::new(input.version),
            outcomes,
            committed: false,
            next_ino: input.next_ino,
            holders: BTreeMap::new(),
            chains: BTreeMap::new(),
        });
    }
    let committed = !exec.delta.is_empty();
    let (holders, chains) = if committed {
        let chains = exec
            .chains()
            .map_err(|needs| needs.into_iter().collect::<Vec<_>>())?;
        (exec.holders(), chains)
    } else {
        (BTreeMap::new(), BTreeMap::new())
    };
    Ok(Executed {
        next_ino: if committed {
            exec.next_ino
        } else {
            input.next_ino
        },
        delta: if committed {
            exec.delta
        } else {
            Delta::new(input.version)
        },
        outcomes,
        committed,
        holders,
        chains,
    })
}

/// Runs a key op for a caller holding `scope`: the delta it commits, the
/// reason it fails, or the reads it needs.
pub fn execute_key(
    op: &KeyOp,
    scope: &Scope,
    now: Millis,
    version: Version,
    base: &dyn View,
) -> Result<Result<Delta, KeyError>, Vec<Read>> {
    let mut exec = Exec {
        base,
        delta: Delta::new(version),
        root: scope.root,
        now,
        version,
        next_ino: 0,
        reaping: None,
    };
    let failed = match op {
        KeyOp::Mint {
            id,
            hash,
            root,
            grant,
            ttl,
        } => match exec.resolve(root) {
            Err(Halt::Need(read)) => return Err(vec![read]),
            Err(Halt::Fail(error)) => Some(KeyError::Fs(error)),
            Ok((_, inode)) if inode.kind != Kind::Dir => Some(KeyError::Fs(Error::NotDir)),
            Ok(_) if base.key(*id).is_some() || base.key_retired(*id) => Some(KeyError::Exists),
            Ok((ino, _)) => {
                let mut expires = if *ttl == 0 {
                    scope.expires
                } else {
                    now.saturating_add(*ttl)
                };
                if scope.expires != 0 {
                    expires = expires.min(scope.expires);
                }
                let record = KeyRecord {
                    hash: *hash,
                    root: ino,
                    grant: *grant,
                    expires,
                    minted_by: scope.key,
                };
                exec.delta.set_key(*id, None, Some(record));
                None
            }
        },
        KeyOp::Revoke { id } => {
            if exec.may_revoke(scope, *id) {
                exec.revoke(*id);
                None
            } else {
                Some(KeyError::NotFound)
            }
        }
    };
    Ok(match failed {
        Some(error) => Err(error),
        None => Ok(exec.delta),
    })
}

/// The most inodes one commit of a detached subtree's freeing frees.
pub const REAP_BUDGET: usize = 1_000;

/// Frees up to `REAP_BUDGET` inodes of the subtree detached at `at` under
/// `top`, deepest first: a file loses its link in the subtree and is freed
/// once it has none left. Once the subtree is empty, frees `top` and drops
/// its detach record. What it frees names the directories inside the
/// subtree above it, then `above`, which lay above `top` as it detached.
pub fn execute_reap(
    top: Ino,
    at: Version,
    above: &[Ino],
    now: Millis,
    version: Version,
    base: &dyn View,
) -> Result<Executed, Vec<Read>> {
    let mut exec = Exec {
        base,
        delta: Delta::new(version),
        root: ROOT,
        now,
        version,
        next_ino: 0,
        reaping: Some((top, above.to_vec())),
    };
    let mut needs = BTreeSet::new();
    if let Err(Halt::Need(read)) = exec.reap(top, at, &mut needs) {
        needs.insert(read);
    }
    if needs.is_empty() {
        exec.finish_holders(&mut needs);
    }
    if needs.is_empty()
        && let Err(read) = exec.revoke_orphans()
    {
        needs.insert(read);
    }
    if !needs.is_empty() {
        return Err(needs.into_iter().collect());
    }
    let chains = exec
        .chains()
        .map_err(|needs| needs.into_iter().collect::<Vec<_>>())?;
    Ok(Executed {
        holders: exec.holders(),
        chains,
        committed: !exec.delta.is_empty(),
        next_ino: 0,
        outcomes: Vec::new(),
        delta: exec.delta,
    })
}

/// Deletes acknowledged free-queue entries with the records they kept: a
/// freed inode's record and current value, or a replaced value. An entry
/// already gone is skipped, so acknowledging twice deletes once.
pub fn execute_ack(
    entries: &[FreeEntry],
    version: Version,
    base: &dyn View,
) -> Result<Delta, Vec<Read>> {
    let mut delta = Delta::new(version);
    let mut needs = BTreeSet::new();
    for entry in entries {
        let acked = Acked {
            version: entry.version,
            freed: entry.freed,
            ancestors: entry.ancestors.clone(),
        };
        match entry.freed {
            Freed::Inode(ino) => {
                let inode = match (base.free_entry(entry.version, ino), base.inode(ino)) {
                    (Ok(None), _) => continue,
                    (Ok(Some(_)), Ok(inode)) => inode,
                    (stands, inode) => {
                        needs.extend(stands.err());
                        needs.extend(inode.err());
                        continue;
                    }
                };
                if let Some(inode) = inode {
                    delta.inodes.insert(
                        ino,
                        Change {
                            before: Some(inode),
                            after: None,
                        },
                    );
                    if inode.kind == Kind::File {
                        delta.dropped.insert((ino, inode.value));
                    }
                }
                delta.acked.push(acked);
            }
            Freed::Value(ino, value) => match base.value(ino, value) {
                Ok(None) => {}
                Ok(Some(_)) => {
                    delta.dropped.insert((ino, value));
                    delta.acked.push(acked);
                }
                Err(read) => {
                    needs.insert(read);
                }
            },
        }
    }
    if needs.is_empty() {
        Ok(delta)
    } else {
        Err(needs.into_iter().collect())
    }
}

/// A directory's chain to the root through `view`, starting with itself.
/// A directory the view lacks has no chain.
fn chain(view: &dyn View, dir: Ino) -> Lookup<Vec<Ino>> {
    chain_until(view, dir, ROOT)
}

/// `dir` and its ancestors up to `top`, or to the root.
fn chain_until(view: &dyn View, dir: Ino, top: Ino) -> Lookup<Vec<Ino>> {
    let mut chain = Vec::new();
    let mut current = dir;
    while let Some(inode) = view.inode(current)? {
        chain.push(current);
        if current == top || current == ROOT || chain.len() >= MAX_DEPTH {
            break;
        }
        current = inode.parent;
    }
    Ok(chain)
}

impl Exec<'_> {
    fn run(&mut self, spec: &OpSpec, when: &[Pred], outcomes: &[Outcome]) -> Result<Outcome, Read> {
        let preds = when
            .iter()
            .enumerate()
            .map(|(index, pred)| (PredSource::When(index), pred))
            .chain(
                spec.cond
                    .iter()
                    .enumerate()
                    .map(|(index, pred)| (PredSource::Cond(index), pred)),
            );
        for (source, pred) in preds {
            match self.holds(pred, outcomes) {
                Ok(true) => {}
                Ok(false) => return Ok(Outcome::Skipped(Skip::Pred(source))),
                Err(Halt::Need(read)) => return Err(read),
                Err(Halt::Fail(_)) => return Ok(Outcome::Skipped(Skip::Pred(source))),
            }
        }
        match self.apply(&spec.op) {
            Ok(ino) => Ok(Outcome::Applied { ino }),
            Err(Halt::Fail(error)) => Ok(Outcome::Skipped(Skip::Error(error))),
            Err(Halt::Need(read)) => Err(read),
        }
    }

    fn holds(&self, pred: &Pred, outcomes: &[Outcome]) -> Step<bool> {
        Ok(match pred {
            Pred::Exists(path) => self.stat(path)?.is_some(),
            Pred::Absent(path) => self.stat(path)?.is_none(),
            Pred::IsFile(path) => {
                matches!(self.stat(path)?, Some((_, inode)) if inode.kind == Kind::File)
            }
            Pred::IsDir(path) => {
                matches!(self.stat(path)?, Some((_, inode)) if inode.kind == Kind::Dir)
            }
            Pred::Empty(path) => match self.stat(path)? {
                Some((ino, inode)) if inode.kind == Kind::Dir => !self.has_entries(ino)?,
                _ => false,
            },
            Pred::VersionIs(path, version) => {
                matches!(self.stat(path)?, Some((_, inode)) if inode.version == *version)
            }
            Pred::Applied(index) => outcomes.get(*index).is_some_and(Outcome::applied),
        })
    }

    fn apply(&mut self, op: &Op) -> Step<Option<Ino>> {
        match op {
            Op::Mkdir {
                path,
                parents,
                if_absent,
            } => self.mkdir(path, *parents, *if_absent),
            Op::Put {
                path,
                value,
                mode,
                mtime,
            } => self.put(path, value, *mode, *mtime),
            Op::Link { from, to } => self.link(from, to),
            Op::Unlink { path, if_exists } => self.unlink(path, *if_exists),
            Op::Rmdir { path, if_exists } => self.rmdir(path, *if_exists),
            Op::RemoveAll { path, if_exists } => self.remove_all(path, *if_exists),
            Op::Rename { from, to, flag } => self.rename(from, to, *flag),
            Op::SetAttr { path, mtime, mode } => self.set_attr(path, *mtime, *mode),
        }
    }

    // Lookups through the batch's changes.

    fn inode(&self, ino: Ino) -> Step<Option<Inode>> {
        Ok(self.delta.inode(self.base, ino)?)
    }

    fn must_inode(&self, ino: Ino) -> Step<Inode> {
        match self.inode(ino)? {
            Some(inode) => Ok(inode),
            None => fail(Error::NotFound),
        }
    }

    fn entry(&self, dir: Ino, name: &[u8]) -> Step<Option<Entry>> {
        Ok(self.delta.entry(self.base, dir, name)?)
    }

    fn links(&self, ino: Ino) -> Step<Links> {
        Ok(self.delta.backptrs(self.base, ino)?)
    }

    fn has_entries(&self, dir: Ino) -> Step<bool> {
        Ok(self
            .delta
            .has_entry_outside(self.base, dir, &BTreeSet::new())?)
    }

    /// The directory that `names` leads to from the root.
    fn walk(&self, names: &[Name]) -> Step<Ino> {
        let mut dir = self.root;
        for name in names {
            match self.entry(dir, name)? {
                None => return fail(Error::NotFound),
                Some(entry) if entry.kind == Kind::File => return fail(Error::NotDir),
                Some(entry) => dir = entry.child,
            }
        }
        Ok(dir)
    }

    fn resolve(&self, path: &Path) -> Step<(Ino, Inode)> {
        let Some((parents, name)) = path.split_last() else {
            return Ok((self.root, self.must_inode(self.root)?));
        };
        let dir = self.walk(parents)?;
        match self.entry(dir, name)? {
            Some(entry) => Ok((entry.child, self.must_inode(entry.child)?)),
            None => fail(Error::NotFound),
        }
    }

    fn stat(&self, path: &Path) -> Step<Option<(Ino, Inode)>> {
        match self.resolve(path) {
            Ok(found) => Ok(Some(found)),
            Err(Halt::Fail(Error::NotFound | Error::NotDir)) => Ok(None),
            Err(other) => Err(other),
        }
    }

    fn key(&self, id: KeyId) -> Option<KeyRecord> {
        self.delta.key(self.base, id)
    }

    /// Whether a caller may revoke a key: its own, or one minted from it.
    fn may_revoke(&self, scope: &Scope, id: KeyId) -> bool {
        let Some(caller) = scope.key else {
            return self.key(id).is_some();
        };
        let mut current = Some(id);
        for _ in 0..MAX_DEPTH {
            let Some(at) = current else {
                return false;
            };
            if at == caller {
                return true;
            }
            current = self.key(at).and_then(|record| record.minted_by);
        }
        false
    }

    /// Revokes a key and every key minted from it. A batch mints no keys,
    /// so the keys minted from one are those the view beneath it holds.
    fn revoke(&mut self, id: KeyId) {
        let mut stack = vec![id];
        while let Some(id) = stack.pop() {
            if self.key(id).is_none() {
                continue;
            }
            stack.extend(self.base.keys_minted_by(id));
            let before = self.base.key(id);
            self.delta.set_key(id, before, None);
        }
    }

    /// Revokes the keys whose roots the batch freed or detached. A freed
    /// directory was empty, so only keys rooted at it go; a detached one
    /// takes every key rooted beneath it.
    fn revoke_orphans(&mut self) -> Result<(), Read> {
        let mut orphans = BTreeSet::new();
        for freed in &self.delta.freed {
            if let Freed::Inode(ino) = freed {
                orphans.extend(self.delta.keys_rooted_at(self.base, *ino));
            }
        }
        if !self.delta.detached.is_empty() {
            for (id, root) in self.delta.key_roots(self.base) {
                for dir in &self.delta.detached {
                    match self.is_under(root, *dir) {
                        Ok(true) => {
                            orphans.insert(id);
                            break;
                        }
                        Ok(false) | Err(Halt::Fail(_)) => {}
                        Err(Halt::Need(read)) => return Err(read),
                    }
                }
            }
        }
        for id in orphans {
            self.revoke(id);
        }
        Ok(())
    }

    fn reap(&mut self, top: Ino, at: Version, needs: &mut BTreeSet<Read>) -> Step<()> {
        let mut freed = 0;
        let mut stack = vec![top];
        while let Some(&dir) = stack.last() {
            if freed == REAP_BUDGET {
                return Ok(());
            }
            // As many entries as the budget can still free.
            let listing =
                self.delta
                    .listing(self.base, dir, Bound::Unbounded, REAP_BUDGET - freed)?;
            self.prefetch(&listing, needs)?;
            let mut deeper = None;
            for (name, entry) in listing {
                match entry.kind {
                    Kind::File => self.unlink_file(dir, &name, entry)?,
                    Kind::Dir if self.has_entries(entry.child)? => {
                        deeper = Some(entry.child);
                        break;
                    }
                    Kind::Dir => {
                        let inode = self.must_inode(entry.child)?;
                        self.put_entry(dir, &name, None)?;
                        self.free_dir(entry.child, inode)?;
                    }
                }
                freed += 1;
            }
            match deeper {
                Some(child) => stack.push(child),
                // A listing the budget cut short may leave entries.
                None if freed == REAP_BUDGET => return Ok(()),
                None => {
                    stack.pop();
                }
            }
        }
        let mut inode = self.must_inode(top)?;
        inode.life = Life::Freed(self.version);
        inode.version = self.version;
        let above = self
            .reaping
            .as_ref()
            .map_or(Vec::new(), |(_, above)| above.clone());
        self.delta.freed.push(Freed::Inode(top));
        self.delta.ancestors.insert(Freed::Inode(top), above);
        self.put_inode(top, inode)?;
        self.delta.undetached.push((at, top));
        Ok(())
    }

    /// Asks at once for what freeing `entries` reads, so a cold subtree
    /// costs a round of reads per directory rather than per entry.
    fn prefetch(&self, entries: &[(Name, Entry)], needs: &mut BTreeSet<Read>) -> Step<()> {
        for (_, entry) in entries {
            let mut want = |read: Option<Read>| {
                if let Some(read) = read
                    && needs.len() < MAX_READS
                {
                    needs.insert(read);
                }
            };
            want(self.delta.inode(self.base, entry.child).err());
            want(match entry.kind {
                Kind::File => self.delta.backptrs(self.base, entry.child).err(),
                Kind::Dir => self
                    .delta
                    .has_entry_outside(self.base, entry.child, &BTreeSet::new())
                    .err(),
            });
        }
        match needs.first() {
            Some(read) => Err(Halt::Need(read.clone())),
            None => Ok(()),
        }
    }

    /// `dir` and the directories above it as the batch has them now. In a
    /// detached subtree the chain goes up to its top, then on through what
    /// lay above the top as it detached, since those directories may since
    /// have moved or gone.
    fn chain(&self, dir: Ino) -> Step<Vec<Ino>> {
        let view = After {
            delta: &self.delta,
            base: self.base,
        };
        let Some((top, above)) = &self.reaping else {
            return Ok(chain(&view, dir)?);
        };
        let mut found = Vec::new();
        let mut current = dir;
        while found.len() < MAX_DEPTH {
            found.push(current);
            if current == *top {
                found.extend(above);
                break;
            }
            current = self.must_inode(current)?.parent;
        }
        Ok(found)
    }

    /// Whether `dir` is `ancestor` or lies beneath it.
    fn is_under(&self, dir: Ino, ancestor: Ino) -> Step<bool> {
        let mut current = dir;
        for _ in 0..MAX_DEPTH {
            if current == ancestor {
                return Ok(true);
            }
            if current == ROOT {
                return Ok(false);
            }
            current = self.must_inode(current)?.parent;
        }
        fail(Error::Invalid)
    }

    // Changes. Each records the state before the batch the first time it
    // touches a key; callers have already read that state.

    fn put_inode(&mut self, ino: Ino, after: Inode) -> Step<()> {
        let before = if self.delta.inodes.contains_key(&ino) || self.delta.created.contains(&ino) {
            None
        } else {
            self.base.inode(ino)?
        };
        self.delta.set_inode(ino, before, after);
        Ok(())
    }

    fn put_entry(&mut self, dir: Ino, name: &[u8], after: Option<Entry>) -> Step<()> {
        let touched = self
            .delta
            .entries
            .get(&dir)
            .is_some_and(|names| names.contains_key(name));
        let before = if touched || self.delta.created.contains(&dir) {
            None
        } else {
            self.base.entry(dir, name)?
        };
        self.delta.set_entry(dir, name, before, after);
        Ok(())
    }

    fn put_links(&mut self, ino: Ino, after: Links) -> Step<()> {
        let before = if self.delta.links.contains_key(&ino) || self.delta.created.contains(&ino) {
            None
        } else {
            Some(self.base.backptrs(ino)?)
        };
        self.delta.set_links(ino, before, after);
        Ok(())
    }

    fn touch(&mut self, dir: Ino) -> Step<()> {
        let mut inode = self.must_inode(dir)?;
        inode.mtime = self.now;
        inode.version = self.version;
        self.put_inode(dir, inode)
    }

    fn alloc(&mut self) -> Ino {
        let ino = self.next_ino;
        self.next_ino += 1;
        self.delta.created.insert(ino);
        ino
    }

    fn entry_for(&self, child: Ino, kind: Kind) -> Entry {
        Entry {
            child,
            kind,
            version: self.version,
        }
    }

    /// Lowers a file's link count after removing its link from `dir`,
    /// freeing it at zero.
    fn drop_link(&mut self, ino: Ino, mut inode: Inode, dir: Ino) -> Step<()> {
        inode.nlink -= 1;
        inode.version = self.version;
        if inode.nlink == 0 {
            inode.life = Life::Freed(self.version);
            self.free(Freed::Inode(ino), &[dir])?;
        }
        self.put_inode(ino, inode)
    }

    fn free_dir(&mut self, ino: Ino, mut inode: Inode) -> Step<()> {
        inode.life = Life::Freed(self.version);
        inode.version = self.version;
        self.free(Freed::Inode(ino), &[inode.parent])?;
        self.put_inode(ino, inode)
    }

    /// Queues a record for freeing, with every directory above `holders`
    /// as the batch has them now.
    fn free(&mut self, freed: Freed, holders: &[Ino]) -> Step<()> {
        let mut above = BTreeSet::new();
        for dir in holders {
            above.extend(self.chain(*dir)?);
        }
        self.delta.freed.push(freed);
        self.delta
            .ancestors
            .insert(freed, above.into_iter().collect());
        Ok(())
    }

    // Ops.

    fn mkdir(&mut self, path: &Path, parents: bool, if_absent: bool) -> Step<Option<Ino>> {
        let names = &path.names;
        if names.is_empty() {
            return if if_absent {
                Ok(Some(self.root))
            } else {
                fail(Error::Exists)
            };
        }
        let mut dir = self.root;
        let mut depth = 0;
        while depth < names.len() {
            match self.entry(dir, &names[depth])? {
                None => break,
                Some(entry) if depth + 1 == names.len() => {
                    return if entry.kind == Kind::Dir && if_absent {
                        Ok(Some(entry.child))
                    } else {
                        fail(Error::Exists)
                    };
                }
                Some(entry) if entry.kind == Kind::File => return fail(Error::NotDir),
                Some(entry) => {
                    dir = entry.child;
                    depth += 1;
                }
            }
        }
        if depth + 1 < names.len() && !parents {
            return fail(Error::NotFound);
        }
        self.must_inode(dir)?;
        for name in &names[depth..] {
            let ino = self.alloc();
            self.delta
                .set_inode(ino, None, Inode::dir(self.version, self.now, dir));
            let entry = self.entry_for(ino, Kind::Dir);
            self.put_entry(dir, name, Some(entry))?;
            self.touch(dir)?;
            dir = ino;
        }
        Ok(Some(dir))
    }

    fn put(
        &mut self,
        path: &Path,
        value: &Bytes,
        mode: PutMode,
        mtime: Option<Millis>,
    ) -> Step<Option<Ino>> {
        if value.len() > MAX_VALUE {
            return fail(Error::TooLarge);
        }
        let Some((parents, name)) = path.split_last() else {
            return fail(Error::IsDir);
        };
        let dir = self.walk(parents)?;
        let mtime = mtime.unwrap_or(self.now);
        match self.entry(dir, name)? {
            None => {
                if mode == PutMode::Replace {
                    return fail(Error::NotFound);
                }
                self.must_inode(dir)?;
                let ino = self.alloc();
                self.delta.set_inode(
                    ino,
                    None,
                    Inode::file(self.version, mtime, value.len() as u64),
                );
                self.delta.values.insert((ino, self.version), value.clone());
                let entry = self.entry_for(ino, Kind::File);
                self.put_entry(dir, name, Some(entry))?;
                self.put_links(ino, Links::from([(dir, Name::from(name))]))?;
                self.touch(dir)?;
                Ok(Some(ino))
            }
            Some(_) if mode == PutMode::Create => fail(Error::Exists),
            Some(entry) if entry.kind == Kind::Dir => fail(Error::IsDir),
            Some(entry) => {
                let mut inode = self.must_inode(entry.child)?;
                if inode.value != self.version {
                    let holders: Vec<Ino> = self
                        .links(entry.child)?
                        .iter()
                        .map(|(parent, _)| *parent)
                        .collect();
                    self.free(Freed::Value(entry.child, inode.value), &holders)?;
                }
                inode.version = self.version;
                inode.value = self.version;
                inode.size = value.len() as u64;
                inode.mtime = mtime;
                self.put_inode(entry.child, inode)?;
                self.delta
                    .values
                    .insert((entry.child, self.version), value.clone());
                Ok(Some(entry.child))
            }
        }
    }

    fn link(&mut self, from: &Path, to: &Path) -> Step<Option<Ino>> {
        // Both paths resolve before either's kind counts, as in Linux. The
        // root, as `from`, is a directory; as `to`, it exists.
        let source = match from.split_last() {
            Some((from_parents, from_name)) => {
                let from_dir = self.walk(from_parents)?;
                let Some(source) = self.entry(from_dir, from_name)? else {
                    return fail(Error::NotFound);
                };
                Some(source)
            }
            None => None,
        };
        let Some((to_parents, to_name)) = to.split_last() else {
            return fail(Error::Exists);
        };
        let to_dir = self.walk(to_parents)?;
        if self.entry(to_dir, to_name)?.is_some() {
            return fail(Error::Exists);
        }
        let Some(source) = source.filter(|source| source.kind == Kind::File) else {
            return fail(Error::IsDir);
        };
        let mut inode = self.must_inode(source.child)?;
        let mut links = self.links(source.child)?;
        self.must_inode(to_dir)?;
        inode.nlink += 1;
        inode.version = self.version;
        self.put_inode(source.child, inode)?;
        let entry = self.entry_for(source.child, Kind::File);
        self.put_entry(to_dir, to_name, Some(entry))?;
        links.insert((to_dir, Name::from(to_name)));
        self.put_links(source.child, links)?;
        self.touch(to_dir)?;
        Ok(Some(source.child))
    }

    /// The directory holding `path`'s last name and the entry there, or
    /// None when the name is absent.
    fn find(&self, path: &Path) -> Step<Option<(Ino, Entry)>> {
        let Some((parents, name)) = path.split_last() else {
            return fail(Error::Invalid);
        };
        let dir = match self.walk(parents) {
            Err(Halt::Fail(Error::NotFound)) => return Ok(None),
            other => other?,
        };
        Ok(self.entry(dir, name)?.map(|entry| (dir, entry)))
    }

    fn absent(if_exists: bool) -> Step<Option<Ino>> {
        if if_exists {
            Ok(None)
        } else {
            fail(Error::NotFound)
        }
    }

    fn unlink(&mut self, path: &Path, if_exists: bool) -> Step<Option<Ino>> {
        let Some((dir, entry)) = self.find(path)? else {
            return Self::absent(if_exists);
        };
        if entry.kind == Kind::Dir {
            return fail(Error::IsDir);
        }
        let name = path.split_last().expect("found has a name").1;
        self.unlink_file(dir, name, entry)?;
        Ok(Some(entry.child))
    }

    fn unlink_file(&mut self, dir: Ino, name: &[u8], entry: Entry) -> Step<()> {
        let inode = self.must_inode(entry.child)?;
        let mut links = self.links(entry.child)?;
        self.must_inode(dir)?;
        self.put_entry(dir, name, None)?;
        links.remove(&(dir, Name::from(name)));
        self.put_links(entry.child, links)?;
        self.touch(dir)?;
        self.drop_link(entry.child, inode, dir)
    }

    fn rmdir(&mut self, path: &Path, if_exists: bool) -> Step<Option<Ino>> {
        let Some((dir, entry)) = self.find(path)? else {
            return Self::absent(if_exists);
        };
        if entry.kind == Kind::File {
            return fail(Error::NotDir);
        }
        if self.has_entries(entry.child)? {
            return fail(Error::NotEmpty);
        }
        let inode = self.must_inode(entry.child)?;
        self.must_inode(dir)?;
        let name = path.split_last().expect("found has a name").1;
        self.put_entry(dir, name, None)?;
        self.touch(dir)?;
        self.free_dir(entry.child, inode)?;
        Ok(Some(entry.child))
    }

    fn remove_all(&mut self, path: &Path, if_exists: bool) -> Step<Option<Ino>> {
        let Some((dir, entry)) = self.find(path)? else {
            return Self::absent(if_exists);
        };
        let name = path.split_last().expect("found has a name").1;
        if entry.kind == Kind::File {
            self.unlink_file(dir, name, entry)?;
            return Ok(Some(entry.child));
        }
        let mut inode = self.must_inode(entry.child)?;
        self.must_inode(dir)?;
        self.put_entry(dir, name, None)?;
        self.touch(dir)?;
        inode.life = Life::Detached(self.version);
        inode.version = self.version;
        self.put_inode(entry.child, inode)?;
        let above = self.chain(dir)?;
        self.delta.above.insert(entry.child, above);
        self.delta.detached.push(entry.child);
        Ok(Some(entry.child))
    }

    fn rename(&mut self, from: &Path, to: &Path, flag: RenameFlag) -> Step<Option<Ino>> {
        let (Some((from_parents, from_name)), Some((to_parents, to_name))) =
            (from.split_last(), to.split_last())
        else {
            return fail(Error::Invalid);
        };
        // Both parents resolve before the source's entry, as in Linux.
        let from_dir = self.walk(from_parents)?;
        let to_dir = self.walk(to_parents)?;
        let Some(source) = self.entry(from_dir, from_name)? else {
            return fail(Error::NotFound);
        };
        if from_dir == to_dir && from_name == to_name {
            // The target exists: it is the source.
            return match flag {
                RenameFlag::NoReplace => fail(Error::Exists),
                _ => Ok(Some(source.child)),
            };
        }
        let target = self.entry(to_dir, to_name)?;
        let ends = Ends {
            from_dir,
            from_name,
            source,
            to_dir,
            to_name,
        };
        match (flag, target) {
            (RenameFlag::NoReplace, Some(_)) => fail(Error::Exists),
            (RenameFlag::Exchange, None) => fail(Error::NotFound),
            (RenameFlag::Exchange, Some(target)) => self.exchange(ends, target),
            (_, Some(target)) if target.child == source.child => Ok(Some(source.child)),
            (_, target) => self.move_entry(ends, target),
        }
    }

    fn move_entry(&mut self, ends: Ends<'_>, target: Option<Entry>) -> Step<Option<Ino>> {
        let Ends {
            from_dir,
            from_name,
            source,
            to_dir,
            to_name,
        } = ends;
        if source.kind == Kind::Dir && self.is_under(to_dir, source.child)? {
            return fail(Error::Invalid);
        }
        // A target directory holding the source isn't empty, whatever the
        // source's kind; Linux checks this before the kinds.
        if let Some(target) = target
            && target.kind == Kind::Dir
            && self.is_under(from_dir, target.child)?
        {
            return fail(Error::NotEmpty);
        }
        if let Some(target) = target {
            match (source.kind, target.kind) {
                (Kind::Dir, Kind::File) => return fail(Error::NotDir),
                (Kind::File, Kind::Dir) => return fail(Error::IsDir),
                (Kind::Dir, Kind::Dir) if self.has_entries(target.child)? => {
                    return fail(Error::NotEmpty);
                }
                _ => {}
            }
        }
        let source_inode = self.must_inode(source.child)?;
        let source_links = match source.kind {
            Kind::File => Some(self.links(source.child)?),
            Kind::Dir => None,
        };
        let replaced = match target {
            Some(target) => {
                let inode = self.must_inode(target.child)?;
                let links = match target.kind {
                    Kind::File => Some(self.links(target.child)?),
                    Kind::Dir => None,
                };
                Some((target, inode, links))
            }
            None => None,
        };
        self.must_inode(from_dir)?;
        self.must_inode(to_dir)?;

        self.put_entry(from_dir, from_name, None)?;
        let moved = self.entry_for(source.child, source.kind);
        self.put_entry(to_dir, to_name, Some(moved))?;
        match source_links {
            Some(mut links) => {
                links.remove(&(from_dir, Name::from(from_name)));
                links.insert((to_dir, Name::from(to_name)));
                self.put_links(source.child, links)?;
            }
            None if from_dir != to_dir => {
                let mut inode = source_inode;
                inode.parent = to_dir;
                inode.version = self.version;
                self.put_inode(source.child, inode)?;
            }
            None => {}
        }
        if let Some((target, inode, links)) = replaced {
            match links {
                Some(mut links) => {
                    links.remove(&(to_dir, Name::from(to_name)));
                    self.put_links(target.child, links)?;
                    self.drop_link(target.child, inode, to_dir)?;
                }
                None => self.free_dir(target.child, inode)?,
            }
        }
        self.touch(from_dir)?;
        if to_dir != from_dir {
            self.touch(to_dir)?;
        }
        Ok(Some(source.child))
    }

    fn exchange(&mut self, ends: Ends<'_>, target: Entry) -> Step<Option<Ino>> {
        let Ends {
            from_dir,
            from_name,
            source,
            to_dir,
            to_name,
        } = ends;
        if target.child == source.child {
            return Ok(Some(source.child));
        }
        if source.kind == Kind::Dir && self.is_under(to_dir, source.child)? {
            return fail(Error::Invalid);
        }
        if target.kind == Kind::Dir && self.is_under(from_dir, target.child)? {
            return fail(Error::Invalid);
        }
        let source_inode = self.must_inode(source.child)?;
        let target_inode = self.must_inode(target.child)?;
        let source_links = match source.kind {
            Kind::File => Some(self.links(source.child)?),
            Kind::Dir => None,
        };
        let target_links = match target.kind {
            Kind::File => Some(self.links(target.child)?),
            Kind::Dir => None,
        };
        self.must_inode(from_dir)?;
        self.must_inode(to_dir)?;

        let swapped_in = self.entry_for(target.child, target.kind);
        self.put_entry(from_dir, from_name, Some(swapped_in))?;
        let swapped_out = self.entry_for(source.child, source.kind);
        self.put_entry(to_dir, to_name, Some(swapped_out))?;
        let sides = [
            (
                source.child,
                source_inode,
                source_links,
                (from_dir, from_name),
                (to_dir, to_name),
            ),
            (
                target.child,
                target_inode,
                target_links,
                (to_dir, to_name),
                (from_dir, from_name),
            ),
        ];
        for (ino, mut inode, links, (old_dir, old_name), (new_dir, new_name)) in sides {
            match links {
                Some(mut links) => {
                    links.remove(&(old_dir, Name::from(old_name)));
                    links.insert((new_dir, Name::from(new_name)));
                    self.put_links(ino, links)?;
                }
                None if old_dir != new_dir => {
                    inode.parent = new_dir;
                    inode.version = self.version;
                    self.put_inode(ino, inode)?;
                }
                None => {}
            }
        }
        self.touch(from_dir)?;
        if to_dir != from_dir {
            self.touch(to_dir)?;
        }
        Ok(Some(source.child))
    }

    fn set_attr(
        &mut self,
        path: &Path,
        mtime: Option<Millis>,
        mode: Option<u32>,
    ) -> Step<Option<Ino>> {
        let (ino, mut inode) = self.resolve(path)?;
        if let Some(mtime) = mtime {
            inode.mtime = mtime;
        }
        if let Some(mode) = mode {
            inode.mode = mode;
        }
        inode.version = self.version;
        self.put_inode(ino, inode)?;
        Ok(Some(ino))
    }

    // The changefeed's routing for files.

    /// Reads the links of every changed file that the batch's changes don't
    /// already hold.
    fn finish_holders(&self, needs: &mut BTreeSet<Read>) {
        for (ino, change) in &self.delta.inodes {
            let is_file = change
                .after
                .or(change.before)
                .is_some_and(|inode| inode.kind == Kind::File);
            if is_file
                && !self.delta.links.contains_key(ino)
                && !self.delta.created.contains(ino)
                && let Err(read) = self.base.backptrs(*ino)
                && needs.len() < MAX_READS
            {
                needs.insert(read);
            }
        }
    }

    /// Every directory the changefeed routes the batch's effects by.
    fn routed_dirs(&self) -> BTreeSet<Ino> {
        let mut dirs: BTreeSet<Ino> = self.delta.entries.keys().copied().collect();
        for (ino, change) in &self.delta.inodes {
            if change
                .after
                .or(change.before)
                .is_some_and(|inode| inode.kind == Kind::Dir)
            {
                dirs.insert(*ino);
            }
        }
        for held in self.holders().into_values() {
            dirs.extend(held);
        }
        dirs
    }

    /// Each routed directory's chains before and after the batch, or the
    /// reads they need.
    fn chains(&self) -> Result<BTreeMap<Ino, Chains>, BTreeSet<Read>> {
        let after = After {
            delta: &self.delta,
            base: self.base,
        };
        let mut chains = BTreeMap::new();
        let mut needs = BTreeSet::new();
        // A detached subtree's chains go up to its top, then on through the
        // directories above the top as it detached: whoever saw the subtree
        // before it left sees it freed, and no one else does, wherever the
        // top's old parent lies now.
        let route = |view: &dyn View, dir: Ino| -> Lookup<Vec<Ino>> {
            let Some((top, above)) = &self.reaping else {
                return chain(view, dir);
            };
            let mut found = chain_until(view, dir, *top)?;
            if found.last() == Some(top) {
                found.extend(above);
            }
            Ok(found)
        };
        for dir in self.routed_dirs() {
            // A directory the batch created had no chain before it.
            let before = if self.delta.created.contains(&dir) {
                Ok(Vec::new())
            } else {
                route(self.base, dir)
            };
            match (before, route(&after, dir)) {
                (Ok(before), Ok(after)) => {
                    chains.insert(dir, Chains { before, after });
                }
                (before, after) => {
                    needs.extend(before.err());
                    needs.extend(after.err());
                }
            }
        }
        if needs.is_empty() {
            Ok(chains)
        } else {
            Err(needs)
        }
    }

    fn holders(&self) -> BTreeMap<Ino, BTreeSet<Ino>> {
        let mut holders = BTreeMap::new();
        for (ino, change) in &self.delta.inodes {
            let is_file = change
                .after
                .or(change.before)
                .is_some_and(|inode| inode.kind == Kind::File);
            if !is_file {
                continue;
            }
            let mut dirs = BTreeSet::new();
            match self.delta.links.get(ino) {
                Some(links) => {
                    for set in [&links.before, &links.after].into_iter().flatten() {
                        dirs.extend(set.iter().map(|(parent, _)| *parent));
                    }
                }
                None => {
                    if let Ok(links) = self.base.backptrs(*ino) {
                        dirs.extend(links.iter().map(|(parent, _)| *parent));
                    }
                }
            }
            holders.insert(*ino, dirs);
        }
        holders
    }
}

/// A rename's source and destination.
#[derive(Clone, Copy)]
struct Ends<'a> {
    from_dir: Ino,
    from_name: &'a [u8],
    source: Entry,
    to_dir: Ino,
    to_name: &'a [u8],
}
