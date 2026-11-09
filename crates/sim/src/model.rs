//! A deliberately naive model of InodeDB: a map from full path to node,
//! whose renames rewrite every path under the source. It evaluates ops and
//! conditions itself and shares no code with the core's execution.

use inodedb_core::Bytes;
use inodedb_core::ops::{Batch, Op, Outcome, Pred, PredSource, PutMode, RenameFlag, Skip};
use inodedb_core::query::{ENTRY_COST, Query, Target};
use inodedb_core::types::{Error, Kind, MAX_VALUE, Millis, Name, Version};
use std::collections::{BTreeMap, BTreeSet};

pub type NodeId = u64;
pub const MODEL_ROOT: NodeId = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub kind: Kind,
    pub version: Version,
    pub mtime: Millis,
    pub mode: u32,
    pub value: Bytes,
    /// Links inside subtrees that `RemoveAll` detached.
    pub detached_links: u32,
    /// The directories above those links as each detached: while they
    /// hold it, or once freeing the last of them freed it, they bound who
    /// reaches it by inode.
    pub detached_under: Vec<NodeId>,
    /// The latest version that detached a subtree linking it.
    pub detached_at: Version,
    /// The directories above the names it lost at `lost_at`, the version
    /// it last lost one: once it has no name, they bound who reaches it by
    /// inode.
    pub lost_under: Vec<NodeId>,
    pub lost_at: Version,
    pub removed: bool,
}

/// What a path names: a node, and the version that created its entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Named {
    pub node: NodeId,
    pub entry_version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelOutcome {
    Applied(Option<NodeId>),
    Skipped(Skip),
}

#[derive(Clone, Debug)]
pub struct Model {
    pub version: Version,
    paths: BTreeMap<Vec<Name>, Named>,
    pub nodes: BTreeMap<NodeId, Node>,
    next: NodeId,
}

type Path = Vec<Name>;

fn under(path: &[Name], ancestor: &[Name]) -> bool {
    path.starts_with(ancestor)
}

impl Model {
    /// The tree after version 1: the root alone.
    pub fn new(root_mtime: Millis) -> Model {
        Model {
            version: 1,
            paths: BTreeMap::new(),
            nodes: BTreeMap::from([(
                MODEL_ROOT,
                Node {
                    kind: Kind::Dir,
                    version: 1,
                    mtime: root_mtime,
                    mode: 0,
                    value: Bytes::new(),
                    detached_links: 0,
                    detached_under: Vec::new(),
                    detached_at: 0,
                    lost_under: Vec::new(),
                    lost_at: 0,
                    removed: false,
                },
            )]),
            next: MODEL_ROOT + 1,
        }
    }

    /// Moves to a version that changed nothing in the tree, as a key op's.
    pub fn advance(&mut self, version: Version) {
        self.version = version;
    }

    pub fn path_of(&self, node: NodeId) -> Option<Path> {
        if node == MODEL_ROOT {
            return Some(Vec::new());
        }
        self.paths
            .iter()
            .find(|(_, named)| named.node == node)
            .map(|(path, _)| path.clone())
    }

    /// Every path that names `node`; the root's is empty.
    pub fn paths_of(&self, node: NodeId) -> Vec<Path> {
        if node == MODEL_ROOT {
            return vec![Vec::new()];
        }
        self.paths
            .iter()
            .filter(|(_, named)| named.node == node)
            .map(|(path, _)| path.clone())
            .collect()
    }

    /// The node `path` names from the tree's root.
    pub fn node_at_path(&self, path: &[Name]) -> Option<NodeId> {
        self.named(path).map(|named| named.node)
    }

    fn named(&self, path: &[Name]) -> Option<Named> {
        if path.is_empty() {
            return Some(Named {
                node: MODEL_ROOT,
                entry_version: 0,
            });
        }
        self.paths.get(path).copied()
    }

    pub fn is_dir(&self, node: NodeId) -> bool {
        self.kind(node) == Kind::Dir
    }

    fn kind(&self, node: NodeId) -> Kind {
        self.nodes[&node].kind
    }

    /// The directory a path's names lead to, from `root`.
    fn walk_dir(&self, root: &[Name], names: &[Name]) -> Result<Path, Error> {
        let mut path = root.to_vec();
        for name in names {
            path.push(name.clone());
            match self.named(&path) {
                None => return Err(Error::NotFound),
                Some(named) if self.kind(named.node) == Kind::File => return Err(Error::NotDir),
                Some(_) => {}
            }
        }
        Ok(path)
    }

    /// The node a path names.
    fn lookup(&self, root: &[Name], names: &[Name]) -> Result<(Path, NodeId), Error> {
        let Some((last, parents)) = names.split_last() else {
            let named = self.named(root).ok_or(Error::NotFound)?;
            return Ok((root.to_vec(), named.node));
        };
        let mut path = self.walk_dir(root, parents)?;
        path.push(last.clone());
        match self.named(&path) {
            Some(named) => Ok((path, named.node)),
            None => Err(Error::NotFound),
        }
    }

    fn stat(&self, root: &[Name], names: &[Name]) -> Option<(Path, NodeId)> {
        self.lookup(root, names).ok()
    }

    fn children(&self, dir: &[Name]) -> Vec<(Name, Named)> {
        self.paths
            .range(dir.to_vec()..)
            .take_while(|(path, _)| under(path, dir))
            .filter(|(path, _)| path.len() == dir.len() + 1)
            .map(|(path, named)| (path.last().expect("a child has a name").clone(), *named))
            .collect()
    }

    /// Every node a path names.
    pub fn named_nodes(&self) -> BTreeSet<NodeId> {
        self.paths.values().map(|named| named.node).collect()
    }

    pub fn nlink(&self, node: NodeId) -> u32 {
        let named = self
            .paths
            .values()
            .filter(|named| named.node == node)
            .count() as u32;
        named + self.nodes[&node].detached_links
    }

    fn touch(&mut self, dir: &[Name], version: Version, now: Millis) {
        let node = self.named(dir).expect("a touched directory exists").node;
        let node = self.nodes.get_mut(&node).expect("named nodes exist");
        node.mtime = now;
        node.version = version;
    }

    fn create(&mut self, kind: Kind, version: Version, mtime: Millis, value: Bytes) -> NodeId {
        let id = self.next;
        self.next += 1;
        self.nodes.insert(
            id,
            Node {
                kind,
                version,
                mtime,
                mode: 0,
                value,
                detached_links: 0,
                detached_under: Vec::new(),
                detached_at: 0,
                lost_under: Vec::new(),
                lost_at: 0,
                removed: false,
            },
        );
        id
    }

    /// The directories above `path`'s name.
    fn above(&self, path: &[Name]) -> Vec<NodeId> {
        (0..path.len())
            .filter_map(|depth| self.named(&path[..depth]).map(|named| named.node))
            .collect()
    }

    /// Removes `path`'s name, keeping it among the names its node lost at
    /// `version`.
    fn drop_name(&mut self, path: &[Name], version: Version) -> Option<Named> {
        let above = self.above(path);
        let named = self.paths.remove(path)?;
        let node = self.nodes.get_mut(&named.node).expect("named nodes exist");
        if node.lost_at != version {
            node.lost_under.clear();
            node.lost_at = version;
        }
        node.lost_under.extend(above);
        Some(named)
    }

    fn bump(&mut self, node: NodeId, version: Version) {
        self.nodes.get_mut(&node).expect("node exists").version = version;
    }

    /// Removes one name of a file, freeing the file with its last link.
    fn unlink_path(&mut self, path: &[Name], version: Version) -> NodeId {
        let named = self.drop_name(path, version).expect("the path exists");
        self.bump(named.node, version);
        if self.nlink(named.node) == 0 {
            self.nodes
                .get_mut(&named.node)
                .expect("node exists")
                .removed = true;
        }
        named.node
    }

    /// Re-roots every path under `from` at `to`, keeping descendants' entry
    /// versions; the top entry is new.
    fn move_tree(&mut self, from: &[Name], to: &[Name], version: Version) {
        let moved: Vec<(Path, Named)> = self
            .paths
            .range(from.to_vec()..)
            .take_while(|(path, _)| under(path, from))
            .map(|(path, named)| (path.clone(), *named))
            .collect();
        for (path, _) in &moved {
            self.paths.remove(path);
        }
        for (path, mut named) in moved {
            let mut target = to.to_vec();
            target.extend_from_slice(&path[from.len()..]);
            if path.len() == from.len() {
                named.entry_version = version;
            }
            self.paths.insert(target, named);
        }
    }

    fn holds(&self, root: &[Name], pred: &Pred, outcomes: &[ModelOutcome]) -> bool {
        match pred {
            Pred::Exists(path) => self.stat(root, &path.names).is_some(),
            Pred::Absent(path) => self.stat(root, &path.names).is_none(),
            Pred::IsFile(path) => self
                .stat(root, &path.names)
                .is_some_and(|(_, node)| self.kind(node) == Kind::File),
            Pred::IsDir(path) => self
                .stat(root, &path.names)
                .is_some_and(|(_, node)| self.kind(node) == Kind::Dir),
            Pred::Empty(path) => self.stat(root, &path.names).is_some_and(|(found, node)| {
                self.kind(node) == Kind::Dir && self.children(&found).is_empty()
            }),
            Pred::VersionIs(path, version) => self
                .stat(root, &path.names)
                .is_some_and(|(_, node)| self.nodes[&node].version == *version),
            Pred::Applied(index) => {
                matches!(outcomes.get(*index), Some(ModelOutcome::Applied(_)))
            }
        }
    }

    /// Applies a batch at `version`. Returns each op's outcome and whether
    /// anything changed; a batch that changes nothing leaves the model as
    /// it was.
    pub fn apply(
        &mut self,
        root: &[Name],
        batch: &Batch,
        version: Version,
        now: Millis,
    ) -> (Vec<ModelOutcome>, bool) {
        let before = self.clone();
        let mut outcomes = Vec::with_capacity(batch.ops.len());
        let mut changed = false;
        let mut first_skip = None;
        for (index, spec) in batch.ops.iter().enumerate() {
            let preds = batch
                .when
                .iter()
                .enumerate()
                .map(|(i, pred)| (PredSource::When(i), pred))
                .chain(
                    spec.cond
                        .iter()
                        .enumerate()
                        .map(|(i, pred)| (PredSource::Cond(i), pred)),
                );
            let mut outcome = None;
            for (source, pred) in preds {
                if !self.holds(root, pred, &outcomes) {
                    outcome = Some(ModelOutcome::Skipped(Skip::Pred(source)));
                    break;
                }
            }
            let outcome = match outcome {
                Some(skipped) => skipped,
                None => match self.op(root, &spec.op, version, now) {
                    Ok((node, did_change)) => {
                        changed |= did_change;
                        ModelOutcome::Applied(node)
                    }
                    Err(error) => ModelOutcome::Skipped(Skip::Error(error)),
                },
            };
            if !matches!(outcome, ModelOutcome::Applied(_)) && first_skip.is_none() {
                first_skip = Some(index);
            }
            outcomes.push(outcome);
            if batch.all_or_nothing && first_skip.is_some() {
                break;
            }
        }
        if batch.all_or_nothing
            && let Some(skipped) = first_skip
        {
            *self = before;
            let reason = outcomes[skipped].clone();
            let outcomes = (0..batch.ops.len())
                .map(|index| {
                    if index == skipped {
                        reason.clone()
                    } else {
                        ModelOutcome::Skipped(Skip::Aborted)
                    }
                })
                .collect();
            return (outcomes, false);
        }
        if changed {
            self.version = version;
        } else {
            *self = before;
        }
        (outcomes, changed)
    }

    /// One op: the node it touched, and whether it changed anything.
    fn op(
        &mut self,
        root: &[Name],
        op: &Op,
        version: Version,
        now: Millis,
    ) -> Result<(Option<NodeId>, bool), Error> {
        match op {
            Op::Mkdir {
                path,
                parents,
                if_absent,
            } => {
                let names = &path.names;
                if names.is_empty() {
                    return if *if_absent {
                        Ok((Some(self.named(root).expect("roots exist").node), false))
                    } else {
                        Err(Error::Exists)
                    };
                }
                let mut current = root.to_vec();
                let mut depth = 0;
                while depth < names.len() {
                    let mut next = current.clone();
                    next.push(names[depth].clone());
                    match self.named(&next) {
                        None => break,
                        Some(named) if depth + 1 == names.len() => {
                            return if self.kind(named.node) == Kind::Dir && *if_absent {
                                Ok((Some(named.node), false))
                            } else {
                                Err(Error::Exists)
                            };
                        }
                        Some(named) if self.kind(named.node) == Kind::File => {
                            return Err(Error::NotDir);
                        }
                        Some(_) => {
                            current = next;
                            depth += 1;
                        }
                    }
                }
                if depth + 1 < names.len() && !*parents {
                    return Err(Error::NotFound);
                }
                let mut node = 0;
                for name in &names[depth..] {
                    node = self.create(Kind::Dir, version, now, Bytes::new());
                    self.touch(&current, version, now);
                    current.push(name.clone());
                    self.paths.insert(
                        current.clone(),
                        Named {
                            node,
                            entry_version: version,
                        },
                    );
                }
                Ok((Some(node), true))
            }
            Op::Put {
                path,
                value,
                mode,
                mtime,
            } => {
                if value.len() > MAX_VALUE {
                    return Err(Error::TooLarge);
                }
                let Some((last, parents)) = path.names.split_last() else {
                    return Err(Error::IsDir);
                };
                let dir = self.walk_dir(root, parents)?;
                let mut full = dir.clone();
                full.push(last.clone());
                let mtime = mtime.unwrap_or(now);
                match self.named(&full) {
                    None => {
                        if *mode == PutMode::Replace {
                            return Err(Error::NotFound);
                        }
                        let node = self.create(Kind::File, version, mtime, value.clone());
                        self.paths.insert(
                            full,
                            Named {
                                node,
                                entry_version: version,
                            },
                        );
                        self.touch(&dir, version, now);
                        Ok((Some(node), true))
                    }
                    Some(_) if *mode == PutMode::Create => Err(Error::Exists),
                    Some(named) if self.kind(named.node) == Kind::Dir => Err(Error::IsDir),
                    Some(named) => {
                        let node = self.nodes.get_mut(&named.node).expect("node exists");
                        node.value = value.clone();
                        node.version = version;
                        node.mtime = mtime;
                        Ok((Some(named.node), true))
                    }
                }
            }
            Op::Link { from, to } => {
                let named = match from.names.split_last() {
                    Some((from_last, from_parents)) => {
                        let mut source = self.walk_dir(root, from_parents)?;
                        source.push(from_last.clone());
                        let Some(named) = self.named(&source) else {
                            return Err(Error::NotFound);
                        };
                        Some(named)
                    }
                    None => None,
                };
                let Some((to_last, to_parents)) = to.names.split_last() else {
                    return Err(Error::Exists);
                };
                let dir = self.walk_dir(root, to_parents)?;
                let mut target = dir.clone();
                target.push(to_last.clone());
                if self.named(&target).is_some() {
                    return Err(Error::Exists);
                }
                let Some(named) = named.filter(|named| self.kind(named.node) == Kind::File) else {
                    return Err(Error::IsDir);
                };
                self.paths.insert(
                    target,
                    Named {
                        node: named.node,
                        entry_version: version,
                    },
                );
                self.bump(named.node, version);
                self.touch(&dir, version, now);
                Ok((Some(named.node), true))
            }
            Op::Unlink { path, if_exists } => {
                let Some((dir, full, named)) = self.find(root, &path.names)? else {
                    return absent(*if_exists);
                };
                if self.kind(named.node) == Kind::Dir {
                    return Err(Error::IsDir);
                }
                self.touch(&dir, version, now);
                let node = self.unlink_path(&full, version);
                Ok((Some(node), true))
            }
            Op::Rmdir { path, if_exists } => {
                let Some((dir, full, named)) = self.find(root, &path.names)? else {
                    return absent(*if_exists);
                };
                if self.kind(named.node) == Kind::File {
                    return Err(Error::NotDir);
                }
                if !self.children(&full).is_empty() {
                    return Err(Error::NotEmpty);
                }
                self.drop_name(&full, version);
                self.touch(&dir, version, now);
                let node = self.nodes.get_mut(&named.node).expect("node exists");
                node.removed = true;
                node.version = version;
                Ok((Some(named.node), true))
            }
            Op::RemoveAll { path, if_exists } => {
                let Some((dir, full, named)) = self.find(root, &path.names)? else {
                    return absent(*if_exists);
                };
                self.touch(&dir, version, now);
                if self.kind(named.node) == Kind::File {
                    let node = self.unlink_path(&full, version);
                    return Ok((Some(node), true));
                }
                let doomed: Vec<(Path, Named)> = self
                    .paths
                    .range(full.clone()..)
                    .take_while(|(path, _)| under(path, &full))
                    .map(|(path, named)| (path.clone(), *named))
                    .collect();
                // Deepest first, so each name's directories still have
                // names as it goes.
                for (doomed_path, doomed_named) in doomed.into_iter().rev() {
                    let above = self.above(&doomed_path);
                    self.drop_name(&doomed_path, version);
                    if doomed_path.len() > full.len() && self.kind(doomed_named.node) == Kind::File
                    {
                        let node = self.nodes.get_mut(&doomed_named.node).expect("node exists");
                        node.detached_links += 1;
                        node.detached_under.extend(above);
                        node.detached_at = version;
                    }
                }
                let node = self.nodes.get_mut(&named.node).expect("node exists");
                node.removed = true;
                node.version = version;
                Ok((Some(named.node), true))
            }
            Op::Rename { from, to, flag } => {
                self.rename(root, &from.names, &to.names, *flag, version, now)
            }
            Op::SetAttr { path, mtime, mode } => {
                let (_, node) = self.lookup(root, &path.names)?;
                let node_ref = self.nodes.get_mut(&node).expect("node exists");
                if let Some(mtime) = mtime {
                    node_ref.mtime = *mtime;
                }
                if let Some(mode) = mode {
                    node_ref.mode = *mode;
                }
                node_ref.version = version;
                Ok((Some(node), true))
            }
        }
    }

    /// The directory, full path and entry for a path's last name, or None
    /// when absent.
    #[allow(clippy::type_complexity)]
    fn find(&self, root: &[Name], names: &[Name]) -> Result<Option<(Path, Path, Named)>, Error> {
        let Some((last, parents)) = names.split_last() else {
            return Err(Error::Invalid);
        };
        let dir = match self.walk_dir(root, parents) {
            Err(Error::NotFound) => return Ok(None),
            other => other?,
        };
        let mut full = dir.clone();
        full.push(last.clone());
        Ok(self.named(&full).map(|named| (dir, full, named)))
    }

    fn rename(
        &mut self,
        root: &[Name],
        from: &[Name],
        to: &[Name],
        flag: RenameFlag,
        version: Version,
        now: Millis,
    ) -> Result<(Option<NodeId>, bool), Error> {
        let (Some((from_last, from_parents)), Some((to_last, to_parents))) =
            (from.split_last(), to.split_last())
        else {
            return Err(Error::Invalid);
        };
        let from_dir = self.walk_dir(root, from_parents)?;
        let to_dir = self.walk_dir(root, to_parents)?;
        let mut source_path = from_dir.clone();
        source_path.push(from_last.clone());
        let Some(source) = self.named(&source_path) else {
            return Err(Error::NotFound);
        };
        let mut target_path = to_dir.clone();
        target_path.push(to_last.clone());
        if source_path == target_path {
            return match flag {
                RenameFlag::NoReplace => Err(Error::Exists),
                _ => Ok((Some(source.node), false)),
            };
        }
        let target = self.named(&target_path);
        let source_kind = self.kind(source.node);
        match (flag, target) {
            (RenameFlag::NoReplace, Some(_)) => Err(Error::Exists),
            (RenameFlag::Exchange, None) => Err(Error::NotFound),
            (RenameFlag::Exchange, Some(target)) => {
                if target.node == source.node {
                    return Ok((Some(source.node), false));
                }
                if source_kind == Kind::Dir && under(&to_dir, &source_path) {
                    return Err(Error::Invalid);
                }
                let target_kind = self.kind(target.node);
                if target_kind == Kind::Dir && under(&from_dir, &target_path) {
                    return Err(Error::Invalid);
                }
                // Swap through a scratch name no real path uses.
                let mut scratch = root.to_vec();
                scratch.push(Name::from(&b"\0swap"[..]));
                self.move_tree(&source_path, &scratch, version);
                self.move_tree(&target_path, &source_path, version);
                self.move_tree(&scratch, &target_path, version);
                if from_dir != to_dir {
                    if source_kind == Kind::Dir {
                        self.bump(source.node, version);
                    }
                    if target_kind == Kind::Dir {
                        self.bump(target.node, version);
                    }
                }
                self.touch(&from_dir, version, now);
                if to_dir != from_dir {
                    self.touch(&to_dir, version, now);
                }
                Ok((Some(source.node), true))
            }
            (_, Some(target)) if target.node == source.node => Ok((Some(source.node), false)),
            (_, target) => {
                if source_kind == Kind::Dir && under(&to_dir, &source_path) {
                    return Err(Error::Invalid);
                }
                if let Some(target) = &target
                    && self.kind(target.node) == Kind::Dir
                    && under(&from_dir, &target_path)
                {
                    return Err(Error::NotEmpty);
                }
                if let Some(target) = target {
                    match (source_kind, self.kind(target.node)) {
                        (Kind::Dir, Kind::File) => return Err(Error::NotDir),
                        (Kind::File, Kind::Dir) => return Err(Error::IsDir),
                        (Kind::Dir, Kind::Dir) if !self.children(&target_path).is_empty() => {
                            return Err(Error::NotEmpty);
                        }
                        _ => {}
                    }
                    match self.kind(target.node) {
                        Kind::File => {
                            self.unlink_path(&target_path, version);
                        }
                        Kind::Dir => {
                            self.drop_name(&target_path, version);
                            let node = self.nodes.get_mut(&target.node).expect("node exists");
                            node.removed = true;
                            node.version = version;
                        }
                    }
                }
                self.move_tree(&source_path, &target_path, version);
                if source_kind == Kind::Dir && from_dir != to_dir {
                    self.bump(source.node, version);
                }
                self.touch(&from_dir, version, now);
                if to_dir != from_dir {
                    self.touch(&to_dir, version, now);
                }
                Ok((Some(source.node), true))
            }
        }
    }

    /// Whether a batch is malformed, as the core must judge it.
    /// The node at a path.
    pub fn node_at(&self, path: &[Name]) -> Option<NodeId> {
        if path.is_empty() {
            return Some(MODEL_ROOT);
        }
        self.paths.get(path).map(|named| named.node)
    }

    pub fn malformed(batch: &Batch) -> bool {
        batch
            .when
            .iter()
            .any(|pred| matches!(pred, Pred::Applied(_)))
            || batch.ops.iter().enumerate().any(|(index, spec)| {
                spec.cond
                    .iter()
                    .any(|pred| matches!(pred, Pred::Applied(earlier) if *earlier >= index))
            })
    }

    pub fn query(&self, root: &[Name], query: &Query) -> ModelAnswer {
        let resolve = |target: &Target| -> Result<NodeId, Error> {
            match target {
                Target::Path(path) => self.lookup(root, &path.names).map(|(_, node)| node),
                Target::Ino(_) => unreachable!("the simulator maps handles before asking"),
            }
        };
        match query {
            Query::Many(queries) => ModelAnswer::Many(
                queries
                    .iter()
                    .map(|query| self.query(root, query))
                    .collect(),
            ),
            Query::Kept { .. } => unreachable!("the simulator's clients ask for paths"),
            Query::Stat(target) => match resolve(target) {
                Ok(node) => ModelAnswer::Stat(node),
                Err(error) => ModelAnswer::Failed(error),
            },
            Query::Read { target, .. } => match resolve(target) {
                Ok(node) if self.kind(node) == Kind::Dir => ModelAnswer::Failed(Error::IsDir),
                Ok(node) => ModelAnswer::Read(node, self.nodes[&node].value.clone()),
                Err(error) => ModelAnswer::Failed(error),
            },
            Query::List {
                target,
                after,
                prefix,
                limit,
                budget,
                values,
                ..
            } => match match target {
                Target::Path(path) => self.lookup(root, &path.names),
                Target::Ino(_) => unreachable!("the simulator lists by path"),
            } {
                Ok((_, node)) if self.kind(node) != Kind::Dir => ModelAnswer::Failed(Error::NotDir),
                Ok((found, _)) => {
                    let mut entries: Vec<(Name, Named)> = self
                        .children(&found)
                        .into_iter()
                        .filter(|(name, _)| after.as_ref().is_none_or(|after| name > after))
                        .filter(|(name, _)| {
                            prefix
                                .as_ref()
                                .is_none_or(|prefix| name.starts_with(prefix))
                        })
                        .collect();
                    let mut more = entries.len() > *limit;
                    entries.truncate(*limit);
                    let mut spent = 0;
                    for index in 0..entries.len() {
                        let (name, named) = &entries[index];
                        let node = &self.nodes[&named.node];
                        let value = if *values && node.kind == Kind::File {
                            node.value.len()
                        } else {
                            0
                        };
                        let cost = name.len() + ENTRY_COST + value;
                        if index > 0 && spent + cost > *budget {
                            entries.truncate(index);
                            more = true;
                            break;
                        }
                        spent += cost;
                    }
                    ModelAnswer::List(entries, more)
                }
                Err(error) => ModelAnswer::Failed(error),
            },
        }
    }
}

fn absent(if_exists: bool) -> Result<(Option<NodeId>, bool), Error> {
    if if_exists {
        Ok((None, false))
    } else {
        Err(Error::NotFound)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelAnswer {
    Stat(NodeId),
    Read(NodeId, Bytes),
    List(Vec<(Name, Named)>, bool),
    Failed(Error),
    Many(Vec<ModelAnswer>),
}

impl ModelOutcome {
    pub fn matches(&self, outcome: &Outcome, map: &mut crate::Mapping) -> Result<(), String> {
        match (self, outcome) {
            (ModelOutcome::Applied(None), Outcome::Applied { ino: None }) => Ok(()),
            (ModelOutcome::Applied(Some(node)), Outcome::Applied { ino: Some(ino) }) => {
                map.bind(*ino, *node)
            }
            (ModelOutcome::Skipped(expected), Outcome::Skipped(actual)) if expected == actual => {
                Ok(())
            }
            _ => Err(format!("expected {self:?}, got {outcome:?}")),
        }
    }
}
