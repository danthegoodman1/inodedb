//! Reads: `Stat`, `Read` and `List` against a view.

use crate::delta::Freed;
use crate::types::{Entry, Error, Ino, Inode, Kind, Life, Name, Path, ROOT, Version};
use crate::view::{Lookup, Read, View};
use bytes::Bytes;
use std::ops::Bound;

/// The deepest ancestry a scope check follows.
const MAX_DEPTH: usize = 1 << 16;

/// What a listed entry costs beyond its name and value: its kind, handle,
/// versions and attributes, as a response encodes them.
pub const ENTRY_COST: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Path(Path),
    Ino(Ino),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {
    Stat(Target),
    /// A file's value: its current one, or the one stored under `version`.
    Read {
        target: Target,
        version: Option<Version>,
    },
    /// A directory's entries in name order, after `after`, with names
    /// starting with `prefix`: at most `limit` of them, ending sooner once
    /// they would cost more than `budget` bytes. Each entry costs its name,
    /// `ENTRY_COST`, and its value when values are asked for. A listing
    /// holds at least one entry whatever its cost. A listing in pages names
    /// the directory by inode after the first, so it follows the directory
    /// the first page listed.
    List {
        target: Target,
        after: Option<Name>,
        prefix: Option<Name>,
        limit: usize,
        budget: usize,
        attrs: bool,
        values: bool,
    },
    /// Several queries answered at one version.
    Many(Vec<Query>),
    /// What a claimed free-queue entry kept: its inode's record while
    /// storage holds it, and the value it frees. It lies within the root
    /// when `above`, the directories the entry recorded above it, holds
    /// the root, so it reads whatever became of the inode since.
    Kept {
        freed: Freed,
        above: Vec<Ino>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    pub name: Name,
    pub entry: Entry,
    pub inode: Option<Inode>,
    pub value: Option<Bytes>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryResult {
    Stat {
        ino: Ino,
        inode: Inode,
    },
    Read {
        ino: Ino,
        inode: Inode,
        value: Bytes,
    },
    List {
        /// The directory listed.
        dir: Ino,
        entries: Vec<Listed>,
        more: bool,
    },
    Kept {
        inode: Option<Inode>,
        value: Option<Bytes>,
    },
    Failed(Error),
    Many(Vec<QueryResult>),
}

/// The inode a path names, or why it names none.
pub fn resolve(view: &dyn View, root: Ino, path: &Path) -> Lookup<Result<(Ino, Inode), Error>> {
    let mut current = root;
    for (index, name) in path.names.iter().enumerate() {
        match view.entry(current, name)? {
            None => return Ok(Err(Error::NotFound)),
            Some(entry) if entry.kind == Kind::File && index + 1 < path.names.len() => {
                return Ok(Err(Error::NotDir));
            }
            Some(entry) => current = entry.child,
        }
    }
    Ok(view
        .inode(current)?
        .map(|inode| (current, inode))
        .ok_or(Error::NotFound))
}

fn target(view: &dyn View, root: Ino, target: &Target) -> Lookup<Result<(Ino, Inode), Error>> {
    match target {
        Target::Path(path) => resolve(view, root, path),
        Target::Ino(ino) => Ok(match view.inode(*ino)? {
            Some(inode) if within(view, root, *ino, &inode)? => Ok((*ino, inode)),
            _ => Err(Error::NotFound),
        }),
    }
}

/// Whether an inode lies at or under `root`: a directory through its
/// parents, a file through any of its links, and a freed inode through the
/// directories its free-queue entry recorded. Everything lies under the
/// tree's root.
pub fn within(view: &dyn View, root: Ino, ino: Ino, inode: &Inode) -> Lookup<bool> {
    if root == ROOT || ino == root {
        return Ok(true);
    }
    if let Life::Freed(version) = inode.life {
        return Ok(view
            .free_entry(version, ino)?
            .is_some_and(|above| above.contains(&root)));
    }
    if let Life::Detached(version) = inode.life {
        return detached_within(view, root, version, ino);
    }
    match inode.kind {
        Kind::Dir => dir_within(view, root, inode.parent),
        Kind::File => {
            for (parent, _) in view.backptrs(ino)? {
                if dir_within(view, root, parent)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn dir_within(view: &dyn View, root: Ino, dir: Ino) -> Lookup<bool> {
    let mut current = dir;
    for _ in 0..MAX_DEPTH {
        if current == root {
            return Ok(true);
        }
        if current == ROOT {
            return Ok(false);
        }
        match view.inode(current)? {
            // A removed subtree lies under what lay above its top as it
            // went, wherever its old parent has moved since.
            Some(Inode {
                life: Life::Detached(version),
                ..
            }) => return detached_within(view, root, version, current),
            Some(inode) => current = inode.parent,
            None => return Ok(false),
        }
    }
    Ok(false)
}

/// Whether `root` lay above the detached top `ino` as it went.
fn detached_within(view: &dyn View, root: Ino, version: Version, ino: Ino) -> Lookup<bool> {
    Ok(view
        .detach_entry(version, ino)?
        .is_some_and(|above| above.contains(&root)))
}

pub fn run(view: &dyn View, root: Ino, query: &Query) -> Result<QueryResult, Vec<Read>> {
    let single = |read| vec![read];
    match query {
        Query::Many(queries) => {
            let mut results = Vec::with_capacity(queries.len());
            let mut needs = Vec::new();
            for query in queries {
                match run(view, root, query) {
                    Ok(result) => results.push(result),
                    Err(reads) => needs.extend(reads),
                }
            }
            if needs.is_empty() {
                Ok(QueryResult::Many(results))
            } else {
                needs.sort();
                needs.dedup();
                Err(needs)
            }
        }
        Query::Kept { freed, above } => {
            if root != ROOT && !above.contains(&root) {
                return Ok(QueryResult::Failed(Error::NotFound));
            }
            let (ino, value) = match *freed {
                Freed::Inode(ino) => (ino, None),
                Freed::Value(ino, version) => (ino, Some(version)),
            };
            let inode = view.inode(ino).map_err(single)?;
            let value = match value.or(inode
                .filter(|inode| inode.kind == Kind::File)
                .map(|inode| inode.value))
            {
                Some(version) => view.value(ino, version).map_err(single)?,
                None => None,
            };
            Ok(QueryResult::Kept { inode, value })
        }
        Query::Stat(which) => Ok(match target(view, root, which).map_err(single)? {
            Ok((ino, inode)) => QueryResult::Stat { ino, inode },
            Err(error) => QueryResult::Failed(error),
        }),
        Query::Read {
            target: which,
            version,
        } => {
            let (ino, inode) = match target(view, root, which).map_err(single)? {
                Ok(found) => found,
                Err(error) => return Ok(QueryResult::Failed(error)),
            };
            if inode.kind == Kind::Dir {
                return Ok(QueryResult::Failed(Error::IsDir));
            }
            let version = version.unwrap_or(inode.value);
            Ok(match view.value(ino, version).map_err(single)? {
                Some(value) => QueryResult::Read { ino, inode, value },
                None => QueryResult::Failed(Error::NotFound),
            })
        }
        Query::List {
            target: which,
            after,
            prefix,
            limit,
            budget,
            attrs,
            values,
        } => {
            let (dir, inode) = match target(view, root, which).map_err(single)? {
                Ok(found) => found,
                Err(error) => return Ok(QueryResult::Failed(error)),
            };
            if inode.kind != Kind::Dir {
                return Ok(QueryResult::Failed(Error::NotDir));
            }
            // Names with a prefix sit together, so the listing starts at the
            // later of `after` and `prefix` and ends at the first name
            // without the prefix.
            let start = match (after, prefix) {
                (Some(after), Some(prefix)) if prefix > after => Bound::Included(&prefix[..]),
                (Some(after), _) => Bound::Excluded(&after[..]),
                (None, Some(prefix)) => Bound::Included(&prefix[..]),
                (None, None) => Bound::Unbounded,
            };
            let page = view
                .listing(dir, start, limit.saturating_add(1))
                .map_err(single)?;
            let mut entries: Vec<Listed> = page
                .into_iter()
                .take_while(|(name, _)| {
                    prefix
                        .as_ref()
                        .is_none_or(|prefix| name.starts_with(prefix))
                })
                .map(|(name, entry)| Listed {
                    name,
                    entry,
                    inode: None,
                    value: None,
                })
                .collect();
            let mut more = entries.len() > *limit;
            entries.truncate(*limit);
            if *attrs || *values {
                let needs: Vec<Read> = entries
                    .iter_mut()
                    .filter_map(|listed| match view.inode(listed.entry.child) {
                        Ok(inode) => {
                            listed.inode = inode;
                            None
                        }
                        Err(read) => Some(read),
                    })
                    .collect();
                if !needs.is_empty() {
                    return Err(needs);
                }
            }
            let mut spent = 0usize;
            let mut fits = 0;
            for listed in &entries {
                let value = match listed.inode {
                    Some(inode) if *values && inode.kind == Kind::File => inode.size as usize,
                    _ => 0,
                };
                let cost = listed.name.len() + ENTRY_COST + value;
                if fits > 0 && spent + cost > *budget {
                    break;
                }
                spent += cost;
                fits += 1;
            }
            if fits < entries.len() {
                entries.truncate(fits);
                more = true;
            }
            if !*attrs && !*values {
                return Ok(QueryResult::List { dir, entries, more });
            }
            let mut needs = Vec::new();
            if *values {
                for listed in &mut entries {
                    if let Some(inode) = listed.inode
                        && inode.kind == Kind::File
                    {
                        match view.value(listed.entry.child, inode.value) {
                            Ok(value) => listed.value = value,
                            Err(read) => needs.push(read),
                        }
                    }
                }
            }
            if !needs.is_empty() {
                return Err(needs);
            }
            if !*attrs {
                for listed in &mut entries {
                    listed.inode = None;
                }
            }
            Ok(QueryResult::List { dir, entries, more })
        }
    }
}
