//! Checks the tree's invariants over a full copy of storage.

use crate::auth::{KeyId, KeyRecord};
use crate::keys::{self, Parsed};
use crate::log::LogEntry;
use crate::ops::CommitResult;
use crate::types::{Entry, Ino, Inode, Kind, Life, ROOT, Version};
use crate::writer::Head;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Default)]
struct Tree {
    inodes: BTreeMap<Ino, Inode>,
    entries: BTreeMap<(Ino, Box<[u8]>), Entry>,
    backptrs: BTreeSet<(Ino, Ino, Box<[u8]>)>,
    values: BTreeMap<(Ino, Version), usize>,
    free_inodes: BTreeMap<Ino, Version>,
    free_values: BTreeSet<(Ino, Version)>,
    detached: BTreeMap<Ino, Version>,
    head: Option<Head>,
    /// Request records' parts, by request id.
    records: BTreeMap<u128, BTreeMap<u16, Vec<u8>>>,
    /// Expiry keys, by request id.
    expiries: BTreeMap<u128, Vec<u64>>,
    keys: BTreeMap<KeyId, KeyRecord>,
    handle_secret: Option<usize>,
}

/// Every invariant the stored tree breaks, or none.
pub fn check<'a>(pairs: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) -> Vec<String> {
    let mut problems = Vec::new();
    load(pairs, &mut problems).check(&mut problems);
    problems
}

/// Every invariant the stored tree and its log break, or none. The log
/// comes from the same snapshot as the pairs.
pub fn check_with_log<'a>(
    pairs: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    log: impl IntoIterator<Item = (Version, &'a [u8])>,
) -> Vec<String> {
    let mut problems = Vec::new();
    let tree = load(pairs, &mut problems);
    tree.check(&mut problems);
    let mut checked = LogCheck::default();
    for (version, bytes) in log {
        checked.entry(version, bytes);
    }
    problems.extend(checked.finish(tree.head.as_ref().map_or(0, |head| head.version)));
    problems
}

/// Checks a log an entry at a time, oldest first, so a long one needn't
/// fit in memory: each entry decodes and carries its own version, versions
/// run without gaps, and the newest is the head's.
#[derive(Default)]
pub struct LogCheck {
    newest: Option<Version>,
    problems: Vec<String>,
}

impl LogCheck {
    pub fn entry(&mut self, version: Version, bytes: &[u8]) {
        match LogEntry::decode(bytes) {
            Ok(entry) if entry.version == version => {}
            Ok(entry) => self.problems.push(format!(
                "log entry {version} carries version {}",
                entry.version
            )),
            Err(_) => self
                .problems
                .push(format!("log entry {version} doesn't decode")),
        }
        if let Some(last) = self.newest
            && version != last + 1
        {
            self.problems
                .push(format!("the log skips from {last} to {version}"));
        }
        self.newest = Some(version);
    }

    /// The problems found in a log that truncation cut while it was read,
    /// whose end then says nothing.
    pub fn cut(self) -> Vec<String> {
        self.problems
    }

    /// The problems found, for a store whose head is at `head`.
    pub fn finish(mut self, head: Version) -> Vec<String> {
        if let Some(newest) = self.newest
            && newest != head
        {
            self.problems
                .push(format!("the log ends at {newest}, the head at {head}"));
        }
        self.problems
    }
}

fn load<'a>(
    pairs: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    problems: &mut Vec<String>,
) -> Tree {
    let mut tree = Tree::default();
    for (key, value) in pairs {
        match keys::parse(key) {
            Some(Parsed::Inode(ino)) => match Inode::from_bytes(value) {
                Ok(inode) => {
                    tree.inodes.insert(ino, inode);
                }
                Err(_) => problems.push(format!("inode {ino} doesn't decode")),
            },
            Some(Parsed::Entry(dir, name)) => match Entry::from_bytes(value) {
                Ok(entry) => {
                    tree.entries.insert((dir, name.into()), entry);
                }
                Err(_) => problems.push(format!("entry {dir}/{name:?} doesn't decode")),
            },
            Some(Parsed::Backptr(ino, parent, name)) => {
                tree.backptrs.insert((ino, parent, name.into()));
            }
            Some(Parsed::Value(ino, version)) => {
                tree.values.insert((ino, version), value.len());
            }
            Some(Parsed::Free(version, ino, freed)) => {
                // Every chain of directories ends at the root.
                if !crate::delta::decode_ancestors(value).is_ok_and(|dirs| dirs.contains(&ROOT)) {
                    problems.push(format!(
                        "the free-queue entry for inode {ino} at {version} names no directories above it"
                    ));
                }
                match freed {
                    None => {
                        if tree.free_inodes.insert(ino, version).is_some() {
                            problems.push(format!("inode {ino} is queued twice"));
                        }
                    }
                    Some(value) => {
                        tree.free_values.insert((ino, value));
                    }
                }
            }
            Some(Parsed::Detached(version, ino)) => {
                tree.detached.insert(ino, version);
                if !crate::delta::decode_ancestors(value).is_ok_and(|dirs| dirs.contains(&ROOT)) {
                    problems.push(format!(
                        "the detach record for inode {ino} at {version} names no directories above it"
                    ));
                }
            }
            Some(Parsed::System(b'h', _)) => match Head::decode(value) {
                Ok(head) => tree.head = Some(head),
                Err(_) => problems.push("head doesn't decode".into()),
            },
            Some(Parsed::System(b'r', rest)) if rest.len() == 18 => {
                let id = u128::from_be_bytes(rest[..16].try_into().expect("16 bytes"));
                let part = u16::from_be_bytes(rest[16..].try_into().expect("2 bytes"));
                tree.records
                    .entry(id)
                    .or_default()
                    .insert(part, value.to_vec());
            }
            Some(Parsed::System(b't', rest)) if rest.len() == 24 => {
                let at = u64::from_be_bytes(rest[..8].try_into().expect("8 bytes"));
                let id = u128::from_be_bytes(rest[8..].try_into().expect("16 bytes"));
                tree.expiries.entry(id).or_default().push(at);
            }
            Some(Parsed::System(b'r' | b't', _)) => {
                problems.push(format!("malformed request key {key:?}"))
            }
            Some(Parsed::System(b'k', rest)) if rest.len() == 8 => {
                let id = u64::from_be_bytes(rest.try_into().expect("8 bytes"));
                match KeyRecord::decode(value) {
                    Ok(record) => {
                        tree.keys.insert(id, record);
                    }
                    Err(_) => problems.push(format!("key {id:016x} doesn't decode")),
                }
            }
            Some(Parsed::System(b'm', [])) => tree.handle_secret = Some(value.len()),
            Some(Parsed::System(b'k' | b'm', _)) => problems.push(format!("malformed key {key:?}")),
            Some(Parsed::System(..) | Parsed::Log) => {}
            None => problems.push(format!("unknown key {key:?}")),
        }
    }
    tree
}

impl Tree {
    /// Each key is rooted at a live directory in the tree and minted by a
    /// key that exists; the handle secret holds 32 bytes.
    fn check_keys(&self, problems: &mut Vec<String>) {
        if self.handle_secret != Some(32) {
            problems.push(format!(
                "the handle secret holds {:?} bytes",
                self.handle_secret
            ));
        }
        for (id, record) in &self.keys {
            if !self.in_tree(record.root) {
                problems.push(format!(
                    "key {id:016x} is rooted at {}, which isn't a live directory in the tree",
                    record.root
                ));
            }
            if let Some(minter) = record.minted_by
                && !self.keys.contains_key(&minter)
            {
                problems.push(format!(
                    "key {id:016x} was minted by missing key {minter:016x}"
                ));
            }
        }
    }

    /// Whether a directory and every directory above it are live.
    fn in_tree(&self, dir: Ino) -> bool {
        let mut current = dir;
        for _ in 0..self.inodes.len() + 1 {
            match self.inodes.get(&current) {
                Some(inode) if inode.kind == Kind::Dir && inode.is_live() => {}
                _ => return false,
            }
            if current == ROOT {
                return true;
            }
            current = self.inodes[&current].parent;
        }
        false
    }

    /// Each request record has its parts from zero, decodes, and has one
    /// expiry key; each expiry key has a record.
    fn check_requests(&self, head: &Head, problems: &mut Vec<String>) {
        for (id, parts) in &self.records {
            if !parts.keys().copied().eq(0..parts.len() as u16) {
                problems.push(format!("request {id:x} is missing parts"));
                continue;
            }
            let bytes: Vec<u8> = parts.values().flatten().copied().collect();
            match CommitResult::decode(&bytes) {
                Ok(result) if result.committed && result.version <= head.version => {}
                Ok(result) => problems.push(format!("request {id:x} records {result:?}")),
                Err(_) => problems.push(format!("request {id:x} doesn't decode")),
            }
            match self.expiries.get(id).map(Vec::len) {
                Some(1) => {}
                count => problems.push(format!(
                    "request {id:x} has {} expiry keys",
                    count.unwrap_or(0)
                )),
            }
        }
        for id in self.expiries.keys() {
            if !self.records.contains_key(id) {
                problems.push(format!("expiry key for request {id:x} without a record"));
            }
        }
    }

    fn check(&self, problems: &mut Vec<String>) {
        let Some(head) = &self.head else {
            if !self.inodes.is_empty() {
                problems.push("inodes without a head record".into());
            }
            return;
        };
        match self.inodes.get(&ROOT) {
            Some(root) if root.kind == Kind::Dir && root.parent == ROOT && root.is_live() => {}
            _ => problems.push("the root is missing or malformed".into()),
        }
        let mut naming: BTreeMap<Ino, Vec<(Ino, &[u8])>> = BTreeMap::new();
        for ((dir, name), entry) in &self.entries {
            naming.entry(entry.child).or_default().push((*dir, name));
            match self.inodes.get(dir) {
                Some(parent)
                    if parent.kind == Kind::Dir && !matches!(parent.life, Life::Freed(_)) =>
                {
                    if entry.version > parent.version {
                        problems.push(format!("entry {dir}/{name:?} is newer than its directory"));
                    }
                }
                _ => problems.push(format!("entry {dir}/{name:?} sits in no live directory")),
            }
            match self.inodes.get(&entry.child) {
                Some(child) if child.kind == entry.kind => {
                    if !child.is_live() {
                        problems.push(format!(
                            "entry {dir}/{name:?} names inode {} in state {:?}",
                            entry.child, child.life
                        ));
                    }
                }
                Some(_) => problems.push(format!("entry {dir}/{name:?} has the wrong kind")),
                None => problems.push(format!(
                    "entry {dir}/{name:?} names missing inode {}",
                    entry.child
                )),
            }
            if entry.version > head.version {
                problems.push(format!("entry {dir}/{name:?} is from the future"));
            }
        }
        self.check_requests(head, problems);
        self.check_keys(problems);
        for (ino, parent, name) in &self.backptrs {
            match self.entries.get(&(*parent, name.clone())) {
                Some(entry) if entry.child == *ino && entry.kind == Kind::File => {}
                _ => problems.push(format!(
                    "back-pointer {ino} -> {parent}/{name:?} has no entry"
                )),
            }
        }
        for (ino, inode) in &self.inodes {
            if *ino >= head.next_ino {
                problems.push(format!(
                    "inode {ino} is past the id counter {}",
                    head.next_ino
                ));
            }
            if inode.version > head.version {
                problems.push(format!("inode {ino} is from the future"));
            }
            let names = naming.get(ino).map_or(&[][..], Vec::as_slice);
            match inode.kind {
                Kind::Dir => self.check_dir(*ino, inode, names, problems),
                Kind::File => self.check_file(*ino, inode, names, problems),
            }
            match inode.life {
                Life::Freed(at) if self.free_inodes.get(ino) != Some(&at) => {
                    problems.push(format!("freed inode {ino} has no queue entry at {at}"));
                }
                Life::Detached(at) if self.detached.get(ino) != Some(&at) => {
                    problems.push(format!("detached inode {ino} has no detach record at {at}"));
                }
                _ => {}
            }
        }
        for (ino, at) in &self.free_inodes {
            if !matches!(self.inodes.get(ino), Some(inode) if inode.life == Life::Freed(*at)) {
                problems.push(format!("queued inode {ino} wasn't freed at {at}"));
            }
        }
        for (ino, at) in &self.detached {
            if !matches!(self.inodes.get(ino), Some(inode) if inode.life == Life::Detached(*at)) {
                problems.push(format!("detach record {ino} doesn't match its inode"));
            }
        }
        for (ino, version) in self.values.keys() {
            let current = self
                .inodes
                .get(ino)
                .is_some_and(|inode| inode.kind == Kind::File && inode.value == *version);
            if !current && !self.free_values.contains(&(*ino, *version)) {
                problems.push(format!(
                    "value {ino}@{version} is neither current nor queued"
                ));
            }
        }
        for (ino, version) in &self.free_values {
            if !self.values.contains_key(&(*ino, *version)) {
                problems.push(format!("queued value {ino}@{version} is missing"));
            }
            if self
                .inodes
                .get(ino)
                .is_some_and(|inode| inode.value == *version)
            {
                problems.push(format!("queued value {ino}@{version} is still current"));
            }
        }
        self.check_reachable(problems);
    }

    fn check_dir(
        &self,
        ino: Ino,
        inode: &Inode,
        names: &[(Ino, &[u8])],
        problems: &mut Vec<String>,
    ) {
        if ino == ROOT {
            if !names.is_empty() {
                problems.push("the root has an entry naming it".into());
            }
            return;
        }
        match inode.life {
            Life::Live => match names {
                [(dir, _)] if *dir == inode.parent => {}
                [(dir, name)] => problems.push(format!(
                    "directory {ino} sits in {dir}/{name:?} but names parent {}",
                    inode.parent
                )),
                _ => problems.push(format!(
                    "directory {ino} has {} entries naming it",
                    names.len()
                )),
            },
            Life::Detached(_) | Life::Freed(_) => {
                if !names.is_empty() {
                    problems.push(format!("removed directory {ino} is still named"));
                }
            }
        }
        if matches!(inode.life, Life::Freed(_))
            && self
                .entries
                .range((ino, Box::from(&[][..]))..)
                .next()
                .is_some_and(|((dir, _), _)| *dir == ino)
        {
            problems.push(format!("freed directory {ino} still has entries"));
        }
        if inode.life == Life::Live {
            let mut current = ino;
            let mut seen = BTreeSet::new();
            loop {
                if current == ROOT {
                    break;
                }
                if !seen.insert(current) {
                    problems.push(format!("directory {ino}'s parents loop"));
                    break;
                }
                match self.inodes.get(&current) {
                    Some(dir) if matches!(dir.life, Life::Detached(_)) => break,
                    Some(dir) => current = dir.parent,
                    None => {
                        problems.push(format!(
                            "directory {ino}'s parents lead to missing {current}"
                        ));
                        break;
                    }
                }
            }
        }
    }

    fn check_file(
        &self,
        ino: Ino,
        inode: &Inode,
        names: &[(Ino, &[u8])],
        problems: &mut Vec<String>,
    ) {
        let links = self
            .backptrs
            .range((ino, 0, Box::from(&[][..]))..)
            .take_while(|(file, _, _)| *file == ino)
            .count();
        if inode.nlink as usize != names.len() || links != names.len() {
            problems.push(format!(
                "file {ino} counts {} links, has {} entries and {links} back-pointers",
                inode.nlink,
                names.len()
            ));
        }
        match inode.life {
            Life::Live if inode.nlink == 0 => {
                problems.push(format!("live file {ino} has no links"))
            }
            Life::Freed(_) if inode.nlink != 0 => {
                problems.push(format!("freed file {ino} still has links"))
            }
            Life::Detached(_) => problems.push(format!("file {ino} is detached")),
            _ => {}
        }
        match self.values.get(&(ino, inode.value)) {
            Some(len) if *len as u64 == inode.size => {}
            Some(len) => problems.push(format!(
                "file {ino} has size {} but value {len}",
                inode.size
            )),
            None => problems.push(format!("file {ino} lacks its value {}", inode.value)),
        }
    }

    /// Every inode is reachable from the root, lies under a detached
    /// directory, or waits in the free queue.
    fn check_reachable(&self, problems: &mut Vec<String>) {
        let mut reached = BTreeSet::new();
        let mut queue: VecDeque<Ino> = VecDeque::from([ROOT]);
        queue.extend(self.detached.keys());
        while let Some(dir) = queue.pop_front() {
            if !reached.insert(dir) {
                continue;
            }
            for ((_, _), entry) in self
                .entries
                .range((dir, Box::from(&[][..]))..)
                .take_while(|((parent, _), _)| *parent == dir)
            {
                match entry.kind {
                    Kind::Dir => queue.push_back(entry.child),
                    Kind::File => {
                        reached.insert(entry.child);
                    }
                }
            }
        }
        for (ino, inode) in &self.inodes {
            if !reached.contains(ino) && !matches!(inode.life, Life::Freed(_)) {
                problems.push(format!("inode {ino} is unreachable and not queued"));
            }
        }
    }
}
