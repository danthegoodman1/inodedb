//! The changefeed as a subtree's subscriber sees it: commits routed to the
//! subtree, and a cache kept current by applying them.

use crate::auth::{KeyId, KeyRecord, Keys};
use crate::cache::{Budget, Cache, rebase};
use crate::delta::{Acked, Change as Changed, Delta, Freed};
use crate::log::{EntryEffect, InodeEffect, LogEntry};
use crate::query::{self, Query, QueryResult};
use crate::snapshot::{Current, Kept, Miss, SnapshotView, Undo};
use crate::types::{Ino, Kind, Life, Millis, ROOT, Version};
use crate::view::Fact;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// What a subscriber to a subtree receives of one commit. A change with no
/// effects reports progress: nothing in the subtree changed up to
/// `version`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Change {
    pub version: Version,
    pub time: Millis,
    pub entries: Vec<EntryEffect>,
    pub inodes: Vec<InodeEffect>,
    /// Directories that moved into the subtree, whose contents the
    /// subscriber has never seen.
    pub entered: Vec<Ino>,
    /// Keys minted and revoked, which only the whole tree's follower sees.
    pub keys: Vec<(KeyId, Option<KeyRecord>)>,
    /// Free-queue entries acknowledged, which only the whole tree's
    /// follower sees.
    pub acked: Vec<Freed>,
}

impl Change {
    /// Every effect of a commit, as the whole tree's follower sees it.
    pub fn whole(entry: &LogEntry) -> Change {
        Change {
            version: entry.version,
            time: entry.time,
            entries: entry.entries.clone(),
            inodes: entry.inodes.clone(),
            entered: Vec::new(),
            keys: entry.keys.clone(),
            acked: entry.acked.clone(),
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.inodes.is_empty()
            && self.keys.is_empty()
            && self.acked.is_empty()
    }
}

/// The effects of a commit inside the subtree under `root`, as a key
/// rooted at `scope` sees them, or None when it touched nothing there. An
/// effect belongs when its directory was under both before the commit, or
/// is after, so a key sees nothing of what happened to the subtree outside
/// its tree.
pub fn route(entry: &LogEntry, root: Ino, scope: Ino) -> Option<Change> {
    let under = |chain: &[Ino]| chain.contains(&root) && (scope == ROOT || chain.contains(&scope));
    let was = |dir: Ino| {
        entry
            .chains
            .get(&dir)
            .is_some_and(|chains| under(&chains.before))
    };
    let is = |dir: Ino| {
        entry
            .chains
            .get(&dir)
            .is_some_and(|chains| under(&chains.after))
    };
    let entries: Vec<EntryEffect> = entry
        .entries
        .iter()
        .filter(|effect| was(effect.dir) || is(effect.dir))
        .cloned()
        .collect();
    let mut inodes = Vec::new();
    let mut entered = Vec::new();
    for effect in &entry.inodes {
        let kind = effect.after.or(effect.before).map(|inode| inode.kind);
        let inside = match kind {
            Some(Kind::Dir) => {
                // A directory that existed and arrived from outside.
                if effect.before.is_some() && is(effect.ino) && !was(effect.ino) {
                    entered.push(effect.ino);
                }
                was(effect.ino) || is(effect.ino)
            }
            _ => effect.holders.iter().any(|&dir| was(dir) || is(dir)),
        };
        if inside {
            // A subscriber learns of no directory outside its subtree.
            let mut effect = effect.clone();
            effect.holders.retain(|&dir| was(dir) || is(dir));
            inodes.push(effect);
        }
    }
    if entries.is_empty() && inodes.is_empty() {
        return None;
    }
    Some(Change {
        version: entry.version,
        time: entry.time,
        entries,
        inodes,
        entered,
        ..Change::default()
    })
}

/// How many recent changes a subscriber keeps for moving older reads
/// forward.
const RECENT: usize = 4096;

/// A cache of one subtree kept current by its routed changes. Reads fill
/// it: a read at the subscriber's version fills at once, an older one is
/// moved forward through recent changes, and a newer one waits until the
/// feed reaches it. Snapshots read it as of earlier versions
/// ([`crate::snapshot`]).
pub struct Subscriber {
    root: Ino,
    version: Version,
    budget: Budget,
    cache: Cache,
    pending: BTreeMap<Version, Vec<Fact>>,
    /// Recent changes as deltas, oldest first, complete from `recent_from`.
    recent: VecDeque<Delta>,
    recent_from: Version,
    /// The latest change after which the cache forgot something that left
    /// the subtree or entered it. A read older than it may describe
    /// something whose changes outside the subtree the feed never showed.
    /// A feed at the tree's root shows every change to what the root
    /// reaches, and only what it can't reach changes unseen, so it keeps
    /// this at its start and rebases any read the recent changes cover.
    last_forget: Version,
    undo: Undo,
    /// Eviction spares what the cache wrote at or after this mark, which
    /// reads in progress need.
    spare: u64,
}

impl Subscriber {
    /// A subscriber with nothing cached, at `version`.
    pub fn new(root: Ino, version: Version) -> Subscriber {
        Subscriber::with_budget(root, version, Budget::default())
    }

    pub fn with_budget(root: Ino, version: Version, budget: Budget) -> Subscriber {
        Subscriber {
            root,
            version,
            budget,
            cache: Cache::with_budget(budget),
            pending: BTreeMap::new(),
            recent: VecDeque::new(),
            recent_from: version,
            last_forget: version,
            undo: Undo::default(),
            spare: u64::MAX,
        }
    }

    /// Where the cache's writes stand, for `spare`.
    pub fn mark(&self) -> u64 {
        self.cache.mark()
    }

    /// Spares from eviction what the cache writes at or after `mark`, until
    /// spared again: a read's facts stay until its query runs.
    pub fn spare(&mut self, mark: u64) {
        self.spare = mark;
    }

    pub fn root(&self) -> Ino {
        self.root
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Holds `keys` for authorizing, as the whole tree's follower does; its
    /// changes keep them current.
    pub fn load_keys(&mut self, keys: Keys) {
        self.cache.load_keys(keys);
    }

    /// The cache, as of the subscriber's version.
    pub fn into_cache(self) -> Cache {
        self.cache
    }

    /// Starts over at `version` with nothing cached, as after the feed's
    /// position was truncated.
    pub fn reset(&mut self, version: Version) {
        *self = Subscriber::with_budget(self.root, version, self.budget);
    }

    /// Applies the next change. Changes arrive in version order; one at or
    /// below the subscriber's version is already applied.
    pub fn apply(&mut self, change: &Change) {
        if change.version <= self.version {
            return;
        }
        if !change.is_empty() {
            let recording = self.undo.recording();
            if recording {
                for effect in &change.entries {
                    self.undo
                        .entry(effect.dir, &effect.name, change.version, effect.before);
                }
                for effect in &change.inodes {
                    self.undo.inode(effect.ino, change.version, effect.before);
                }
            }
            let mut forgot: Vec<(Ino, Kept)> = Vec::new();
            let delta = delta(change);
            self.cache.apply(&delta);
            // What leaves the subtree stops changing in its feed, so the
            // cache forgets it: a child a name no longer reaches, unless a
            // name in the subtree gains it in the same commit. A directory
            // that leaves alive may come back changed, so the cache then
            // keeps only what the root still reaches, which leaves nothing
            // cached beneath a directory outside. One freed or detached
            // never comes back, but a file beneath it may, through a link
            // elsewhere, so the cache forgets it and everything beneath it,
            // which the undo keeps while a snapshot is open. A directory
            // that enters therefore brings nothing cached with it.
            let mut prune = false;
            let mut gained: BTreeSet<Ino> = change
                .entries
                .iter()
                .filter_map(|effect| effect.after.map(|after| after.child))
                .collect();
            // A commit in which a directory leaves shows no gain to be safe:
            // the directory that gained a child may have gone with it, or
            // beneath it, and uncached, nothing else records that it left.
            // The worst of forgetting one that stayed is a read to fill it
            // again, or a snapshot that ends early.
            let directory_left = change.entries.iter().any(|effect| {
                effect.before.is_some_and(|before| {
                    before.kind == Kind::Dir
                        && effect.after.is_none_or(|after| after.child != before.child)
                        && !gained.contains(&before.child)
                })
            });
            if directory_left {
                gained.clear();
            }
            let removed: BTreeSet<Ino> = change
                .inodes
                .iter()
                .filter(|effect| {
                    effect
                        .after
                        .is_some_and(|inode| !matches!(inode.life, Life::Live))
                })
                .map(|effect| effect.ino)
                .collect();
            for effect in &change.entries {
                if let Some(before) = effect.before
                    && effect.after.is_none_or(|after| after.child != before.child)
                    && !gained.contains(&before.child)
                {
                    if before.kind == Kind::Dir {
                        if !removed.contains(&before.child) {
                            prune = true;
                            forgot.push((before.child, self.cache.forget_entries(before.child)));
                        } else if recording {
                            forgot.extend(self.cache.forget_beneath(before.child));
                        } else {
                            self.cache.forget_beneath(before.child);
                        }
                    }
                    forgot.push((before.child, self.cache.forget_file(before.child)));
                    if self.root != ROOT {
                        self.last_forget = change.version;
                    }
                }
            }
            if prune {
                forgot.extend(self.cache.keep_reachable(self.root));
            }
            if recording {
                for (ino, kept) in forgot {
                    self.undo.leave(ino, change.version, kept);
                }
                self.undo.fit(self.budget.undo_bytes);
            }
            self.recent.push_back(delta);
            if self.recent.len() > RECENT
                && let Some(oldest) = self.recent.pop_front()
            {
                self.recent_from = oldest.version;
            }
        }
        self.version = change.version;
        let ready: Vec<Version> = self
            .pending
            .range(..=self.version)
            .map(|(at, _)| *at)
            .collect();
        for at in ready {
            for fact in self.pending.remove(&at).unwrap_or_default() {
                self.fill(fact, at);
            }
        }
        self.cache.evict(self.spare);
    }

    /// Opens a snapshot at the subscriber's version, which it returns.
    pub fn open_snapshot(&mut self) -> Version {
        self.undo.open(self.version);
        self.version
    }

    pub fn close_snapshot(&mut self, at: Version) {
        self.undo.close(at);
    }

    /// The oldest version a reader of the subscriber uses: its oldest
    /// snapshot's, ended by the budget or not, until closed; or its own.
    pub fn oldest(&self) -> Version {
        self.undo.pinned().unwrap_or(self.version)
    }

    pub fn undo(&self) -> &Undo {
        &self.undo
    }

    /// Answers `query` from `root` as of the snapshot at `at`, with facts
    /// a read fetched over the cache. A fetched fact counts only once the
    /// subscriber has reached the version it reflects.
    pub fn run_at(
        &self,
        at: Version,
        root: Ino,
        query: &Query,
        fetched: &Cache,
    ) -> Result<QueryResult, Miss> {
        if !self.undo.holds(at) {
            return Err(Miss::TooOld);
        }
        let current = Current {
            fetched,
            cache: &self.cache,
        };
        let view = SnapshotView::new(&self.undo, &current, at);
        match query::run(&view, root, query) {
            Ok(result) => Ok(result),
            Err(_) if view.lost() => Err(Miss::TooOld),
            Err(reads) => Err(Miss::Reads(reads)),
        }
    }

    /// Fills the cache with a fact read at version `at`.
    pub fn fill(&mut self, fact: Fact, at: Version) {
        // A value under a version past the read's may yet be written, and a
        // feed carries no values to say so.
        if let Fact::Value(_, version, None) = &fact
            && *version > at
        {
            return;
        }
        if at > self.version {
            self.pending.entry(at).or_default().push(fact);
        } else if at == self.version {
            self.cache.fill(fact);
        } else if at >= self.recent_from && at >= self.last_forget {
            let start = self.recent.partition_point(|delta| delta.version <= at);
            let window: Vec<&Delta> = self.recent.range(start..).collect();
            if let Some(fact) = rebase(fact, at, self.version, &window) {
                self.cache.fill(fact);
            }
        }
    }
}

/// A change's effects as a delta the cache applies. A directory the change
/// created starts complete and empty.
fn delta(change: &Change) -> Delta {
    let mut delta = Delta::new(change.version);
    for effect in &change.entries {
        delta.entries.entry(effect.dir).or_default().insert(
            effect.name.clone(),
            Changed {
                before: effect.before,
                after: effect.after,
            },
        );
    }
    for effect in &change.inodes {
        if effect.before.is_none() && effect.after.is_some_and(|inode| inode.kind == Kind::Dir) {
            delta.created.insert(effect.ino);
        }
        delta.inodes.insert(
            effect.ino,
            Changed {
                before: effect.before,
                after: effect.after,
            },
        );
    }
    for (id, record) in &change.keys {
        delta.keys.insert(
            *id,
            Changed {
                before: None,
                after: *record,
            },
        );
    }
    for freed in &change.acked {
        match *freed {
            Freed::Value(ino, version) => {
                delta.dropped.insert((ino, version));
            }
            // An acknowledged file's current value goes with its inode.
            Freed::Inode(ino) => {
                if let Some(Some(inode)) = delta.inodes.get(&ino).map(|change| change.before)
                    && inode.kind == Kind::File
                {
                    delta.dropped.insert((ino, inode.value));
                }
            }
        }
        delta.acked.push(Acked {
            version: change.version,
            freed: *freed,
            ancestors: Vec::new(),
        });
    }
    delta
}
