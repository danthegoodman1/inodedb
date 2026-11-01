//! Leases and claims: how long freed records stay, and who deletes them.
//!
//! A reader's lease holds the oldest version it still uses. A free-queue
//! entry freed at version F is released once every live lease's oldest
//! version has reached F; the release horizon is the highest version
//! released so far, and only rises. A consumer claims released entries
//! under its root, deletes what they reference, and acknowledges them,
//! which deletes each entry with the records it kept. A claim that runs out
//! returns its entries to the queue.
//!
//! The writer keeps leases in memory. A lease's opening commits before the
//! writer answers it, so no writer releases what an answered lease holds;
//! heartbeats reach storage with the next commit, and a lease a starting
//! writer loads gets a fresh time to live. Claims use only a horizon
//! storage holds, so a starting writer, which begins from that horizon,
//! never releases less than a claim was given. Claims live in memory
//! alone: a new writer has none, and consumers delete idempotently.

use crate::auth::KeyId;
use crate::codec::{Dec, DecodeError, Enc};
use crate::delta::Freed;
use crate::types::{Ino, Millis, ROOT, Version};
use std::collections::{BTreeMap, BTreeSet};

pub type LeaseId = u64;
pub type ClaimId = u64;

/// What a lease holds, as heartbeats report it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    /// The oldest version the reader still uses.
    pub oldest: Version,
    /// The reader's changefeed position.
    pub position: Version,
}

impl LeaseRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(20);
        enc.varint(self.oldest).varint(self.position);
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<LeaseRecord, DecodeError> {
        let mut dec = Dec::new(bytes);
        let record = LeaseRecord {
            oldest: dec.varint()?,
            position: dec.varint()?,
        };
        dec.end()?;
        Ok(record)
    }
}

/// A lease as storage keeps it: what it holds, and the key that opened it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredLease {
    pub record: LeaseRecord,
    /// None for a trusted caller.
    pub owner: Option<KeyId>,
}

impl StoredLease {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(30);
        enc.varint(self.record.oldest).varint(self.record.position);
        match self.owner {
            Some(owner) => enc.u8(1).be64(owner),
            None => enc.u8(0),
        };
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<StoredLease, DecodeError> {
        let mut dec = Dec::new(bytes);
        let record = LeaseRecord {
            oldest: dec.varint()?,
            position: dec.varint()?,
        };
        let owner = match dec.u8()? {
            0 => None,
            1 => Some(dec.be64()?),
            _ => return Err(DecodeError),
        };
        dec.end()?;
        Ok(StoredLease { record, owner })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseError {
    /// The lease ran out or was released, or another key opened it; the
    /// reader drops its handles and reloads.
    Expired,
    /// A new lease must start at or after the release horizon.
    TooOld { horizon: Version },
    /// A heartbeat may not lower the oldest version in use.
    Lowered { oldest: Version },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Granted {
    pub lease: LeaseId,
    pub expires_in: Millis,
    /// The oldest version the lease holds.
    pub oldest: Version,
}

/// A free-queue entry: what it frees, under the version that freed it, and
/// the directories it recorded above.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreeEntry {
    pub version: Version,
    pub freed: Freed,
    pub ancestors: Vec<Ino>,
}

impl FreeEntry {
    pub fn key(&self) -> (Version, Freed) {
        (self.version, self.freed)
    }

    /// Whether the entry lies under a consumer's root.
    pub fn within(&self, root: Ino) -> bool {
        root == ROOT || self.ancestors.contains(&root)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimError {
    /// The claim ran out, or never was.
    Unknown,
}

/// What storage keeps of release: the horizon claims may use, and the next
/// lease id. A starting writer begins from both, so it never releases less
/// than a claim was given, nor opens a lease under an id handed out before.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Release {
    pub horizon: Version,
    pub next_lease: LeaseId,
}

impl Release {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(20);
        enc.varint(self.horizon).varint(self.next_lease);
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Release, DecodeError> {
        let mut dec = Dec::new(bytes);
        let release = Release {
            horizon: dec.varint()?,
            next_lease: dec.varint()?,
        };
        dec.end()?;
        Ok(release)
    }
}

#[derive(Debug)]
struct Held {
    record: LeaseRecord,
    owner: Option<KeyId>,
    expires: Millis,
}

#[derive(Debug)]
struct Claim {
    root: Ino,
    entries: BTreeMap<(Version, Freed), FreeEntry>,
    expires: Millis,
}

/// Counts of equal values, for the lowest one at once.
#[derive(Debug, Default)]
struct Multiset(BTreeMap<Version, usize>);

impl Multiset {
    fn insert(&mut self, value: Version) {
        *self.0.entry(value).or_default() += 1;
    }

    fn remove(&mut self, value: Version) {
        if let std::collections::btree_map::Entry::Occupied(mut count) = self.0.entry(value) {
            *count.get_mut() -= 1;
            if *count.get() == 0 {
                count.remove();
            }
        }
    }

    fn lowest(&self) -> Option<Version> {
        self.0.keys().next().copied()
    }
}

#[derive(Debug)]
pub struct Leases {
    ttl: Millis,
    claim_ttl: Millis,
    held: BTreeMap<LeaseId, Held>,
    /// The held leases by expiry, oldest version and position, so expiry
    /// and the lowest of each take no pass over every lease.
    expiries: BTreeSet<(Millis, LeaseId)>,
    oldest: Multiset,
    positions: Multiset,
    /// Lease records for the next group to write, or delete (None).
    writes: BTreeMap<LeaseId, Option<StoredLease>>,
    next: LeaseId,
    horizon: Version,
    /// What storage holds of release. Claims use its horizon alone.
    stored: Release,
    claims: BTreeMap<ClaimId, Claim>,
    claim_expiries: BTreeSet<(Millis, ClaimId)>,
    claimed: BTreeMap<(Version, Freed), ClaimId>,
    next_claim: ClaimId,
}

impl Leases {
    /// Leases whose claims take ids from `claim_base` on, which a writer
    /// draws from its own id so no two writers hand out the same claim.
    pub fn new(ttl: Millis, claim_ttl: Millis, claim_base: ClaimId) -> Leases {
        Leases {
            ttl,
            claim_ttl,
            held: BTreeMap::new(),
            expiries: BTreeSet::new(),
            oldest: Multiset::default(),
            positions: Multiset::default(),
            writes: BTreeMap::new(),
            next: 1,
            horizon: 0,
            stored: Release::default(),
            claims: BTreeMap::new(),
            claim_expiries: BTreeSet::new(),
            claimed: BTreeMap::new(),
            next_claim: claim_base.max(1),
        }
    }

    /// Takes the leases and release record storage holds, each lease with
    /// a fresh time to live, at a store's committed version.
    pub fn load(
        &mut self,
        stored: Vec<(LeaseId, StoredLease)>,
        release: Option<Release>,
        now: Millis,
        committed: Version,
    ) {
        let release = release.unwrap_or_default();
        self.next = self.next.max(release.next_lease);
        for (id, lease) in stored {
            self.next = self.next.max(id + 1);
            self.insert(
                id,
                Held {
                    record: lease.record,
                    owner: lease.owner,
                    expires: now + self.ttl,
                },
            );
        }
        self.stored = release;
        self.horizon = release.horizon.max(self.floor(committed));
    }

    /// Holds a lease, in place of any earlier holding under its id.
    fn insert(&mut self, id: LeaseId, held: Held) {
        self.remove(id);
        self.expiries.insert((held.expires, id));
        self.oldest.insert(held.record.oldest);
        self.positions.insert(held.record.position);
        self.held.insert(id, held);
    }

    fn remove(&mut self, id: LeaseId) -> Option<Held> {
        let held = self.held.remove(&id)?;
        self.expiries.remove(&(held.expires, id));
        self.oldest.remove(held.record.oldest);
        self.positions.remove(held.record.position);
        Some(held)
    }

    /// The lowest version any lease holds, or `committed` with none.
    fn floor(&self, committed: Version) -> Version {
        self.oldest.lowest().unwrap_or(committed).min(committed)
    }

    pub fn horizon(&self) -> Version {
        self.horizon
    }

    /// Raises the horizon to what the leases allow at `committed`.
    pub fn advance(&mut self, committed: Version) {
        self.horizon = self.horizon.max(self.floor(committed));
    }

    /// The release record storage should hold now.
    pub fn release(&self) -> Release {
        Release {
            horizon: self.horizon,
            next_lease: self.next,
        }
    }

    /// Whether storage lags what release has reached.
    pub fn release_due(&self) -> bool {
        self.release() != self.stored
    }

    /// Notes a release record storage now holds.
    pub fn stored(&mut self, release: Release) {
        self.stored = Release {
            horizon: self.stored.horizon.max(release.horizon),
            next_lease: self.stored.next_lease.max(release.next_lease),
        };
    }

    /// An id for a lease about to open. Its opening commits a release
    /// record past it.
    pub fn reserve(&mut self) -> LeaseId {
        self.next += 1;
        self.next - 1
    }

    /// Opens a lease under a reserved id; opening it again is a no-op.
    /// Returns what storage keeps of it, which commits before the opening
    /// is answered.
    pub fn open(
        &mut self,
        id: LeaseId,
        record: LeaseRecord,
        owner: Option<KeyId>,
        now: Millis,
    ) -> Result<StoredLease, LeaseError> {
        if !self.held.contains_key(&id) {
            if record.oldest < self.horizon {
                return Err(LeaseError::TooOld {
                    horizon: self.horizon,
                });
            }
            // The opening writes the record; a deletion still pending from
            // an earlier run of it, which ran out, would undo that.
            self.writes.remove(&id);
            self.insert(
                id,
                Held {
                    record,
                    owner,
                    expires: now + self.ttl,
                },
            );
        }
        let held = &self.held[&id];
        Ok(StoredLease {
            record: held.record,
            owner: held.owner,
        })
    }

    pub fn granted(&self, id: LeaseId) -> Granted {
        Granted {
            lease: id,
            expires_in: self.ttl,
            oldest: self.held[&id].record.oldest,
        }
    }

    pub fn beat(
        &mut self,
        id: LeaseId,
        record: LeaseRecord,
        owner: Option<KeyId>,
        now: Millis,
    ) -> Result<Granted, LeaseError> {
        let Some(held) = self
            .held
            .get(&id)
            .filter(|held| held.expires > now && held.owner == owner)
        else {
            return Err(LeaseError::Expired);
        };
        if record.oldest < held.record.oldest {
            return Err(LeaseError::Lowered {
                oldest: held.record.oldest,
            });
        }
        self.remove(id);
        self.insert(
            id,
            Held {
                record,
                owner,
                expires: now + self.ttl,
            },
        );
        let stored = StoredLease { record, owner };
        self.writes.insert(id, Some(stored));
        Ok(Granted {
            lease: id,
            expires_in: self.ttl,
            oldest: record.oldest,
        })
    }

    /// Ends a lease its owner holds.
    pub fn release_lease(&mut self, id: LeaseId, owner: Option<KeyId>) {
        if self.held.get(&id).is_some_and(|held| held.owner == owner) {
            self.drop_lease(id);
        }
    }

    fn drop_lease(&mut self, id: LeaseId) {
        if self.remove(id).is_some() {
            self.writes.insert(id, None);
        }
    }

    /// Drops the leases and claims that ran out by `now`.
    pub fn expire(&mut self, now: Millis) {
        while let Some(&(expires, id)) = self.expiries.first()
            && expires <= now
        {
            self.drop_lease(id);
        }
        while let Some(&(expires, id)) = self.claim_expiries.first()
            && expires <= now
        {
            self.claim_expiries.remove(&(expires, id));
            if let Some(claim) = self.claims.remove(&id) {
                for key in claim.entries.keys() {
                    self.claimed.remove(key);
                }
            }
        }
    }

    /// The lease records a commit writes, taken from the pending ones
    /// until they pass `budget` bytes; the rest wait for a later commit.
    pub fn take_writes(&mut self, budget: usize) -> BTreeMap<LeaseId, Option<StoredLease>> {
        let mut taken = BTreeMap::new();
        let mut bytes = 0;
        while bytes < budget
            && let Some((id, record)) = self.writes.pop_first()
        {
            bytes += crate::keys::lease(id).len()
                + record.as_ref().map_or(0, |stored| stored.encode().len());
            taken.insert(id, record);
        }
        taken
    }

    /// Whether heartbeats or endings wait to reach storage.
    pub fn writes_due(&self) -> bool {
        !self.writes.is_empty()
    }

    /// Puts back the writes of a group that failed, under any newer ones.
    pub fn restore_writes(&mut self, writes: BTreeMap<LeaseId, Option<StoredLease>>) {
        for (id, record) in writes {
            self.writes.entry(id).or_insert(record);
        }
    }

    /// The lowest changefeed position a live lease reports.
    pub fn min_position(&self) -> Option<Version> {
        self.positions.lowest()
    }

    pub fn len(&self) -> usize {
        self.held.len()
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// Whether a consumer at `root` may claim an entry now: storage holds a
    /// horizon at or past it, and no claim holds it.
    pub fn claimable(&self, root: Ino, entry: &FreeEntry) -> bool {
        entry.version <= self.stored.horizon
            && !self.claimed.contains_key(&entry.key())
            && entry.within(root)
    }

    /// The horizon claims may use.
    pub fn claim_horizon(&self) -> Version {
        self.stored.horizon
    }

    /// Claims entries for a consumer at `root`.
    pub fn claim(&mut self, root: Ino, entries: Vec<FreeEntry>, now: Millis) -> ClaimId {
        let id = self.next_claim;
        self.next_claim = self.next_claim.wrapping_add(1).max(1);
        for entry in &entries {
            self.claimed.insert(entry.key(), id);
        }
        let expires = now + self.claim_ttl;
        self.claim_expiries.insert((expires, id));
        self.claims.insert(
            id,
            Claim {
                root,
                entries: entries
                    .into_iter()
                    .map(|entry| (entry.key(), entry))
                    .collect(),
                expires,
            },
        );
        id
    }

    /// The claimed entries a consumer at `root` acknowledges, which renews
    /// the claim; acknowledging none only renews it. Entries the claim no
    /// longer holds, as when a retry follows an acknowledgement that
    /// committed, are skipped: a claim lasts its time even once empty.
    pub fn acknowledge(
        &mut self,
        claim: ClaimId,
        root: Ino,
        keys: &[(Version, Freed)],
        now: Millis,
    ) -> Result<Vec<FreeEntry>, ClaimError> {
        let Some(held) = self
            .claims
            .get_mut(&claim)
            .filter(|held| held.expires > now && held.root == root)
        else {
            return Err(ClaimError::Unknown);
        };
        let entries = keys
            .iter()
            .filter_map(|key| held.entries.get(key).cloned())
            .collect();
        self.claim_expiries.remove(&(held.expires, claim));
        held.expires = now + self.claim_ttl;
        self.claim_expiries.insert((held.expires, claim));
        Ok(entries)
    }

    /// Forgets entries whose acknowledgement committed.
    pub fn acknowledged(&mut self, entries: &[FreeEntry]) {
        for entry in entries {
            if let Some(id) = self.claimed.remove(&entry.key())
                && let Some(claim) = self.claims.get_mut(&id)
            {
                claim.entries.remove(&entry.key());
            }
        }
    }

    /// Entries claimed now, for tests and metrics.
    pub fn claimed(&self) -> BTreeSet<(Version, Freed)> {
        self.claimed.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(oldest: Version) -> LeaseRecord {
        LeaseRecord {
            oldest,
            position: oldest,
        }
    }

    fn entry(version: Version, ino: Ino, ancestors: &[Ino]) -> FreeEntry {
        FreeEntry {
            version,
            freed: Freed::Inode(ino),
            ancestors: ancestors.to_vec(),
        }
    }

    #[test]
    fn heartbeats_reach_storage_a_budget_at_a_time() {
        let mut leases = Leases::new(100, 100, 1);
        leases.load(Vec::new(), None, 0, 10);
        for _ in 0..1_000 {
            let id = leases.reserve();
            leases.open(id, record(10), Some(9), 0).expect("opens");
            leases.beat(id, record(10), Some(9), 1).expect("beats");
        }
        let mut taken = 0;
        let mut commits = 0;
        while leases.writes_due() {
            let writes = leases.take_writes(4_096);
            let bytes: usize = writes
                .iter()
                .map(|(id, record)| {
                    crate::keys::lease(*id).len()
                        + record.as_ref().map_or(0, |stored| stored.encode().len())
                })
                .sum();
            assert!(bytes < 4_096 + 100, "{bytes} bytes in one commit");
            taken += writes.len();
            commits += 1;
        }
        assert_eq!(taken, 1_000);
        assert!(commits > 1);
    }

    #[test]
    fn the_horizon_waits_for_the_oldest_lease_and_only_rises() {
        let mut leases = Leases::new(100, 100, 1);
        leases.load(Vec::new(), None, 0, 10);
        assert_eq!(leases.horizon(), 10);
        let a = leases.reserve();
        leases.open(a, record(10), Some(9), 0).expect("opens");
        leases.advance(20);
        assert_eq!(leases.horizon(), 10, "held at the lease's oldest");
        assert_eq!(
            leases.open(leases.next, record(9), Some(9), 0),
            Err(LeaseError::TooOld { horizon: 10 })
        );
        assert_eq!(
            leases.beat(a, record(15), Some(8), 50),
            Err(LeaseError::Expired),
            "another key's"
        );
        leases.beat(a, record(15), Some(9), 50).expect("beats");
        leases.advance(20);
        assert_eq!(leases.horizon(), 15);
        assert_eq!(
            leases.beat(a, record(12), Some(9), 60),
            Err(LeaseError::Lowered { oldest: 15 })
        );
        leases.expire(200);
        assert_eq!(
            leases.beat(a, record(16), Some(9), 200),
            Err(LeaseError::Expired)
        );
        leases.advance(20);
        assert_eq!(leases.horizon(), 20, "free once the lease runs out");
        // A loaded lease starts its time to live afresh and holds the
        // horizon at once.
        let mut restarted = Leases::new(100, 100, 1);
        let stored = StoredLease {
            record: record(12),
            owner: None,
        };
        assert_eq!(StoredLease::decode(&stored.encode()), Ok(stored));
        restarted.load(vec![(7, stored)], None, 1_000, 30);
        assert_eq!(restarted.horizon(), 12);
        assert_eq!(restarted.reserve(), 8);
        assert!(restarted.beat(7, record(12), None, 1_050).is_ok());
        // The stored release record outweighs leases that lag it, and
        // ids handed out before stay used.
        let release = Release {
            horizon: 25,
            next_lease: 40,
        };
        assert_eq!(Release::decode(&release.encode()), Ok(release));
        let mut restarted = Leases::new(100, 100, 1);
        restarted.load(vec![(7, stored)], Some(release), 1_000, 30);
        assert_eq!(restarted.horizon(), 25);
        assert_eq!(restarted.reserve(), 40);
        // Loading again, as a writer that starts over does, replaces what
        // the first load held.
        restarted.load(vec![(7, stored)], Some(release), 1_050, 30);
        restarted.expire(1_120);
        assert!(restarted.beat(7, record(12), None, 1_120).is_ok());
        restarted.expire(1_300);
        assert_eq!(
            restarted.beat(7, record(12), None, 1_300),
            Err(LeaseError::Expired)
        );
    }

    #[test]
    fn claims_stay_within_their_root_and_run_out() {
        let mut leases = Leases::new(100, 100, 1);
        leases.load(Vec::new(), None, 0, 10);
        let inside = entry(5, 40, &[1, 3]);
        let outside = entry(6, 41, &[1, 4]);
        let unreleased = entry(11, 42, &[1, 3]);
        assert!(
            !leases.claimable(3, &inside),
            "until storage holds the horizon"
        );
        assert!(leases.release_due());
        leases.stored(leases.release());
        assert!(leases.claimable(3, &inside));
        assert!(!leases.claimable(3, &outside));
        assert!(!leases.claimable(3, &unreleased));
        assert!(leases.claimable(ROOT, &outside));
        let claim = leases.claim(3, vec![inside.clone()], 0);
        assert!(!leases.claimable(3, &inside), "claimed once");
        assert_eq!(
            leases.acknowledge(claim, 3, &[outside.key()], 10),
            Ok(Vec::new())
        );
        assert_eq!(
            leases.acknowledge(claim, 4, &[inside.key()], 10),
            Err(ClaimError::Unknown),
            "another root's claim"
        );
        assert!(leases.acknowledge(claim, 3, &[], 90).is_ok(), "renews");
        // A retry of an acknowledgement that committed skips what is gone.
        leases.acknowledged(std::slice::from_ref(&inside));
        assert_eq!(
            leases.acknowledge(claim, 3, &[inside.key()], 95),
            Ok(Vec::new())
        );
        let returned = leases.claim(3, vec![inside.clone()], 95);
        leases.expire(150);
        assert!(!leases.claimable(3, &inside), "renewed past 150");
        leases.expire(200);
        assert!(leases.claimable(3, &inside), "back in the queue");
        assert_eq!(
            leases.acknowledge(returned, 3, &[inside.key()], 200),
            Err(ClaimError::Unknown)
        );
    }
}
