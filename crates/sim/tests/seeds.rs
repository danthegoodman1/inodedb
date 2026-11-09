//! Seeds the simulator failed on, each run whole as a regression.

use inodedb_sim::Simulator;

fn passes(seed: u64) {
    if let Err(failure) = Simulator::from_seed(seed).run() {
        panic!("{failure}");
    }
}

/// Forgetting the writer's whole cache on many events dropped the reads
/// that freeing a detached subtree was gathering, so it never finished.
#[test]
fn seed_130() {
    passes(130);
}

/// A consumer acknowledged a detached subtree's former parent before the
/// subtree was freed, so the freed records named no directories above
/// them.
#[test]
fn seed_403() {
    passes(403);
}

/// A file created inside a subtree its own batch detached never had its
/// inode seen, so the model kept counting the link freeing removed, and
/// a listing's `freed_through` past the detach looked too high.
#[test]
fn seed_2051() {
    passes(2051);
}

/// A failed group's run again reopened leases that had run out meanwhile,
/// and their pending deletions, written after the openings, removed them
/// from storage; a restarted writer lost them.
#[test]
fn seed_5646() {
    passes(5646);
}

/// Freeing a detached subtree routed to no subscriber, so a snapshot
/// older than the detach lacked undo records for a directory that had
/// moved into the subtree, and read it emptied.
#[test]
fn seed_6384() {
    passes(6384);
}

/// A read by inode inside a detached subtree walked up through the top's
/// old parent, which had since moved under another key's root, so that key
/// reached what the subtree held.
#[test]
fn seed_541() {
    passes(541);
}

/// A subscriber forgot only a removed directory's own entries, so a file
/// beneath it with a link elsewhere stayed cached, missed a change made
/// while it lay outside the subtree, and answered stale when renamed back
/// in.
#[test]
fn seed_75494() {
    passes(75494);
}

/// A successor writer expired request records by the time other writers
/// stamped them, on clocks that had drifted apart past the records'
/// lifetime, so a retry ran again.
#[test]
fn seed_97619() {
    passes(97619);
}

/// The model forgot that a file unlinked outside a detached subtree was
/// still held by a link inside it, so it refused a key above that link the
/// stat by inode the core rightly answered once freeing the subtree freed
/// the file.
#[test]
fn seed_7638357803986486160() {
    passes(7638357803986486160);
}

/// The model took a file's name outside a key's root to rule the file out of
/// it, so it refused the stat by inode the core rightly answered through a
/// link that a removed subtree under the key's root still held.
#[test]
fn seed_927052834940686739() {
    passes(927052834940686739);
}

/// The same, on another run.
#[test]
fn seed_927052834940736596() {
    passes(927052834940736596);
}

/// A file moved into a directory that left the subtree in the same commit
/// counted as gained, so the subscriber kept no record of its leaving. A
/// snapshot from before read its later state from storage.
#[test]
fn seed_15179128079008634066() {
    passes(15179128079008634066);
}

/// A starting writer's first commit failed after a lease ran out. It loaded
/// the lease again with a fresh time to live, kept the deletion it had
/// queued, and its next commit removed the lease from storage, so the writer
/// after it lost a lease it had just renewed.
#[test]
fn seed_16586037544200087192() {
    passes(16586037544200087192);
}
