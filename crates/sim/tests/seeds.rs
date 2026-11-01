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
