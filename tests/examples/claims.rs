//! Freeing many tenants' directories, and claiming one tenant's entries
//! from the queue that leaves.
//!
//!     cargo run --release -p inodedb-tests --example claims
//!
//! Each run makes tenants through the harness's writer over the memory
//! backend. A tenant is a directory `tenants/tN` that holds one part,
//! `store/aa/table/part`, of 20 files, so six directories lie between a
//! file and the tree's root. The run removes every tenant's part, 50 to a
//! commit, and times each commit with the freeing that follows it. It then
//! claims up to 60 entries under one tenant's root and acknowledges them,
//! for 20 tenants spread across the queue, and reports the median claim:
//! the storage reads it took, and how long the claim and its
//! acknowledgement took together.

use inodedb_core::auth::Caller;
use inodedb_core::query::QueryResult;
use inodedb_sim::harness::*;
use std::time::{Duration, Instant};

const FILES: usize = 20;
const REMOVED: usize = 50;
const CLAIMS: usize = 20;

fn median(mut times: Vec<Duration>) -> f64 {
    times.sort();
    times[times.len() / 2].as_secs_f64() * 1000.0
}

fn main() {
    println!(
        "| Tenants | Queue entries | Queue bytes an entry | One commit removing {REMOVED} parts | Reads for a claim | A claim and its acknowledgement |"
    );
    println!("| ---: | ---: | ---: | ---: | ---: | ---: |");
    for tenants in [100, 1_000, 10_000] {
        let mut h = Harness::new();
        let part = |n: usize| format!("tenants/t{n}/store/aa/table/part");
        for chunk in (0..tenants).collect::<Vec<_>>().chunks(200) {
            h.ops(chunk.iter().map(|n| mkdir_p(&part(*n))));
            h.ops(
                chunk
                    .iter()
                    .flat_map(|n| (0..FILES).map(move |file| (*n, file)))
                    .map(|(n, file)| put(&format!("{}/f{file}", part(n)), "blobs")),
            );
        }
        let roots: Vec<Caller> = (0..CLAIMS)
            .map(|pick| {
                let tenant = pick * tenants / CLAIMS;
                match h.stat(&format!("tenants/t{tenant}")) {
                    QueryResult::Stat { ino, .. } => Caller::Trusted(ino),
                    other => panic!("{other:?}"),
                }
            })
            .collect();
        let mut removals = Vec::new();
        for chunk in (0..tenants).collect::<Vec<_>>().chunks(REMOVED) {
            let started = Instant::now();
            h.ops(chunk.iter().map(|n| remove_all(&part(*n))));
            removals.push(started.elapsed());
        }
        assert_eq!(h.writer.detached(), 0);
        // The queue's own entries, and those it keeps under directories.
        let (mut entries, mut bytes) = (0, 0);
        for (key, value) in h.storage.dump() {
            match key.get(1) {
                Some(b'f') => {
                    entries += 1;
                    bytes += key.len() + value.len();
                }
                Some(b'u') => bytes += key.len() + value.len(),
                _ => {}
            }
        }
        // The first claim commits the release horizon; leave it untimed.
        let warm = h.claim(Caller::Trusted(h.root), 1);
        assert_eq!(warm.1.len(), 1);
        let (mut reads, mut times) = (Vec::new(), Vec::new());
        for root in roots {
            let (before, started) = (h.reads, Instant::now());
            let (claim, claimed) = h.claim(root, 60);
            reads.push(h.reads - before);
            h.ack(root, claim, &claimed).expect("acknowledges");
            times.push(started.elapsed());
            assert!(claimed.len() >= FILES, "{} claimed", claimed.len());
        }
        reads.sort();
        println!(
            "| {tenants} | {entries} | {} | {:.2} ms | {} | {:.3} ms |",
            bytes / entries,
            median(removals),
            reads[CLAIMS / 2],
            median(times)
        );
    }
}
