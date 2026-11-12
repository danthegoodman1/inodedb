//! Removing directories from a store that holds many keys.
//!
//!     cargo run --release -p inodedb-tests --example detach
//!
//! Each run makes tenants, each a directory with two keys rooted at it,
//! through the harness's writer over the memory backend. It then times a
//! commit that removes 50 empty directories under one tenant, none of them
//! a key's root, with the freeing that follows it: the median of 20 such
//! commits. The first of them searches for keys beneath what it removes,
//! and leaves the writer the directories at and above the key roots; the
//! others look their directories up among those. Last it times a commit
//! that removes one tenant, a key's root, which searches every time: the
//! median of 20 again. The first row holds no keys.

use inodedb_core::auth::{Caller, Grant};
use inodedb_core::types::ROOT;
use inodedb_sim::harness::*;
use std::time::{Duration, Instant};

const PARTS: usize = 50;
const ROUNDS: usize = 20;

fn main() {
    println!(
        "| Tenants | Keys | One commit removing {PARTS} directories | One commit removing a tenant |"
    );
    println!("| ---: | ---: | ---: | ---: |");
    for (tenants, keyed) in [
        (10_000, false),
        (1_000, true),
        (10_000, true),
        (50_000, true),
    ] {
        let mut h = Harness::new();
        let tenant = |n: usize| format!("tenants/t{n}");
        h.ops([mkdir("tenants")]);
        let names: Vec<String> = (0..tenants).map(tenant).collect();
        for chunk in names.chunks(5_000) {
            h.ops(chunk.iter().map(|name| mkdir(name)));
        }
        if keyed {
            for name in &names {
                h.mint(Caller::Trusted(ROOT), name, Grant::Write, 0);
                h.mint(Caller::Trusted(ROOT), name, Grant::Read, 0);
            }
        }
        let part = |n: usize| format!("{}/p{n}", tenant(0));
        let mut times: Vec<Duration> = (0..ROUNDS)
            .map(|_| {
                h.ops((0..PARTS).map(|n| mkdir(&part(n))));
                let started = Instant::now();
                h.ops((0..PARTS).map(|n| remove_all(&part(n))));
                started.elapsed()
            })
            .collect();
        times.sort();
        assert_eq!(h.writer.detached(), 0);
        let keys = h.writer.keys().len();
        let mut tenants_gone: Vec<Duration> = (1..=ROUNDS)
            .map(|n| {
                let started = Instant::now();
                h.ops([remove_all(&tenant(n))]);
                started.elapsed()
            })
            .collect();
        tenants_gone.sort();
        assert_eq!(h.writer.detached(), 0);
        println!(
            "| {tenants} | {keys} | {:.2} ms | {:.2} ms |",
            times[ROUNDS / 2].as_secs_f64() * 1000.0,
            tenants_gone[ROUNDS / 2].as_secs_f64() * 1000.0
        );
    }
}
