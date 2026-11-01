//! Revoking a key with many keys minted from it, and removing their root.
//!
//!     cargo run --release -p inodedb-tests --example revoke
//!
//! Each run mints keys under one admin key, then revokes it, which revokes
//! them all in one commit; then mints them again and removes the directory
//! they are rooted at, which revokes them too.

use inodedb_core::auth::{Caller, Grant, KeyOp};
use inodedb_core::types::ROOT;
use inodedb_core::writer::Answer;
use inodedb_sim::harness::*;
use std::time::Instant;

fn mint_under(h: &mut Harness, count: u64) -> Caller {
    let top = h.mint(Caller::Trusted(ROOT), 1, "t", Grant::Admin, 0);
    for id in 0..count {
        h.mint(top, 1_000_000 + id, "", Grant::Read, 0);
    }
    top
}

fn main() {
    println!("| Keys | Revoke the minter | Remove their root |");
    println!("| ---: | ---: | ---: |");
    for count in [4_000, 16_000, 64_000] {
        let mut h = Harness::new();
        h.ops([mkdir("t")]);
        mint_under(&mut h, count);
        let started = Instant::now();
        assert!(matches!(
            h.key_op(Caller::Trusted(ROOT), KeyOp::Revoke { id: 1 }),
            Answer::Key(Ok(_))
        ));
        let revoked = started.elapsed();
        let mut h = Harness::new();
        h.ops([mkdir("t")]);
        mint_under(&mut h, count);
        let started = Instant::now();
        h.ops([remove_all("t")]);
        let removed = started.elapsed();
        assert!(h.writer.keys().is_empty());
        println!(
            "| {count} | {:.1} ms | {:.1} ms |",
            revoked.as_secs_f64() * 1000.0,
            removed.as_secs_f64() * 1000.0
        );
    }
}
