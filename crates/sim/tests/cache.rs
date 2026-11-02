//! Scripted scenarios for what a writer's cache keeps within its budget.

use inodedb_core::cache::{Budget, INODE_BYTES};
use inodedb_core::query::QueryResult;
use inodedb_core::writer::Config;
use inodedb_sim::harness::*;
use inodedb_storage::Memory;

#[test]
fn records_reads_use_stay_cached_as_new_files_pass_through() {
    let config = Config {
        cache: Budget {
            tree_bytes: 30 * INODE_BYTES,
            ..Budget::default()
        },
        ..Config::default()
    };
    let mut h = Harness::with(Memory::new(), config);
    h.ops([mkdir("hot"), put("hot/f", "1"), mkdir("cold")]);
    let mut reads = 0;
    for round in 0..300 {
        let before = h.reads;
        assert!(matches!(h.stat("hot/f"), QueryResult::Stat { .. }));
        if round >= 20 {
            reads += h.reads - before;
        }
        h.ops([put(&format!("cold/f{round}"), "")]);
    }
    assert_eq!(reads, 0, "each stat passes its records over for eviction");
    assert!(h.writer.cache().tree_bytes() <= 30 * INODE_BYTES);
}
