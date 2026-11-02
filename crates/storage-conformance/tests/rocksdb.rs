#![cfg(feature = "rocksdb")]

use inodedb_storage_conformance::backends::Rock;
use inodedb_storage_conformance::conformance;
use inodedb_storage_rocksdb::Rocks;

conformance!(Rock::new());

#[test]
fn a_second_opener_is_refused() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let first = Rocks::open(dir.path()).expect("opens");
    assert!(
        Rocks::open(dir.path()).is_err(),
        "RocksDB's lock file admits one opener"
    );
    drop(first);
}

/// The WAL's files are written when the store opens, so commits overwrite
/// written blocks from the first on: four to come, and the one in use.
#[test]
fn the_wal_starts_written() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let store = Rocks::open(dir.path()).expect("opens");
    let sizes: Vec<u64> = std::fs::read_dir(dir.path())
        .expect("lists the store")
        .map(|file| file.expect("an entry"))
        .filter(|file| file.path().extension().is_some_and(|ext| ext == "log"))
        .map(|file| file.metadata().expect("its size").len())
        .collect();
    assert_eq!(sizes.len(), 5, "the WAL's files: {sizes:?}");
    assert!(
        sizes.iter().all(|size| *size >= 40 << 20),
        "the WAL's files: {sizes:?}"
    );
    drop(store);
}
