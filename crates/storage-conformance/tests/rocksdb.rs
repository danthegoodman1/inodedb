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
