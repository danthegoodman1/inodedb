use inodedb_storage_conformance::backends::Lite;
use inodedb_storage_conformance::conformance;
use inodedb_storage_sqlite::Sqlite;

conformance!(Lite::new());

#[test]
fn a_second_process_is_refused() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let path = dir.path().join("db.sqlite");
    let first = Sqlite::open(&path).expect("opens");
    assert!(
        Sqlite::open(&path).is_err(),
        "the lock file admits one opener"
    );
    drop(first);
    Sqlite::open(&path).expect("reopens once released");
}
