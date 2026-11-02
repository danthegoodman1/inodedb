use inodedb_storage_conformance::backends::Lion;
use inodedb_storage_conformance::conformance;
use inodedb_storage_lionturtle::Lionturtle;

conformance!(Lion::new());

#[test]
fn a_second_opener_is_refused() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let first = Lionturtle::open(dir.path()).expect("opens");
    assert!(
        Lionturtle::open(dir.path()).is_err(),
        "the lock file admits one opener"
    );
    drop(first);
    Lionturtle::open(dir.path()).expect("reopens once released");
}
