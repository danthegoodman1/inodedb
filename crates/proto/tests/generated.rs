//! The checked-in protocol code matches `proto/inodedb.proto`.

use std::path::Path;

#[test]
fn the_checked_in_code_matches_the_proto_file() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let protos = crate_dir.join("../../proto");
    let out = tempfile::tempdir().unwrap();
    tonic_prost_build::configure()
        .bytes(".")
        .out_dir(out.path())
        .emit_rerun_if_changed(false)
        .compile_protos(&[protos.join("inodedb.proto")], &[protos])
        .expect("protoc compiles proto/inodedb.proto");
    let generated = std::fs::read_to_string(out.path().join("inodedb.v1.rs")).unwrap();
    let checked_in = crate_dir.join("src/inodedb.v1.rs");
    if std::fs::read_to_string(&checked_in).unwrap() != generated {
        std::fs::write(&checked_in, generated).unwrap();
        panic!(
            "src/inodedb.v1.rs differed from proto/inodedb.proto's code and now holds it; commit the change"
        );
    }
}
