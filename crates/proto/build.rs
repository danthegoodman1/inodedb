fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .bytes(".")
        .compile_protos(&["../../proto/inodedb.proto"], &["../../proto"])?;
    println!("cargo:rerun-if-changed=../../proto/inodedb.proto");
    Ok(())
}
