//! Refuses to build an SQLite that would sync with `fsync`.
//!
//! libsqlite3-sys compiles SQLite with the flags in `LIBSQLITE3_FLAGS`, and
//! the repository's `.cargo/config.toml` sets the one that selects
//! `fdatasync`. Cargo reads that file only when it runs inside the
//! repository: a build started elsewhere, with `--manifest-path` or as
//! another workspace's dependency, drops the flag without a word, and its
//! commits pay for `fsync`. This script sees the same variable, so it stops
//! such a build and says how to carry the flag.

#[path = "src/sync.rs"]
mod sync;

fn main() {
    println!("cargo:rerun-if-env-changed=LIBSQLITE3_FLAGS");
    let flags = std::env::var("LIBSQLITE3_FLAGS").unwrap_or_default();
    if !sync::selects_fdatasync(&flags) {
        eprintln!(
            "error: this build would compile SQLite to sync with fsync, which costs InodeDB's \
             commits more than fdatasync.\n\
             LIBSQLITE3_FLAGS is {flags:?} and must hold -DHAVE_FDATASYNC=1. Cargo sets it from \
             InodeDB's .cargo/config.toml only when it runs inside InodeDB's repository.\n\
             Run cargo there, pass it `--config <inodedb>/.cargo/config.toml`, or set \
             LIBSQLITE3_FLAGS=-DHAVE_FDATASYNC=1 in the environment."
        );
        std::process::exit(1);
    }
    println!("cargo:rustc-env=INODEDB_SQLITE_FLAGS={flags}");
}
