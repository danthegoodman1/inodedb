//! InodeDB's gRPC protocol, generated from `proto/inodedb.proto`.
//!
//! The generated code lives in `src/inodedb.v1.rs`, so building this crate
//! needs no `protoc`. `tests/generated.rs` checks that file against the proto
//! file and rewrites it when the two differ.

#![allow(clippy::all, missing_docs)]

pub mod v1 {
    include!("inodedb.v1.rs");
}

pub use v1::*;
