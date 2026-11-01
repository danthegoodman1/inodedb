//! InodeDB's gRPC protocol, generated from `proto/inodedb.proto`.

#![allow(clippy::all, missing_docs)]

pub mod v1 {
    tonic::include_proto!("inodedb.v1");
}

pub use v1::*;
