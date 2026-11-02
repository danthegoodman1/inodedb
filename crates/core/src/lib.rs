//! InodeDB's semantics as deterministic state machines.
//!
//! The core does no I/O, reads no clocks and starts no threads. Its owner,
//! the server or the simulator, hands it requests, storage results and the
//! time, and carries out the actions it returns.

#![forbid(unsafe_code)]

pub mod auth;
pub mod cache;
pub mod codec;
pub mod delta;
pub mod exec;
pub mod feed;
pub mod fsck;
pub mod keys;
pub mod lease;
pub mod log;
pub mod ops;
pub mod overlay;
pub mod preload;
pub mod query;
pub mod snapshot;
pub mod standby;
pub mod types;
pub mod view;
pub mod writer;

pub use bytes::Bytes;
