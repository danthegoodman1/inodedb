#![cfg(feature = "fdb")]

use inodedb_storage_conformance::backends::Fdb;
use inodedb_storage_conformance::conformance;

conformance!(Fdb::new());
