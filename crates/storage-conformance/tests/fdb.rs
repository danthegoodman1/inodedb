#![cfg(feature = "fdb")]

use inodedb_storage_conformance::backends::Fdb;
use inodedb_storage_conformance::conformance;

conformance!(Fdb::new());
conformance!(@tests Fdb::new(); a_writers_handle_reads_from_its_latest_commit);
