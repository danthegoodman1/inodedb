#![cfg(feature = "fdb")]

use inodedb_tests::backends::Fdb;
use inodedb_tests::{api_conformance, standby_conformance};

api_conformance!(Fdb::new());

mod standby {
    use super::*;
    standby_conformance!(Fdb::new());
}
