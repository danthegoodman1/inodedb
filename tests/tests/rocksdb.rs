#![cfg(feature = "rocksdb")]

use inodedb_tests::api_conformance;
use inodedb_tests::backends::Rock;

api_conformance!(Rock::new());
