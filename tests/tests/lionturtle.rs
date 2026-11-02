use inodedb_tests::api_conformance;
use inodedb_tests::backends::Lion;

api_conformance!(Lion::new());
