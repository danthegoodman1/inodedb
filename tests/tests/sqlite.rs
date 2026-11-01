use inodedb_tests::api_conformance;
use inodedb_tests::backends::Lite;

api_conformance!(Lite::new());
