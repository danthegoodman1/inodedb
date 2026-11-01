use inodedb_tests::backends::Mem;
use inodedb_tests::{api_conformance, standby_conformance};

api_conformance!(Mem);

mod standby {
    use super::*;
    standby_conformance!(Mem);
}
