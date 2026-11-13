//! Which sync a set of SQLite build flags selects. The build script refuses
//! flags that leave `fsync`, and the tests hold it to that.

/// Whether SQLite built with `flags`, as `LIBSQLITE3_FLAGS` gives them, syncs
/// with `fdatasync`. Its Unix layer calls `fsync` unless `HAVE_FDATASYNC` is
/// defined and nonzero, and a later flag overrides an earlier one.
pub fn selects_fdatasync(flags: &str) -> bool {
    flags
        .split_whitespace()
        .rev()
        .find_map(|flag| match flag.strip_prefix("-DHAVE_FDATASYNC") {
            Some("") => Some(true),
            Some(value) if value.starts_with('=') => Some(value != "=0"),
            _ => (flag == "-UHAVE_FDATASYNC").then_some(false),
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::selects_fdatasync;

    /// Only the flag, defined and nonzero, selects `fdatasync`.
    #[test]
    fn only_the_defined_flag_selects_fdatasync() {
        assert!(selects_fdatasync("-DHAVE_FDATASYNC=1"));
        assert!(selects_fdatasync("-DSQLITE_MAX_COLUMN=64 -DHAVE_FDATASYNC"));
        assert!(!selects_fdatasync(""));
        assert!(!selects_fdatasync("-DSQLITE_MAX_COLUMN=64"));
        assert!(!selects_fdatasync("-DHAVE_FDATASYNC=0"));
        assert!(!selects_fdatasync("-DHAVE_FDATASYNC=1 -UHAVE_FDATASYNC"));
        assert!(!selects_fdatasync("-DHAVE_FDATASYNCS=1"));
    }

    /// The flags this build passed to SQLite select `fdatasync`.
    #[test]
    fn this_build_syncs_with_fdatasync() {
        assert!(selects_fdatasync(env!("INODEDB_SQLITE_FLAGS")));
    }
}
