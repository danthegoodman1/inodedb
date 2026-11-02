//! Backends for the suites: each makes fresh stores in a scratch directory
//! and reopens them from what they made durable.

use crate::Backend;
use inodedb_storage::Memory;
use inodedb_storage_sqlite::Sqlite;
use std::path::PathBuf;

/// Memory keeps nothing across a reopen, so reopening keeps the same map,
/// as a process that never crashed would.
pub struct Mem;

impl Backend for Mem {
    type S = Memory;

    fn fresh(&mut self) -> Memory {
        Memory::new()
    }

    fn reopen(&mut self, store: Memory) -> Memory {
        store
    }
}

/// Numbered stores under one scratch directory.
pub struct Scratch {
    dir: tempfile::TempDir,
    count: usize,
}

impl Scratch {
    fn new() -> Scratch {
        Scratch {
            dir: tempfile::tempdir().expect("a scratch directory"),
            count: 0,
        }
    }

    fn next(&mut self) -> PathBuf {
        self.count += 1;
        self.current()
    }

    fn current(&self) -> PathBuf {
        self.dir.path().join(format!("db-{}", self.count))
    }
}

pub struct Lite(Scratch);

impl Lite {
    pub fn new() -> Lite {
        Lite(Scratch::new())
    }
}

impl Default for Lite {
    fn default() -> Lite {
        Lite::new()
    }
}

impl Backend for Lite {
    type S = Sqlite;

    fn fresh(&mut self) -> Sqlite {
        Sqlite::open(self.0.next()).expect("opens")
    }

    fn reopen(&mut self, store: Sqlite) -> Sqlite {
        drop(store);
        Sqlite::open(self.0.current()).expect("reopens")
    }
}

pub struct Lion(Scratch);

impl Lion {
    pub fn new() -> Lion {
        Lion(Scratch::new())
    }
}

impl Default for Lion {
    fn default() -> Lion {
        Lion::new()
    }
}

impl Backend for Lion {
    type S = inodedb_storage_lionturtle::Lionturtle;

    fn fresh(&mut self) -> Self::S {
        inodedb_storage_lionturtle::Lionturtle::open(self.0.next()).expect("opens")
    }

    fn reopen(&mut self, store: Self::S) -> Self::S {
        drop(store);
        inodedb_storage_lionturtle::Lionturtle::open(self.0.current()).expect("reopens")
    }
}

#[cfg(feature = "rocksdb")]
pub struct Rock(Scratch);

#[cfg(feature = "rocksdb")]
impl Rock {
    pub fn new() -> Rock {
        Rock(Scratch::new())
    }
}

#[cfg(feature = "rocksdb")]
impl Default for Rock {
    fn default() -> Rock {
        Rock::new()
    }
}

#[cfg(feature = "rocksdb")]
impl Backend for Rock {
    type S = inodedb_storage_rocksdb::Rocks;

    fn fresh(&mut self) -> Self::S {
        inodedb_storage_rocksdb::Rocks::open(self.0.next()).expect("opens")
    }

    fn reopen(&mut self, store: Self::S) -> Self::S {
        drop(store);
        inodedb_storage_rocksdb::Rocks::open(self.0.current()).expect("reopens")
    }
}

/// Stores under fresh prefixes in the cluster the default cluster file
/// names, one network thread for the process.
#[cfg(feature = "fdb")]
pub struct Fdb {
    run: String,
    count: usize,
}

#[cfg(feature = "fdb")]
impl Fdb {
    pub fn new() -> Fdb {
        inodedb_storage_fdb::boot_for_process();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        Fdb {
            run: format!("test-{}-{nanos}", std::process::id()),
            count: 0,
        }
    }

    fn prefix(&self) -> Vec<u8> {
        format!("{}-{}/", self.run, self.count).into_bytes()
    }
}

#[cfg(feature = "fdb")]
impl Default for Fdb {
    fn default() -> Fdb {
        Fdb::new()
    }
}

#[cfg(feature = "fdb")]
impl Backend for Fdb {
    type S = inodedb_storage_fdb::Fdb;

    fn fresh(&mut self) -> Self::S {
        self.count += 1;
        inodedb_storage_fdb::Fdb::open(None, &self.prefix()).expect("opens")
    }

    fn reopen(&mut self, store: Self::S) -> Self::S {
        drop(store);
        inodedb_storage_fdb::Fdb::open(None, &self.prefix()).expect("reopens")
    }
}
