//! Path resolution through a warm cache.
//!
//!     cargo run --release -p inodedb-tests --example resolve
//!
//! Resolves a six-level path through cached entries, as the writer and a
//! cached client do for each op and read; then reads the file by handle
//! under a key rooted five levels up, which checks the file lies beneath
//! the key's root, and makes and checks a handle's MAC as the server does.

use inodedb_core::cache::Cache;
use inodedb_core::query::{Query, Target, resolve, run};
use inodedb_core::types::{Entry, Ino, Inode, Kind, Name, Path, ROOT};
use inodedb_core::view::Fact;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let mut cache = Cache::new();
    let names = [
        "clickhouse",
        "tenant-42",
        "store",
        "abc",
        "abcdef01-2345",
        "all_1_1_0",
    ];
    // Each level holds 1,000 siblings, so lookups search real maps.
    let mut dir = ROOT;
    let mut levels: Vec<Ino> = Vec::new();
    cache.fill(Fact::Inode(ROOT, Some(Inode::dir(1, 0, ROOT))));
    for (depth, name) in names.iter().enumerate() {
        let child = 10 + depth as u64 * 10_000;
        let mut entries: Vec<(Name, Option<Entry>)> = (0..1_000)
            .map(|index| {
                (
                    Name::from(format!("sibling-{index:04}").as_bytes()),
                    Some(Entry {
                        child: child + 1 + index,
                        kind: Kind::Dir,
                        version: 1,
                    }),
                )
            })
            .collect();
        entries.push((
            Name::from(name.as_bytes()),
            Some(Entry {
                child,
                kind: if depth + 1 == names.len() {
                    Kind::File
                } else {
                    Kind::Dir
                },
                version: 1,
            }),
        ));
        cache.fill(Fact::Listing {
            dir,
            entries,
            complete: true,
        });
        let inode = if depth + 1 == names.len() {
            Inode::file(1, 0, 0)
        } else {
            Inode::dir(1, 0, dir)
        };
        cache.fill(Fact::Inode(child, Some(inode)));
        if depth + 1 == names.len() {
            cache.fill(Fact::Backptrs(
                child,
                [(dir, Name::from(name.as_bytes()))].into(),
            ));
        }
        levels.push(child);
        dir = child;
    }
    let path: Path = names.join("/").parse().expect("a valid path");
    let rounds = 2_000_000;
    let started = Instant::now();
    for _ in 0..rounds {
        black_box(
            resolve(&cache, ROOT, black_box(&path))
                .expect("cached")
                .expect("found"),
        );
    }
    let resolved = started.elapsed() / rounds;
    let query = Query::Stat(Target::Path(path));
    let started = Instant::now();
    for _ in 0..rounds {
        black_box(run(&cache, ROOT, black_box(&query)).expect("cached"));
    }
    let stat = started.elapsed() / rounds;
    let (file, key_root) = (levels[names.len() - 1], levels[0]);
    let by_handle = Query::Stat(Target::Ino(file));
    let started = Instant::now();
    for _ in 0..rounds {
        black_box(run(&cache, key_root, black_box(&by_handle)).expect("cached"));
    }
    let scoped = started.elapsed() / rounds;
    let mut request = tonic::Request::new(());
    request.metadata_mut().insert(
        "authorization",
        "Bearer 0000000000000001.0123456789abcdef"
            .parse()
            .expect("a header"),
    );
    let (_, scope) = inodedb_server::auth::Auth::new([7; 32])
        .caller(&request)
        .expect("a well-formed key");
    let started = Instant::now();
    for ino in 0..rounds as u64 {
        black_box(
            scope
                .check(&scope.handle(black_box(ino)))
                .expect("its own handle"),
        );
    }
    let mac = started.elapsed() / rounds;
    println!(
        "six-level resolution: {} ns; stat through the query engine: {} ns; \
         stat by handle under a key five levels up: {} ns; make and check a handle: {} ns",
        resolved.as_nanos(),
        stat.as_nanos(),
        scoped.as_nanos(),
        mac.as_nanos()
    );
}
