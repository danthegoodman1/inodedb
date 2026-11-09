//! What InodeDB's state costs, which its defaults rest on: the log's bytes
//! per batch, the writer's memory per live request record and per cached
//! file, what each cached record holds against what the cache charges for
//! it, and the undo a snapshot keeps per change.
//!
//!     cargo run --release -p inodedb-tests --example footprint
//!
//! Memory counts the bytes allocations ask for, without the allocator's own
//! overhead. The writer runs alone, against storage that keeps nothing,
//! which is enough for batches whose reads the cache answers.

use inodedb_core::cache::Cache;
use inodedb_core::delta::Links;
use inodedb_core::feed::{Subscriber, route};
use inodedb_core::keys::{KvRead, KvResult};
use inodedb_core::log::LogEntry;
use inodedb_core::ops::{Batch, OpSpec};
use inodedb_core::types::{Entry, Inode, Kind, Name, ROOT};
use inodedb_core::view::Fact;
use inodedb_core::writer::{Action, CommitOutcome, Config, Writer};
use inodedb_sim::harness::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

// SAFETY: every call goes straight to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

/// Carries out the writer's actions against storage that holds nothing:
/// reads find nothing, and commits apply.
fn drive(writer: &mut Writer) {
    loop {
        let actions = writer.drain();
        if actions.is_empty() {
            return;
        }
        for action in actions {
            match action {
                Action::Read { id, reads } => {
                    let results = reads
                        .iter()
                        .map(|read| match read {
                            KvRead::Get(_) => KvResult::Get(None),
                            KvRead::Scan { .. } => KvResult::Scan(Vec::new()),
                        })
                        .collect();
                    writer.read_done(id, results);
                }
                Action::Commit { id, .. } => writer.commit_done(id, CommitOutcome::Committed),
                Action::Resolve { .. } | Action::Answer { .. } => {}
            }
        }
    }
}

fn batch(ops: Vec<OpSpec>, request_id: u128) -> Batch {
    Batch {
        ops,
        request_id,
        ..Batch::default()
    }
}

/// The writer's bytes per item, over `count` batches that each add one,
/// and what its cache charges for each.
fn per_item(count: u64, make: impl Fn(u64) -> Batch) -> (f64, f64) {
    let mut writer = Writer::new(Config::default(), 1, 1_000, [7; 32]);
    drive(&mut writer);
    writer.submit(
        0,
        inodedb_core::auth::Caller::Trusted(ROOT),
        batch(vec![mkdir("d")], 0),
    );
    drive(&mut writer);
    let before = live();
    let charged = |writer: &Writer| writer.cache().tree_bytes() + writer.cache().value_bytes();
    let charged_before = charged(&writer);
    for index in 0..count {
        writer.submit(
            index,
            inodedb_core::auth::Caller::Trusted(ROOT),
            make(index),
        );
        drive(&mut writer);
    }
    assert_eq!(writer.pending(), 0);
    (
        (live() - before) as f64 / count as f64,
        (charged(&writer) - charged_before) as f64 / count as f64,
    )
}

/// The writer's bytes per key, over `count` keys an admin key mints with
/// one TTL: while they live, and once they have expired and gone.
fn per_key(count: u64) -> (f64, f64) {
    use inodedb_core::auth::{Caller, Grant, KeyOp};
    let mut writer = Writer::new(Config::default(), 1, 1_000, [7; 32]);
    drive(&mut writer);
    let admin = Caller::Key {
        id: 1 << 40,
        hash: [1; 32],
    };
    let mint = |id, hash, grant, ttl| KeyOp::Mint {
        id,
        hash,
        root: inodedb_core::types::Path::root(),
        grant,
        ttl,
    };
    writer.submit_key(
        0,
        Caller::Trusted(ROOT),
        mint(Some(1 << 40), [1; 32], Grant::Admin, 0),
    );
    drive(&mut writer);
    let before = live();
    for index in 0..count {
        writer.submit_key(index + 1, admin, mint(None, [2; 32], Grant::Read, 60_000));
        drive(&mut writer);
    }
    assert_eq!(writer.keys().len() as u64, count + 1);
    let held = live() - before;
    writer.set_now(62_000);
    writer.tick();
    drive(&mut writer);
    assert_eq!(writer.keys().len(), 1);
    let left = live() - before;
    (held as f64 / count as f64, left as f64 / count as f64)
}

/// The bytes of the log entry and request record each batch adds.
fn stored(h: &mut Harness, ops: Vec<OpSpec>) -> (usize, usize) {
    let before: usize = h
        .storage
        .dump()
        .iter()
        .map(|(k, v)| k.len() + v.len())
        .sum();
    let version = h.commit(batch(ops, 0)).version;
    let log = h.storage.log_now(version, 1)[0].1.len();
    let after: usize = h
        .storage
        .dump()
        .iter()
        .map(|(k, v)| k.len() + v.len())
        .sum();
    (log, after.saturating_sub(before))
}

fn main() {
    println!("| Batch | Log entry bytes | Tree and record bytes |");
    println!("| --- | ---: | ---: |");
    let mut h = Harness::new();
    h.ops([mkdir_p("a/b/c"), mkdir("x")]);
    let value = "v".repeat(100);
    let rows: Vec<(&str, Vec<OpSpec>)> = vec![
        ("put a new 100-byte file", vec![put("a/b/c/f", &value)]),
        ("replace its value", vec![put("a/b/c/f", &value)]),
        ("mkdir", vec![mkdir("a/b/c/d")]),
        (
            "rename a file across directories",
            vec![mv("a/b/c/f", "x/f")],
        ),
        ("unlink a file", vec![unlink("x/f")]),
    ];
    for (name, ops) in rows {
        let (log, tree) = stored(&mut h, ops);
        println!("| {name} | {log} | {tree} |");
    }
    let count = 200_000;
    let (record, _) = per_item(count, |index| {
        batch(vec![set_mtime("d", index)], u128::from(index + 1) | 1 << 64)
    });
    let (file, file_charged) = per_item(count, |index| {
        batch(vec![put(&format!("d/f{index}"), "")], 0)
    });
    let (key, gone) = per_key(50_000);
    println!();
    println!("| Writer state | Held | Charged |");
    println!("| --- | ---: | ---: |");
    println!("| Live request record (`set_mtime` batches with request ids) | {record:.0} | none |");
    println!(
        "| Cached empty file (inode, entry, back-pointer, value) | {file:.0} | {file_charged:.0} |"
    );
    println!("| Live key with a TTL | {key:.0} | none |");
    println!("| Key minted by a key, once expired and deleted | {gone:.0} | none |");
    println!();
    println!("| Cached record | Held | Charged |");
    println!("| --- | ---: | ---: |");
    let name = |index: u64| Name::from(format!("f{index:06}").as_bytes());
    let entry = |child: u64| {
        Some(Entry {
            child,
            kind: Kind::File,
            version: 1,
        })
    };
    type Make = Box<dyn Fn(u64) -> Fact>;
    let records: Vec<(&str, Make)> = vec![
        (
            "Inode record",
            Box::new(|index| Fact::Inode(index + 2, Some(Inode::file(1, 0, 0)))),
        ),
        (
            "Empty directory",
            Box::new(|index| Fact::Listing {
                dir: index + 2,
                entries: Vec::new(),
                complete: true,
            }),
        ),
        (
            "Directory of one 7-byte name",
            Box::new(move |index| Fact::Listing {
                dir: index + 2,
                entries: vec![(name(index), entry(index))],
                complete: true,
            }),
        ),
        (
            "7-byte name in a large directory",
            Box::new(move |index| Fact::Entry(1, name(index), entry(index + 2))),
        ),
        (
            "File's one link, by a 7-byte name",
            Box::new(move |index| Fact::Backptrs(index + 2, Links::from([(1, name(index))]))),
        ),
        (
            "Empty value",
            Box::new(|index| Fact::Value(index + 2, 1, Some(bytes::Bytes::new()))),
        ),
        (
            "100-byte value",
            Box::new(|index| Fact::Value(index + 2, 1, Some(bytes::Bytes::from(vec![0; 100])))),
        ),
    ];
    for (record, fact) in records {
        let mut cache = Cache::new();
        let before = live();
        for index in 0..count {
            cache.fill(fact(index));
        }
        let held = (live() - before) as f64 / count as f64;
        let charged = (cache.tree_bytes() + cache.value_bytes()) as f64 / count as f64;
        println!("| {record} | {held:.0} | {charged:.0} |");
    }
    let mut h = Harness::new();
    h.ops([mkdir("t")]);
    for index in 0..1_000 {
        h.ops([put(&format!("t/f{index}"), "v")]);
    }
    let root = match h.stat("t") {
        inodedb_core::query::QueryResult::Stat { ino, .. } => ino,
        other => panic!("{other:?}"),
    };
    let start = h.writer.version();
    for index in 0..10_000u64 {
        h.ops([put(&format!("t/f{}", index % 1_000), "w")]);
    }
    let changes: Vec<_> = h
        .storage
        .log_now(start + 1, usize::MAX)
        .into_iter()
        .filter_map(|(_, bytes)| route(&LogEntry::decode(&bytes).expect("decodes"), root, ROOT))
        .collect();
    // The same changes with a snapshot open and without: the difference is
    // what the undo holds.
    let held = |snapshot: bool| {
        let mut subscriber = Subscriber::new(root, start);
        if snapshot {
            subscriber.open_snapshot();
        }
        let before = live();
        for change in &changes {
            subscriber.apply(change);
        }
        let held = live() - before;
        (held, subscriber.undo().bytes())
    };
    let (with, counted) = held(true);
    let (without, _) = held(false);
    println!();
    println!("| Snapshot undo per replaced value | Bytes each |");
    println!("| --- | ---: |");
    println!(
        "| Held, as the allocator counts it | {:.0} |",
        (with - without) as f64 / 10_000.0
    );
    println!(
        "| Charged to `undo_bytes` | {:.0} |",
        counted as f64 / 10_000.0
    );
}
