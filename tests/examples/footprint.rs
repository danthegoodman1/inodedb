//! What InodeDB's state costs, which its defaults rest on: the log's bytes
//! per batch, the writer's memory per live request record and per cached
//! file, and the undo a snapshot keeps per change.
//!
//!     cargo run --release -p inodedb-tests --example footprint
//!
//! Memory counts the bytes allocations ask for, without the allocator's own
//! overhead. The writer runs alone, against storage that keeps nothing,
//! which is enough for batches whose reads the cache answers.

use inodedb_core::feed::{Subscriber, route};
use inodedb_core::keys::{KvRead, KvResult};
use inodedb_core::log::LogEntry;
use inodedb_core::ops::{Batch, OpSpec};
use inodedb_core::types::ROOT;
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

/// The writer's bytes per item, over `count` batches that each add one.
fn per_item(count: u64, make: impl Fn(u64) -> Batch) -> f64 {
    let mut writer = Writer::new(Config::default(), 1, 1_000, [7; 32]);
    drive(&mut writer);
    writer.submit(
        0,
        inodedb_core::auth::Caller::Trusted(ROOT),
        batch(vec![mkdir("d")], 0),
    );
    drive(&mut writer);
    let before = live();
    for index in 0..count {
        writer.submit(
            index,
            inodedb_core::auth::Caller::Trusted(ROOT),
            make(index),
        );
        drive(&mut writer);
    }
    assert_eq!(writer.pending(), 0);
    (live() - before) as f64 / count as f64
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
    let record = per_item(count, |index| {
        batch(vec![set_mtime("d", index)], u128::from(index + 1) | 1 << 64)
    });
    let file = per_item(count, |index| {
        batch(vec![put(&format!("d/f{index}"), "")], 0)
    });
    println!();
    println!("| Writer state | Bytes each |");
    println!("| --- | ---: |");
    println!("| Live request record (`set_mtime` batches with request ids) | {record:.0} |");
    println!("| Cached empty file (inode, entry, back-pointer) | {file:.0} |");
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
