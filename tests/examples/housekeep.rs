//! A housekeeping pass over many tenants' tables, against a running
//! server.
//!
//!     cargo run --release -p inodedb-tests --example housekeep -- setup ENDPOINT ADMIN_KEY_FILE KEYS_FILE TENANTS COVERED
//!     cargo run --release -p inodedb-tests --example housekeep -- pass ENDPOINT ADMIN_KEY_FILE KEYS_FILE IN_FLIGHT
//!
//! The tree has the shape a ClickHouse disk gives it. A tenant is
//! `tenants/tN` with a key rooted at it and two tables under
//! `disk/tree/store/abc/`: `daily`, whose parts hold 11 files, and
//! `events`, whose parts hold 13 files and a directory `by_kind.proj` of
//! 10. `setup` gives each table COVERED parts to remove and two to keep,
//! a file `disk/coord/store/abc/<table>/claims/<part>` for each part to
//! remove, and writes the tenants' keys to KEYS_FILE.
//!
//! `pass` housekeeps each tenant once, its two tables one after the
//! other, IN_FLIGHT tenants at a time. A housekeep connects with the
//! tenant's key, takes a lease, lists the table, stats the newest part,
//! removes the covered parts and their claim files in one commit, lists
//! three directories and releases the lease. Then it lists the free queue,
//! claims and acknowledges, until a round claims nothing. The pass reports
//! parts removed a second, then how long the freeing takes to level
//! (`freed_through` under the tree's root reaching the version read).
//! `LEASE=0` leaves the lease out.

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use inodedb::{Client, Grant, Op};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

const TREE: &str = "disk/tree/store/abc";
const COORD: &str = "disk/coord/store/abc";
const TABLES: [(&str, usize, usize); 2] = [("daily", 11, 0), ("events", 13, 10)];

fn part_ops(root: &str, table: &str, files: usize, nested: usize, part: usize) -> Vec<Op> {
    let dir = format!("{root}/{TREE}/{table}/all_{part}_{part}_0");
    let mut ops = vec![Op::mkdir(dir.clone())];
    ops.extend((0..files).map(|file| Op::put(format!("{dir}/f{file}.bin"), vec![b'b'; 200])));
    if nested > 0 {
        ops.push(Op::mkdir(format!("{dir}/by_kind.proj")));
        ops.extend(
            (0..nested)
                .map(|file| Op::put(format!("{dir}/by_kind.proj/f{file}.bin"), vec![b'b'; 200])),
        );
    }
    ops
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mode, endpoint, admin, keys_file) = (&args[0], &args[1], &args[2], &args[3]);
    let admin_key = std::fs::read_to_string(admin).expect("reads the admin key");
    let db = Client::connect(endpoint.clone(), admin_key.trim())
        .await
        .expect("connects");
    match mode.as_str() {
        "setup" => {
            let tenants: usize = args[4].parse().expect("tenants");
            let covered: usize = args[5].parse().expect("covered");
            let started = Instant::now();
            db.commit([Op::mkdir("tenants")]).await.expect("mkdir");
            let next = Arc::new(AtomicUsize::new(0));
            let tasks: Vec<_> = (0..16)
                .map(|_| {
                    let (db, next) = (db.clone(), next.clone());
                    tokio::spawn(async move {
                        let mut keys = Vec::new();
                        loop {
                            let n = next.fetch_add(1, Relaxed);
                            if n >= tenants {
                                return keys;
                            }
                            let root = format!("tenants/t{n}");
                            let mut ops = vec![Op::mkdir(root.clone())];
                            for dir in [
                                "disk",
                                "disk/tree",
                                "disk/tree/store",
                                TREE,
                                "disk/coord",
                                "disk/coord/store",
                                COORD,
                            ] {
                                ops.push(Op::mkdir(format!("{root}/{dir}")));
                            }
                            for (table, _, _) in TABLES {
                                ops.push(Op::mkdir(format!("{root}/{TREE}/{table}")));
                                ops.push(Op::mkdir(format!("{root}/{COORD}/{table}")));
                                ops.push(Op::mkdir(format!("{root}/{COORD}/{table}/claims")));
                                ops.push(Op::mkdir(format!("{root}/{COORD}/{table}/tokens")));
                                ops.push(Op::put(
                                    format!("{root}/{COORD}/{table}/block_number"),
                                    vec![b'1'; 8],
                                ));
                            }
                            db.commit(ops).await.expect("makes the tenant");
                            let key = db
                                .mint_key(&root, Grant::Write, Duration::ZERO)
                                .await
                                .expect("mints");
                            for (table, files, nested) in TABLES {
                                for part in 0..covered + 2 {
                                    let mut ops = part_ops(&root, table, files, nested, part);
                                    if part < covered {
                                        ops.push(Op::put(
                                            format!(
                                                "{root}/{COORD}/{table}/claims/all_{part}_{part}_0"
                                            ),
                                            vec![b'd'; 40],
                                        ));
                                    }
                                    ops.push(Op::put(
                                        format!("{root}/{COORD}/{table}/tokens/t{part}"),
                                        vec![b't'; 40],
                                    ));
                                    db.commit(ops).await.expect("makes a part");
                                }
                            }
                            keys.push((n, key));
                        }
                    })
                })
                .collect();
            let mut keys = Vec::new();
            for task in tasks {
                keys.extend(task.await.expect("joins"));
            }
            keys.sort();
            let lines: Vec<String> = keys.iter().map(|(n, key)| format!("{n} {key}")).collect();
            std::fs::write(keys_file, lines.join("\n")).expect("writes the keys");
            println!(
                "setup: {tenants} tenants, {covered} covered parts a table, in {:.0} s",
                started.elapsed().as_secs_f64()
            );
        }
        "pass" => {
            let in_flight: usize = args[4].parse().expect("in flight");
            let lease = std::env::var("LEASE").map_or(true, |lease| lease != "0");
            let keys: Vec<String> = std::fs::read_to_string(keys_file)
                .expect("reads the keys")
                .lines()
                .map(|line| line.split_once(' ').expect("a key line").1.to_string())
                .collect();
            let tenants = keys.len();
            let keys = Arc::new(keys);
            let next = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(AtomicUsize::new(0));
            let removed = Arc::new(AtomicUsize::new(0));
            let claimed = Arc::new(AtomicUsize::new(0));
            let rounds = Arc::new(AtomicUsize::new(0));
            let started = Instant::now();
            let tasks: Vec<_> = (0..in_flight)
                .map(|_| {
                    let (keys, next, done, removed, claimed, rounds, endpoint) = (
                        keys.clone(),
                        next.clone(),
                        done.clone(),
                        removed.clone(),
                        claimed.clone(),
                        rounds.clone(),
                        endpoint.clone(),
                    );
                    tokio::spawn(async move {
                        let mut slowest = Duration::ZERO;
                        loop {
                            let n = next.fetch_add(1, Relaxed);
                            if n >= keys.len() {
                                return slowest;
                            }
                            for (table, _, _) in TABLES {
                                let began = Instant::now();
                                // One worker process a table: its own connection.
                                let db = Client::connect(endpoint.clone(), keys[n].clone())
                                    .await
                                    .expect("connects as the tenant");
                                let held = if lease {
                                    Some(db.lease_current().await.expect("takes a lease"))
                                } else {
                                    None
                                };
                                let dir = format!("{TREE}/{table}");
                                let listing = db.list(&dir).await.expect("lists the table");
                                let mut names: Vec<Vec<u8>> = listing
                                    .entries
                                    .iter()
                                    .map(|entry| entry.name.to_vec())
                                    .collect();
                                names.sort();
                                let covered = names.len().saturating_sub(2);
                                if let Some(covering) = names.last() {
                                    let path =
                                        format!("{dir}/{}", String::from_utf8_lossy(covering));
                                    db.stat(path.as_str())
                                        .await
                                        .expect("stats the covering part");
                                }
                                let mut ops = Vec::new();
                                for name in &names[..covered] {
                                    let name = String::from_utf8_lossy(name);
                                    ops.push(Op::remove_all(format!("{dir}/{name}")));
                                    ops.push(Op::remove_all(format!("{COORD}/{table}/{name}")));
                                    ops.push(Op::unlink(format!("{COORD}/{table}/claims/{name}")));
                                }
                                if !ops.is_empty() {
                                    db.commit(ops).await.expect("removes the covered parts");
                                    removed.fetch_add(covered, Relaxed);
                                }
                                db.list(format!("{COORD}/{table}/tokens"))
                                    .await
                                    .expect("lists tokens");
                                db.list(format!("{COORD}/{table}/claims"))
                                    .await
                                    .expect("lists claims");
                                db.list(&dir).await.expect("lists the table again");
                                if let Some(held) = held {
                                    held.release().await;
                                }
                                loop {
                                    rounds.fetch_add(1, Relaxed);
                                    db.list_freed(None, 1_000).await.expect("lists freed");
                                    let claim = db.claim_freed(1_000).await.expect("claims");
                                    if claim.entries.is_empty() {
                                        break;
                                    }
                                    claimed.fetch_add(claim.entries.len(), Relaxed);
                                    db.ack_freed(claim.id, &claim.entries)
                                        .await
                                        .expect("acknowledges");
                                }
                                slowest = slowest.max(began.elapsed());
                            }
                            done.fetch_add(1, Relaxed);
                        }
                    })
                })
                .collect();
            let reporter = {
                let (done, removed, claimed, rounds) = (
                    done.clone(),
                    removed.clone(),
                    claimed.clone(),
                    rounds.clone(),
                );
                tokio::spawn(async move {
                    let (mut last_done, mut last_removed) = (0, 0);
                    loop {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        let (d, r) = (done.load(Relaxed), removed.load(Relaxed));
                        println!(
                            "pass: {:4.0} s: {d} tenants, {r} parts removed, {} entries claimed in {} rounds; {:.1} tenants/s, {:.0} parts/s",
                            started.elapsed().as_secs_f64(),
                            claimed.load(Relaxed),
                            rounds.load(Relaxed),
                            (d - last_done) as f64 / 10.0,
                            (r - last_removed) as f64 / 10.0
                        );
                        (last_done, last_removed) = (d, r);
                    }
                })
            };
            let mut slowest = Duration::ZERO;
            for task in tasks {
                slowest = slowest.max(task.await.expect("joins"));
            }
            let elapsed = started.elapsed().as_secs_f64();
            reporter.abort();
            println!(
                "pass done: {in_flight} in flight, lease {lease}, {tenants} tenants in {elapsed:.0} s, {:.1} tenants/s, {:.0} parts/s, slowest housekeep {:.2} s",
                tenants as f64 / elapsed,
                removed.load(Relaxed) as f64 / elapsed,
                slowest.as_secs_f64()
            );
            let waited = Instant::now();
            loop {
                let page = db.list_freed(None, 1).await.expect("lists freed");
                if page.freed_through >= page.version {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            println!(
                "freeing level {:.0} s after the pass",
                waited.elapsed().as_secs_f64()
            );
        }
        other => panic!("unknown mode {other}"),
    }
}
