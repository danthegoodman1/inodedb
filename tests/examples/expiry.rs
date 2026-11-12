//! Batches per second as request records begin to expire.
//!
//!     cargo run --release -p inodedb-tests --example expiry --features rocksdb,fdb -- [TTL_SECONDS [SECONDS_AFTER [BACKENDS]]]
//!
//! Every batch leaves a record, and once records reach their lifetime each
//! group deletes as many as it writes. Commits answer at this rate for as long
//! as a server keeps its load, so this is the steady state the other benchmarks
//! run too briefly to reach. 64 clients each commit one small file per batch.

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use inodedb::{Client, Op};
use inodedb_core::writer::Config;
use inodedb_storage::Storage;
use std::time::{Duration, Instant};

const KEY: &str = "0000000000000001.0123456789abcdef";
const CLIENTS: usize = 64;

fn percentile(sorted: &[Duration], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 * fraction) as usize).min(sorted.len() - 1)].as_secs_f64() * 1000.0
}

fn report(backend: &str, phase: &str, seconds: f64, mut latencies: Vec<Duration>) {
    latencies.sort_unstable();
    println!(
        "| {backend} | {phase} | {:.0} | {:.2} | {:.2} |",
        latencies.len() as f64 / seconds,
        percentile(&latencies, 0.5),
        percentile(&latencies, 0.99),
    );
}

async fn drive<S: Storage + Clone>(backend: &str, storage: S, ttl: u64, after: u64) {
    let config = Config {
        request_ttl: ttl * 1000,
        ..Config::default()
    };
    let server = inodedb_server::start(storage, KEY, config).await;
    let db = Client::connect(server.endpoint(), KEY)
        .await
        .expect("connects");
    db.commit((0..CLIENTS).map(|client| Op::mkdir(format!("c{client}"))))
        .await
        .expect("makes the directories");
    let started = Instant::now();
    let expiring = started + Duration::from_secs(ttl);
    let until = expiring + Duration::from_secs(after);
    let tasks: Vec<_> = (0..CLIENTS)
        .map(|client| {
            let db = db.clone();
            tokio::spawn(async move {
                let (mut before, mut during) = (Vec::new(), Vec::new());
                let mut index = 0u64;
                while Instant::now() < until {
                    let sent = Instant::now();
                    let commit = db
                        .commit([Op::put(format!("c{client}/f{index}"), vec![7u8; 100])])
                        .await
                        .expect("commits");
                    assert!(commit.all_applied());
                    if sent < expiring {
                        &mut before
                    } else {
                        &mut during
                    }
                    .push(sent.elapsed());
                    index += 1;
                }
                (before, during)
            })
        })
        .collect();
    let (mut before, mut during) = (Vec::new(), Vec::new());
    for task in tasks {
        let (first, second) = task.await.expect("joins");
        before.extend(first);
        during.extend(second);
    }
    server.stop().await;
    report(backend, "before records expire", ttl as f64, before);
    report(backend, "while records expire", after as f64, during);
}

fn main() {
    inodedb_server::keep_freed_memory();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("starts a runtime")
        .block_on(run());
}

async fn run() {
    let mut args = std::env::args().skip(1);
    let ttl = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(30);
    let after = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(30);
    let backends = args
        .next()
        .unwrap_or_else(|| "memory,sqlite,rocksdb,lionturtle,fdb".to_string());
    println!("| Backend | Phase | Batches/s | p50 ms | p99 ms |");
    println!("| --- | --- | ---: | ---: | ---: |");
    for backend in backends.split(',') {
        let dir = tempfile::tempdir().expect("a scratch directory");
        match backend {
            "memory" => drive("memory", inodedb_storage::Memory::new(), ttl, after).await,
            "sqlite" => {
                let path = dir.path().join("db.sqlite");
                let storage = inodedb_storage_sqlite::Sqlite::open(path).expect("opens");
                drive("SQLite", storage, ttl, after).await;
            }
            "lionturtle" => {
                let storage = inodedb_storage_lionturtle::Lionturtle::open(dir.path().join("db"))
                    .expect("opens");
                drive("lionturtle", storage, ttl, after).await;
            }
            #[cfg(feature = "rocksdb")]
            "rocksdb" => {
                let storage =
                    inodedb_storage_rocksdb::Rocks::open(dir.path().join("db")).expect("opens");
                drive("RocksDB", storage, ttl, after).await;
            }
            #[cfg(feature = "fdb")]
            "fdb" => {
                use inodedb_storage_conformance::Backend;
                let mut fdb = inodedb_storage_conformance::backends::Fdb::new();
                drive("FoundationDB", fdb.fresh(), ttl, after).await;
            }
            other => eprintln!("skipping {other}: unknown, or built without its feature"),
        }
    }
}
