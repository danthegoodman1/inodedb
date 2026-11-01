//! Batches per second through the server, with and without group commit.
//!
//!     cargo run --release -p inodedb-tests --example commits --features rocksdb,fdb [-- SECONDS]
//!
//! Each client commits one small file per batch into its own directory, as
//! fast as answers come back. "One per group" caps groups at one batch.

use inodedb::{Client, Op};
use inodedb_core::writer::Config;
use inodedb_storage::Storage;
use std::time::{Duration, Instant};

const KEY: &str = "0000000000000001.0123456789abcdef";

struct Run {
    batches: usize,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

async fn drive<S: Storage + Clone>(
    storage: S,
    config: Config,
    clients: usize,
    seconds: u64,
) -> Run {
    let server = inodedb_server::start(storage, KEY, config).await;
    let db = Client::connect(server.endpoint(), KEY)
        .await
        .expect("connects");
    db.commit((0..clients).map(|client| Op::mkdir(format!("c{client}"))))
        .await
        .expect("makes the directories");
    let until = Instant::now() + Duration::from_secs(seconds);
    let started = Instant::now();
    let tasks: Vec<_> = (0..clients)
        .map(|client| {
            let db = db.clone();
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                let mut index = 0u64;
                while Instant::now() < until {
                    let sent = Instant::now();
                    let commit = db
                        .commit([Op::put(format!("c{client}/f{index}"), vec![7u8; 100])])
                        .await
                        .expect("commits");
                    assert!(commit.all_applied());
                    latencies.push(sent.elapsed());
                    index += 1;
                }
                latencies
            })
        })
        .collect();
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.expect("joins"));
    }
    let elapsed = started.elapsed();
    server.stop().await;
    latencies.sort_unstable();
    Run {
        batches: latencies.len(),
        elapsed,
        latencies,
    }
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    sorted[((sorted.len() as f64 * fraction) as usize).min(sorted.len() - 1)]
}

fn report(backend: &str, grouping: &str, clients: usize, run: &Run) {
    println!(
        "| {backend} | {grouping} | {clients} | {:.0} | {:.2} | {:.2} |",
        run.batches as f64 / run.elapsed.as_secs_f64(),
        percentile(&run.latencies, 0.5).as_secs_f64() * 1000.0,
        percentile(&run.latencies, 0.99).as_secs_f64() * 1000.0,
    );
}

#[tokio::main]
async fn main() {
    let seconds = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(3);
    let groupings = [
        (
            "one per group",
            Config {
                group_max_bytes: 1,
                ..Config::default()
            },
        ),
        ("grouped", Config::default()),
    ];
    #[cfg(feature = "fdb")]
    let mut fdb = inodedb_storage_conformance::backends::Fdb::new();
    println!("| Backend | Commit | Clients | Batches/s | p50 ms | p99 ms |");
    println!("| --- | --- | ---: | ---: | ---: | ---: |");
    for clients in [1, 16, 128] {
        for (grouping, config) in &groupings {
            let dir = tempfile::tempdir().expect("a scratch directory");
            let storage =
                inodedb_storage_sqlite::Sqlite::open(dir.path().join("db.sqlite")).expect("opens");
            report(
                "SQLite",
                grouping,
                clients,
                &drive(storage, config.clone(), clients, seconds).await,
            );
            #[cfg(feature = "rocksdb")]
            {
                let dir = tempfile::tempdir().expect("a scratch directory");
                let storage =
                    inodedb_storage_rocksdb::Rocks::open(dir.path().join("db")).expect("opens");
                report(
                    "RocksDB",
                    grouping,
                    clients,
                    &drive(storage, config.clone(), clients, seconds).await,
                );
            }
            #[cfg(feature = "fdb")]
            {
                use inodedb_storage_conformance::Backend;
                let storage = fdb.fresh();
                report(
                    "FoundationDB",
                    grouping,
                    clients,
                    &drive(storage, config.clone(), clients, seconds).await,
                );
            }
        }
    }
}
