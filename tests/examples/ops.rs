//! Operations per second through the server's gRPC API, by backend.
//!
//!     cargo run --release -p inodedb-tests --example ops --features rocksdb,fdb [-- SECONDS]
//!
//! The server runs on a runtime of its own with `SERVER_THREADS` workers,
//! as a separate process would, and each client opens its own connection.
//! The tree holds `FILES` files of 100 bytes in one directory and a
//! directory of 100 more for listings. A server restarted over the store
//! starts with nothing cached: a cold stat is each file's first, and a cold
//! read each value's first. The rest run against a warm cache, but a put
//! names a new file, which the writer reads storage to find absent. Each
//! client waits for each answer before sending the next.

use inodedb::{Client, Op};
use inodedb_server::Settings;
use inodedb_storage::Storage;
use std::time::{Duration, Instant};

const KEY: &str = "0000000000000001.0123456789abcdef";
const FILES: usize = 10_000;
const LISTED: usize = 100;
const SERVER_THREADS: usize = 8;

/// A server over `storage` on its own runtime, stopped by dropping it.
struct Server {
    endpoint: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Server {
    fn start<S: Storage + Clone>(storage: S) -> Server {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds a local port");
        listener.set_nonblocking(true).expect("nonblocking");
        let endpoint = format!("http://{}", listener.local_addr().expect("bound"));
        let mut settings = Settings {
            init_key: Some(KEY.to_string()),
            ..Settings::default()
        };
        settings.writer.address = endpoint.clone();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(SERVER_THREADS)
            .enable_all()
            .build()
            .expect("starts a runtime");
        let (shutdown, signal) = tokio::sync::oneshot::channel::<()>();
        let task = runtime.spawn(async move {
            let listener = async { tokio::net::TcpListener::from_std(listener) };
            inodedb_server::serve(storage, listener, settings, async move {
                let _ = signal.await;
            })
            .await
            .expect("serves");
        });
        Server {
            endpoint,
            shutdown: Some(shutdown),
            task: Some(task),
            runtime: Some(runtime),
        }
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    StatCold,
    ReadCold,
    Stat,
    Read,
    List,
    Put,
    Rename,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Operation::StatCold => "stat, cold cache",
            Operation::ReadCold => "read a 100-byte value, cold cache",
            Operation::Stat => "stat",
            Operation::Read => "read a 100-byte value",
            Operation::List => "list 100 entries",
            Operation::Put => "put a new 100-byte file",
            Operation::Rename => "rename a file",
        }
    }

    /// Runs the operation's `index`th time for client `client`.
    async fn run(self, db: &Client, client: usize, clients: usize, index: usize) {
        // Clients spread over the files, each striding through them.
        let file = (client + index * clients) * 7_919 % FILES;
        match self {
            Operation::StatCold | Operation::Stat => {
                db.stat(format!("t/f{file}")).await.expect("stats");
            }
            Operation::ReadCold | Operation::Read => {
                db.read(format!("t/f{file}")).await.expect("reads");
            }
            Operation::List => {
                let listing = db.list("l").await.expect("lists");
                assert_eq!(listing.entries.len(), LISTED);
            }
            Operation::Put => {
                let commit = db
                    .commit([Op::put(format!("c{client}/f{index}"), vec![7u8; 100])])
                    .await
                    .expect("commits");
                assert!(commit.all_applied());
            }
            Operation::Rename => {
                let (from, to) = if index.is_multiple_of(2) {
                    ("a", "b")
                } else {
                    ("b", "a")
                };
                let commit = db
                    .commit([Op::rename(
                        format!("c{client}/{from}"),
                        format!("c{client}/{to}"),
                    )])
                    .await
                    .expect("commits");
                assert!(commit.all_applied());
            }
        }
    }
}

struct Run {
    elapsed: Duration,
    latencies: Vec<Duration>,
}

/// Runs `operation` from `clients` connections, for `seconds` or, cold,
/// until each file has been read once.
async fn drive(endpoint: &str, operation: Operation, clients: usize, seconds: u64) -> Run {
    let mut connections = Vec::new();
    for _ in 0..clients {
        connections.push(
            Client::connect(endpoint.to_string(), KEY)
                .await
                .expect("connects"),
        );
    }
    let until = Instant::now() + Duration::from_secs(seconds);
    let started = Instant::now();
    let tasks: Vec<_> = connections
        .into_iter()
        .enumerate()
        .map(|(client, db)| {
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                let mut index = 0;
                loop {
                    let done = match operation {
                        Operation::StatCold | Operation::ReadCold => {
                            client + index * clients >= FILES
                        }
                        // Renames end with the file back where it started.
                        Operation::Rename => index.is_multiple_of(2) && Instant::now() >= until,
                        _ => Instant::now() >= until,
                    };
                    if done {
                        return latencies;
                    }
                    let sent = Instant::now();
                    operation.run(&db, client, clients, index).await;
                    latencies.push(sent.elapsed());
                    index += 1;
                }
            })
        })
        .collect();
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.expect("joins"));
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    Run { elapsed, latencies }
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    sorted[((sorted.len() as f64 * fraction) as usize).min(sorted.len() - 1)]
}

fn report(backend: &str, operation: Operation, clients: usize, run: &Run) {
    println!(
        "| {backend} | {} | {clients} | {:.0} | {:.3} | {:.3} |",
        operation.name(),
        run.latencies.len() as f64 / run.elapsed.as_secs_f64(),
        percentile(&run.latencies, 0.5).as_secs_f64() * 1000.0,
        percentile(&run.latencies, 0.99).as_secs_f64() * 1000.0,
    );
}

const CLIENTS: [usize; 2] = [1, 64];

async fn bench<S: Storage + Clone>(backend: &str, storage: S, seconds: u64) {
    let server = Server::start(storage.clone());
    let db = Client::connect(server.endpoint.clone(), KEY)
        .await
        .expect("connects");
    db.commit([Op::mkdir("t"), Op::mkdir("l")])
        .await
        .expect("makes the tree");
    for chunk in (0..FILES).collect::<Vec<_>>().chunks(1_000) {
        db.commit(
            chunk
                .iter()
                .map(|file| Op::put(format!("t/f{file}"), vec![7u8; 100])),
        )
        .await
        .expect("fills the tree");
    }
    db.commit((0..LISTED).map(|file| Op::put(format!("l/f{file}"), vec![7u8; 100])))
        .await
        .expect("fills the listing");
    let most = CLIENTS.iter().max().copied().unwrap_or(1);
    db.commit((0..most).flat_map(|client| {
        [
            Op::mkdir(format!("c{client}")),
            Op::put(format!("c{client}/a"), vec![7u8; 100]),
        ]
    }))
    .await
    .expect("makes the clients' directories");
    server.stop().await;

    // A server restarted over the store starts with nothing cached.
    let server = Server::start(storage);
    let cold = 16;
    for operation in [Operation::StatCold, Operation::ReadCold] {
        report(
            backend,
            operation,
            cold,
            &drive(&server.endpoint, operation, cold, seconds).await,
        );
    }
    for operation in [
        Operation::Stat,
        Operation::Read,
        Operation::List,
        Operation::Put,
        Operation::Rename,
    ] {
        for clients in CLIENTS {
            report(
                backend,
                operation,
                clients,
                &drive(&server.endpoint, operation, clients, seconds).await,
            );
        }
    }
    server.stop().await;
}

#[tokio::main(worker_threads = 8)]
async fn main() {
    let seconds = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(3);
    println!("| Backend | Operation | Clients | Ops/s | p50 ms | p99 ms |");
    println!("| --- | --- | ---: | ---: | ---: | ---: |");
    bench("memory", inodedb_storage::Memory::new(), seconds).await;
    let dir = tempfile::tempdir().expect("a scratch directory");
    let storage =
        inodedb_storage_sqlite::Sqlite::open(dir.path().join("db.sqlite")).expect("opens");
    bench("SQLite", storage, seconds).await;
    #[cfg(feature = "rocksdb")]
    {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let storage = inodedb_storage_rocksdb::Rocks::open(dir.path().join("db")).expect("opens");
        bench("RocksDB", storage, seconds).await;
    }
    #[cfg(feature = "fdb")]
    {
        use inodedb_storage_conformance::Backend;
        let mut fdb = inodedb_storage_conformance::backends::Fdb::new();
        bench("FoundationDB", fdb.fresh(), seconds).await;
    }
}
