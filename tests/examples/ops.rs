//! Operations per second through the server's gRPC API, by backend.
//!
//!     cargo run --release -p inodedb-tests --example ops --features rocksdb,fdb [-- SECONDS [BACKENDS [OPERATIONS]]]
//!
//! The server runs on a runtime of its own with `SERVER_THREADS` workers,
//! as a separate process would, and each client opens its own connection.
//! The tree holds `FILES` files of 100 bytes in one directory and a
//! directory of 100 more for listings. A server restarted over the store
//! without preloading starts with nothing cached: a cold stat is each
//! file's first, and a cold read each value's first. Then a server that
//! preloads, as servers do by default, starts, and clients stat each file
//! once as it reads the tree; the rest run against its cache. Each client
//! waits for each answer before sending the next.
//!
//! A third argument names the operations to run, such as `stat,put`, to
//! profile one; it skips those it leaves out of the warm phase.

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
    fn start<S: Storage + Clone>(storage: S, preload: bool) -> Server {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds a local port");
        listener.set_nonblocking(true).expect("nonblocking");
        let endpoint = format!("http://{}", listener.local_addr().expect("bound"));
        let mut settings = Settings {
            init_key: Some(KEY.to_string()),
            ..Settings::default()
        };
        settings.writer.address = endpoint.clone();
        settings.writer.preload = preload;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(SERVER_THREADS)
            .thread_name("server")
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
            inodedb_server::end_runtime(runtime);
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    StatPreloaded,
    StatCold,
    ReadCold,
    Stat,
    Read,
    List,
    Put,
    Rename,
    /// Nine stats for each put of a new file.
    Mixed,
}

impl Operation {
    /// What a third argument calls it.
    fn short(self) -> &'static str {
        match self {
            Operation::StatPreloaded | Operation::StatCold | Operation::ReadCold => "cold",
            Operation::Stat => "stat",
            Operation::Read => "read",
            Operation::List => "list",
            Operation::Put => "put",
            Operation::Rename => "rename",
            Operation::Mixed => "mixed",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Operation::StatPreloaded => "stat, as a preloading server starts",
            Operation::StatCold => "stat, cold cache",
            Operation::ReadCold => "read a 100-byte value, cold cache",
            Operation::Stat => "stat",
            Operation::Read => "read a 100-byte value",
            Operation::List => "list 100 entries",
            Operation::Put => "put a new 100-byte file",
            Operation::Rename => "rename a file",
            Operation::Mixed => "nine stats for each put",
        }
    }

    /// Runs the operation's `index`th time for client `client`.
    async fn run(self, db: &Client, client: usize, clients: usize, index: usize) {
        // Clients spread over the files, each striding through them.
        let file = (client + index * clients) * 7_919 % FILES;
        match self {
            Operation::StatPreloaded | Operation::StatCold | Operation::Stat => {
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
                // Each run names its own files, and a put that finds one
                // there fails.
                let path = format!("c{client}/run{clients}-f{index}");
                let commit = db
                    .commit([Op::put(path, vec![7u8; 100]).create()])
                    .await
                    .expect("commits");
                assert!(commit.all_applied());
            }
            Operation::Mixed => {
                if index.is_multiple_of(10) {
                    let path = format!("c{client}/mix{clients}-f{index}");
                    let commit = db
                        .commit([Op::put(path, vec![7u8; 100]).create()])
                        .await
                        .expect("commits");
                    assert!(commit.all_applied());
                } else {
                    db.stat(format!("t/f{file}")).await.expect("stats");
                }
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
                        Operation::StatPreloaded | Operation::StatCold | Operation::ReadCold => {
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

async fn bench<S: Storage + Clone>(backend: &str, storage: S, seconds: u64, chosen: Option<&str>) {
    let runs = |operation: Operation| {
        chosen.is_none_or(|chosen| chosen.split(',').any(|name| name == operation.short()))
    };
    let server = Server::start(storage.clone(), false);
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

    // A server restarted over the store without preloading starts with
    // nothing cached.
    let cold = 16;
    if runs(Operation::StatCold) {
        let server = Server::start(storage.clone(), false);
        for operation in [Operation::StatCold, Operation::ReadCold] {
            report(
                backend,
                operation,
                cold,
                &drive(&server.endpoint, operation, cold, seconds).await,
            );
        }
        server.stop().await;
    }

    // A server that preloads reads the tree as clients start asking.
    let server = Server::start(storage, true);
    let preloaded = drive(&server.endpoint, Operation::StatPreloaded, cold, seconds).await;
    if runs(Operation::StatPreloaded) {
        report(backend, Operation::StatPreloaded, cold, &preloaded);
    }
    for operation in [
        Operation::Stat,
        Operation::Read,
        Operation::List,
        Operation::Put,
        Operation::Rename,
        Operation::Mixed,
    ] {
        if !runs(operation) {
            continue;
        }
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

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .thread_name("client")
        .enable_all()
        .build()
        .expect("starts a runtime")
        .block_on(run());
}

async fn run() {
    let mut args = std::env::args().skip(1);
    let seconds = args.next().and_then(|arg| arg.parse().ok()).unwrap_or(3);
    // The backends to run, in order: memory, sqlite, rocksdb, fdb.
    let order = args
        .next()
        .unwrap_or_else(|| "memory,sqlite,rocksdb,fdb".to_string());
    let chosen = args.next();
    let chosen = chosen.as_deref();
    println!("| Backend | Operation | Clients | Ops/s | p50 ms | p99 ms |");
    println!("| --- | --- | ---: | ---: | ---: | ---: |");
    for backend in order.split(',') {
        match backend {
            "memory" => bench("memory", inodedb_storage::Memory::new(), seconds, chosen).await,
            "sqlite" => {
                let dir = tempfile::tempdir().expect("a scratch directory");
                let path = dir.path().join("db.sqlite");
                let storage = inodedb_storage_sqlite::Sqlite::open(path).expect("opens");
                bench("SQLite", storage, seconds, chosen).await;
            }
            #[cfg(feature = "rocksdb")]
            "rocksdb" => {
                let dir = tempfile::tempdir().expect("a scratch directory");
                let storage =
                    inodedb_storage_rocksdb::Rocks::open(dir.path().join("db")).expect("opens");
                bench("RocksDB", storage, seconds, chosen).await;
            }
            #[cfg(feature = "fdb")]
            "fdb" => {
                use inodedb_storage_conformance::Backend;
                let mut fdb = inodedb_storage_conformance::backends::Fdb::new();
                bench("FoundationDB", fdb.fresh(), seconds, chosen).await;
            }
            other => eprintln!("skipping {other}: unknown, or built without its feature"),
        }
    }
}
