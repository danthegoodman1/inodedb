//! InodeDB's server: one writer over a storage backend, behind gRPC.

pub mod auth;
pub mod config;
pub mod convert;
pub mod engine;
pub mod feed;
pub mod service;

use auth::{Auth, Grant};
use engine::Engine;
use feed::{Log, Retention};
use inodedb_core::auth::{Caller, KeyOp};
use inodedb_core::types::{Path, ROOT};
use inodedb_core::writer::Answer;
use inodedb_proto::inode_db_server::InodeDbServer;
use inodedb_storage::Storage;
use service::{Faults, Service};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

/// The largest message either side sends: a full batch plus framing.
pub const MAX_MESSAGE: usize = 16 * 1024 * 1024;
/// How long shutdown waits for requests in progress, and then for the
/// writer to release the storage.
pub const GRACE: Duration = Duration::from_secs(20);

/// Why serving ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ended {
    Shutdown,
    /// The writer stopped, as it does when another writer commits.
    WriterStopped,
}

/// What a server runs with beyond its storage.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub writer: inodedb_core::writer::Config,
    pub retention: Retention,
    /// A key, `<id>.<secret>`, to mint rooted at the tree's root with every
    /// grant when the store has no keys.
    pub init_key: Option<String>,
    /// Faults a test injects; zero outside tests.
    pub faults: Arc<Faults>,
    /// Whether the store admits standbys: the server follows the log until
    /// the writer stops renewing, and follows again once another writer
    /// takes over. A store whose lock admits one process writes at once.
    pub standby: bool,
}

/// Mints the first key when the store has none.
async fn init_key(engine: &Engine, key: &str) -> Result<(), String> {
    let (id, secret) = auth::parse(key).ok_or("the initial key is malformed")?;
    let op = KeyOp::Mint {
        id: Some(id),
        hash: auth::hash(secret),
        root: Path::root(),
        grant: Grant::Admin,
        ttl: 0,
    };
    match engine.key(Caller::Trusted(ROOT), op).await {
        Ok(Answer::Key(Ok(_))) => {
            eprintln!("level=info msg=\"minted the first admin key\" id={id:016x}");
            Ok(())
        }
        Err(status) if service::not_writer(&status) => {
            eprintln!("level=info msg=\"a standby leaves the first key to the writer\"");
            Ok(())
        }
        other => Err(format!("minting the first key failed: {other:?}")),
    }
}

/// Why a server failed.
pub type Failure = Box<dyn std::error::Error + Send + Sync>;

/// Serves until `shutdown` resolves or the writer stops, then waits up to
/// `GRACE` each for requests in progress, for the writer to release the
/// store so a standby takes over at once, and for the engine to stop. The server listens through `listen` once the engine is ready,
/// so a connection, such as a readiness probe's, succeeds only once the
/// server answers.
pub async fn serve<S: Storage + Clone>(
    storage: S,
    listen: impl Future<Output = std::io::Result<TcpListener>>,
    settings: Settings,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<Ended, Failure> {
    let log: Arc<dyn Log> = Arc::new(storage.clone());
    let tail = Arc::new(feed::Tail::new(&settings.retention));
    let (engine, task) = Engine::spawn(storage, settings.writer, settings.standby, tail.clone());
    let watcher = engine.clone();
    let mut shutdown = std::pin::pin!(shutdown);
    // A server that can't reach its store yet still stops when asked.
    let ready = tokio::select! {
        ready = engine.ready() => ready,
        () = shutdown.as_mut() => return Ok(Ended::Shutdown),
    };
    let Ok(ready) = ready else {
        return Ok(Ended::WriterStopped);
    };
    if let Some(key) = &settings.init_key {
        if ready.keys == 0 {
            if let Err(error) = init_key(&engine, key).await {
                eprintln!("level=error msg=\"{error}\"");
            }
        } else {
            eprintln!("level=info msg=\"the store holds keys; the initial key goes unminted\"");
        }
    }
    let listener = listen.await?;
    eprintln!("level=info msg=listening addr={}", listener.local_addr()?);
    // Truncation waits for the writer, which loads the stored leases.
    let truncation = tokio::spawn(feed::truncate(
        log.clone(),
        settings.retention.clone(),
        engine.keep(),
    ));
    let (close, closing) = tokio::sync::watch::channel(false);
    let service = InodeDbServer::new(Service::new(
        engine,
        Auth::new(ready.secret),
        log,
        tail,
        settings.faults,
        closing,
    ))
    .max_decoding_message_size(MAX_MESSAGE)
    .max_encoding_message_size(MAX_MESSAGE);
    let (ended, why) = oneshot::channel();
    let fired = Arc::new(Notify::new());
    let server = inodedb_transport::serve(listener, service, {
        let (watcher, fired) = (watcher.clone(), fired.clone());
        async move {
            let _ = ended.send(tokio::select! {
                () = shutdown => Ended::Shutdown,
                () = watcher.stopped() => Ended::WriterStopped,
            });
            // Streams end, so the requests in progress can finish.
            close.send_replace(true);
            fired.notify_one();
        }
    });
    let served = tokio::select! {
        served = server => served,
        () = async {
            fired.notified().await;
            tokio::time::sleep(GRACE).await;
        } => {
            eprintln!("level=warn msg=\"requests outlived the shutdown grace; dropping them\"");
            Ok(())
        }
    };
    truncation.abort();
    let _ = truncation.await;
    if tokio::time::timeout(GRACE, watcher.release())
        .await
        .is_err()
    {
        eprintln!("level=warn msg=\"releasing the store outlived the shutdown grace\"");
    }
    drop(watcher);
    if tokio::time::timeout(GRACE, task).await.is_err() {
        eprintln!("level=warn msg=\"the writer outlived the shutdown grace\"");
    }
    served?;
    Ok(why.await.unwrap_or(Ended::Shutdown))
}

/// Linux's request for how long a core may take to leave idle, as long as the
/// file stays open.
pub const CPU_LATENCY: &str = "/dev/cpu_dma_latency";

/// Asks that no core take over a microsecond to leave idle, until the returned
/// file drops. A request wakes several threads, and on a cloud instance each
/// one that finds its core in C6 pays 190 microseconds (`BENCHMARKS.md`,
/// "Idle states"), several times what the rest of a read costs. A core in C1
/// still halts. The file takes root, or a device rule that opens it to the
/// server's user.
pub fn hold_shallow_idle(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.write_all(&1i32.to_ne_bytes())?;
    Ok(file)
}

/// mimalloc's option for how long freed memory waits, in milliseconds, before
/// the allocator returns it to the kernel. The crate names only some of
/// mimalloc's options, so this one goes by its place among them.
const PURGE_DELAY: libmimalloc_sys::mi_option_t = 15;

/// Has the allocator keep freed memory for its next allocations, for as long
/// as the process runs. Returning memory to the kernel falls to whichever
/// thread allocates next. lionturtle frees a memtable's millions of
/// allocations at each checkpoint; commits after one then stalled 4 to 40 ms
/// unmapping them, and later commits took page faults mapping them again
/// (`BENCHMARKS.md`, "Sustained puts"). The process holds the most memory it
/// has used. Call it before the process starts its threads: mimalloc stores
/// the option without synchronization.
pub fn keep_freed_memory() {
    // SAFETY: the option takes any delay, and a negative one ends purging.
    unsafe { libmimalloc_sys::mi_option_set(PURGE_DELAY, -1) };
}

/// How long ending a runtime waits for its blocking calls.
const END_RUNTIME: Duration = Duration::from_secs(10);

/// Ends a runtime and every task on it, as a process kill would, and returns
/// once they have dropped what they held. A store they held has then closed,
/// so nothing closes it once the process's exit has begun, when RocksDB's
/// static locks are gone and closing a database aborts the process. It works
/// from any thread: its own thread may block, where a runtime's may not.
pub fn end_runtime(runtime: tokio::runtime::Runtime) {
    let _ = std::thread::spawn(move || runtime.shutdown_timeout(END_RUNTIME)).join();
}

/// A server on a free local port, for tests and tools. It runs on a
/// runtime of its own, so dropping it ends every task it started, as a
/// process kill would.
pub struct Running {
    pub addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<Ended, Failure>>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Running {
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stops accepting requests and waits for the server to finish.
    pub async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = (&mut self.task).await;
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            end_runtime(runtime);
        }
    }
}

/// A server whose store gets `admin_key` as its first key.
pub async fn start<S: Storage + Clone>(
    storage: S,
    admin_key: &str,
    writer: inodedb_core::writer::Config,
) -> Running {
    start_with(
        storage,
        admin_key,
        Settings {
            writer,
            ..Settings::default()
        },
    )
    .await
}

pub async fn start_with<S: Storage + Clone>(
    storage: S,
    admin_key: &str,
    settings: Settings,
) -> Running {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds a local port");
    listener
        .set_nonblocking(true)
        .expect("makes the listener nonblocking");
    let addr = listener.local_addr().expect("a bound address");
    let mut settings = Settings {
        init_key: Some(admin_key.to_string()),
        ..settings
    };
    if settings.writer.address.is_empty() {
        settings.writer.address = format!("http://{addr}");
    }
    let (shutdown, signal) = oneshot::channel();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("starts a runtime");
    let task = runtime.spawn(async move {
        let listener = async { TcpListener::from_std(listener) };
        serve(storage, listener, settings, async move {
            let _ = signal.await;
        })
        .await
    });
    Running {
        addr,
        shutdown: Some(shutdown),
        task,
        runtime: Some(runtime),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The kernel reads the latency as a native 32-bit integer of microseconds.
    #[test]
    fn shallow_idle_asks_for_a_microsecond() {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let path = dir.path().join("cpu_dma_latency");
        std::fs::write(&path, []).expect("makes the file");
        let held = hold_shallow_idle(&path).expect("opens");
        assert_eq!(
            std::fs::read(&path).expect("reads"),
            1i32.to_ne_bytes().to_vec()
        );
        drop(held);
        assert!(hold_shallow_idle(&dir.path().join("missing")).is_err());
    }

    /// The option the server sets is mimalloc's purge delay: it starts at
    /// mimalloc's second, and the server's setting ends purging.
    #[test]
    fn the_server_keeps_freed_memory() {
        // SAFETY: reading an option changes nothing.
        let delay = || unsafe { libmimalloc_sys::mi_option_get(PURGE_DELAY) };
        if std::env::var_os("MIMALLOC_PURGE_DELAY").is_none() {
            assert_eq!(delay(), 1000, "option {PURGE_DELAY} is the purge delay");
        }
        keep_freed_memory();
        assert_eq!(delay(), -1);
    }
}
