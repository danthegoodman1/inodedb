//! `inodedb --config <path> [--init-key <file>]`: serves the configured
//! backend. With `--init-key`, a store with no keys gets a first admin key,
//! rooted at the tree's root: the one in the file, or a new one written
//! there. `INODEDB_ADVERTISE`, when set, is the address standbys name for
//! this server in place of the configured one.

use inodedb_server::config::{Config, StorageConfig};
use std::process::ExitCode;
use tokio::net::TcpListener;

fn usage() -> ExitCode {
    eprintln!("usage: inodedb --config <path> [--init-key <file>]");
    ExitCode::from(2)
}

/// The key in `path`, or a new one written there, readable by its owner
/// alone.
fn initial_key(path: &std::path::Path) -> Result<String, String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    match std::fs::read_to_string(path) {
        Ok(key) => return Ok(key.trim().to_string()),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(format!("cannot read {}: {error}", path.display()));
        }
        Err(_) => {}
    }
    let (id, secret) = inodedb_server::auth::generate();
    let key = inodedb_server::auth::format(id, &secret);
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .and_then(|mut file| writeln!(file, "{key}"))
        .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    Ok(key)
}

/// Resolves on SIGINT or SIGTERM.
async fn signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("installs a SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (path, key_file) = match args.as_slice() {
        [flag, path] if flag == "--config" => (path, None),
        [flag, path, init, file] if flag == "--config" && init == "--init-key" => {
            (path, Some(file))
        }
        _ => return usage(),
    };
    let init_key = match key_file.map(|file| initial_key(std::path::Path::new(file))) {
        None => None,
        Some(Ok(key)) => Some(key),
        Some(Err(error)) => {
            eprintln!("level=error msg=\"{error}\"");
            return ExitCode::FAILURE;
        }
    };
    let config = match Config::load(std::path::Path::new(path)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("level=error msg=\"bad config\" error=\"{error}\"");
            return ExitCode::FAILURE;
        }
    };
    let listener = TcpListener::bind(config.listen);
    let shutdown = async {
        signal().await;
        eprintln!("level=info msg=\"shutting down; a second signal exits at once\"");
        tokio::spawn(async {
            signal().await;
            std::process::exit(1);
        });
    };
    let mut writer = config.core();
    // Each replica of one configuration names its own address.
    let advertised = std::env::var("INODEDB_ADVERTISE").ok();
    writer.address = advertised.clone().unwrap_or_else(|| config.advertised());
    // FoundationDB admits any number of servers; the others' files admit
    // one process.
    let standby = matches!(config.storage, StorageConfig::Fdb { .. });
    // Standbys send clients to the writer by the address it advertises.
    if standby
        && advertised.is_none()
        && config.advertise.is_none()
        && config.listen.ip().is_unspecified()
    {
        eprintln!(
            "level=error msg=\"set advertise or INODEDB_ADVERTISE: clients can't reach {}\"",
            config.listen
        );
        return ExitCode::FAILURE;
    }
    let settings = inodedb_server::Settings {
        writer,
        retention: config.log.retention(),
        init_key,
        standby,
        ..inodedb_server::Settings::default()
    };
    let served = match &config.storage {
        StorageConfig::Memory => {
            inodedb_server::serve(inodedb_storage::Memory::new(), listener, settings, shutdown)
                .await
        }
        StorageConfig::Sqlite { path } => match inodedb_storage_sqlite::Sqlite::open(path) {
            Ok(storage) => inodedb_server::serve(storage, listener, settings, shutdown).await,
            Err(error) => {
                eprintln!("level=error msg=\"cannot open sqlite\" error=\"{error}\"");
                return ExitCode::FAILURE;
            }
        },
        #[cfg(feature = "rocksdb")]
        StorageConfig::Rocksdb { path } => match inodedb_storage_rocksdb::Rocks::open(path) {
            Ok(storage) => inodedb_server::serve(storage, listener, settings, shutdown).await,
            Err(error) => {
                eprintln!("level=error msg=\"cannot open rocksdb\" error=\"{error}\"");
                return ExitCode::FAILURE;
            }
        },
        #[cfg(not(feature = "rocksdb"))]
        StorageConfig::Rocksdb { .. } => {
            eprintln!("level=error msg=\"built without rocksdb; rebuild with --features rocksdb\"");
            return ExitCode::FAILURE;
        }
        #[cfg(feature = "fdb")]
        StorageConfig::Fdb {
            cluster_file,
            prefix,
        } => {
            let network = inodedb_storage_fdb::boot();
            let cluster_file = cluster_file.as_ref().and_then(|path| path.to_str());
            let served = match inodedb_storage_fdb::Fdb::open(cluster_file, prefix.as_bytes()) {
                Ok(storage) => inodedb_server::serve(storage, listener, settings, shutdown).await,
                Err(error) => {
                    eprintln!("level=error msg=\"cannot open foundationdb\" error=\"{error}\"");
                    return ExitCode::FAILURE;
                }
            };
            // The network thread stops before the process exits.
            drop(network);
            served
        }
        #[cfg(not(feature = "fdb"))]
        StorageConfig::Fdb { .. } => {
            eprintln!(
                "level=error msg=\"built without foundationdb; rebuild with --features fdb\""
            );
            return ExitCode::FAILURE;
        }
    };
    match served {
        Ok(inodedb_server::Ended::Shutdown) => ExitCode::SUCCESS,
        Ok(inodedb_server::Ended::WriterStopped) => {
            eprintln!("level=error msg=\"the writer stopped; exiting\"");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("level=error msg=\"server failed\" error=\"{error}\"");
            ExitCode::FAILURE
        }
    }
}
