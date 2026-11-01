//! `idb`: InodeDB from the shell. It reads its endpoint and key from
//! `INODEDB_ENDPOINT` and `INODEDB_KEY`.

#[cfg(feature = "mount")]
mod mount;

use clap::{Parser, Subcommand, ValueEnum};
use inodedb::{Client, Commit, Entry, Error, Grant, Inode, Kind, List, Op, Outcome, Skip};
use std::io::Write;
use std::process::ExitCode;
use tokio::io::AsyncReadExt;

#[derive(Parser)]
#[command(name = "idb", about = "InodeDB from the shell")]
struct Cli {
    #[arg(
        long,
        env = "INODEDB_ENDPOINT",
        default_value = "http://127.0.0.1:7070"
    )]
    endpoint: String,
    #[arg(long, env = "INODEDB_KEY", hide_env_values = true)]
    key: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Lists a directory.
    Ls {
        #[arg(short = 'l')]
        long: bool,
        #[arg(default_value = "")]
        path: String,
    },
    /// Prints an inode's attributes.
    Stat {
        paths: Vec<String>,
    },
    /// Prints a file's value.
    Cat {
        path: String,
    },
    /// Writes a file, from VALUE or standard input.
    Put {
        path: String,
        value: Option<String>,
        /// Fails when the name exists.
        #[arg(long, conflicts_with = "replace")]
        create: bool,
        /// Fails unless a file is there.
        #[arg(long)]
        replace: bool,
    },
    Mkdir {
        #[arg(short = 'p')]
        parents: bool,
        paths: Vec<String>,
    },
    Mv {
        from: String,
        to: String,
        /// Fails when TO exists.
        #[arg(long, conflicts_with = "exchange")]
        no_replace: bool,
        /// Swaps FROM and TO.
        #[arg(long)]
        exchange: bool,
    },
    /// Removes files, or subtrees with -r.
    Rm {
        #[arg(short = 'r')]
        recursive: bool,
        #[arg(short = 'f')]
        force: bool,
        paths: Vec<String>,
    },
    Rmdir {
        paths: Vec<String>,
    },
    /// Makes a hard link at TO to the file at FROM.
    Ln {
        from: String,
        to: String,
    },
    /// Prints the changes inside a directory's subtree as they commit, each
    /// with its version and path: `+` for a name added, `-` for one
    /// removed, `~` for one replaced or for a file whose value or
    /// attributes changed.
    Watch {
        path: String,
        /// The first version to print; by default, the next commit's.
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// Exits after printing this many changes.
        #[arg(long)]
        count: Option<usize>,
    },
    /// Checks the store's invariants, as the simulator does: through the
    /// server, under an admin key rooted at the tree's root, or in a
    /// SQLite store directly, with no server running on it.
    Fsck {
        #[arg(long)]
        sqlite: Option<std::path::PathBuf>,
    },
    /// Mounts the tree under the key's root at DIR, read-only, until it is
    /// unmounted (`fusermount3 -u DIR`). Built with the `mount` feature.
    Mount {
        dir: std::path::PathBuf,
    },
    /// Mints and revokes keys.
    #[command(subcommand)]
    Key(KeyCommand),
}

#[derive(Subcommand)]
enum KeyCommand {
    /// Mints a key rooted at a directory under this key's root, and prints
    /// it.
    Mint {
        #[arg(long, default_value = "")]
        root: String,
        #[arg(long, value_enum)]
        grant: GrantArg,
        /// How long it lasts, as 90s, 15m, 12h or 30d; it never outlasts
        /// this key. Without it, it lasts as long as this key.
        #[arg(long, value_parser = ttl)]
        ttl: Option<std::time::Duration>,
    },
    /// Revokes a key, named by its id or the whole key, with every key
    /// minted from it.
    Revoke { key: String },
}

#[derive(Clone, Copy, ValueEnum)]
enum GrantArg {
    Read,
    Write,
    Admin,
}

fn ttl(text: &str) -> Result<std::time::Duration, String> {
    let split = text.char_indices().last().map_or(0, |(at, _)| at);
    let (count, unit) = text.split_at(split);
    let count: u64 = count
        .parse()
        .map_err(|_| format!("{text}: a number and a unit: s, m, h or d"))?;
    let seconds = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return Err(format!("{text}: the unit is s, m, h or d")),
    };
    Ok(std::time::Duration::from_secs(
        count.saturating_mul(seconds),
    ))
}

/// Prefixes an error with what it concerns.
fn fail(path: &str) -> impl Fn(Error) -> String + '_ {
    move |error| format!("{path}: {error}")
}

fn kind(kind: Kind) -> char {
    match kind {
        Kind::Dir => 'd',
        Kind::File => '-',
    }
}

fn long(name: &str, inode: &Inode) -> String {
    format!(
        "{}{:04o} {:>3} {:>8} {:>14} v{:<8} {name}",
        kind(inode.kind),
        inode.mode,
        inode.nlink,
        inode.size,
        inode.mtime,
        inode.version,
    )
}

fn entry_line(entry: &Entry, long_form: bool) -> String {
    let name = entry.name_lossy();
    match (&entry.inode, long_form) {
        (Some(inode), true) => long(&name, inode),
        _ if entry.kind == Kind::Dir => format!("{name}/"),
        _ => name,
    }
}

/// Fails with the first skipped op's reason.
fn applied(commit: Commit, paths: &[String]) -> Result<(), String> {
    for (outcome, path) in commit.outcomes.iter().zip(paths) {
        if let Outcome::Skipped(skip) = outcome {
            return Err(match skip {
                Skip::Error(error) => format!("{path}: {error}"),
                Skip::When(_) | Skip::Cond(_) => format!("{path}: a condition failed"),
                Skip::Aborted => format!("{path}: another op failed"),
            });
        }
    }
    Ok(())
}

/// Prints a check's problems; fails if there are any.
fn report(
    out: &mut impl Write,
    keys: u64,
    version: u64,
    problems: &[String],
) -> Result<(), String> {
    for problem in problems {
        writeln!(out, "{problem}").map_err(|error| error.to_string())?;
    }
    match problems.len() {
        0 => writeln!(out, "{keys} keys at version {version}: sound")
            .map_err(|error| error.to_string()),
        count => Err(format!(
            "{count} problems in {keys} keys at version {version}"
        )),
    }
}

/// Checks a SQLite store directly.
/// The names a watch has shown below its directory, by inode, from which
/// it prints each inode's path as of the change it prints.
struct Names {
    top: u64,
    shown: String,
    /// Each inode's names: the directory holding each, and the name.
    names: std::collections::BTreeMap<u64, std::collections::BTreeSet<(u64, String)>>,
}

impl Names {
    fn new(top: u64, shown: &str) -> Names {
        Names {
            top,
            shown: shown.to_string(),
            names: std::collections::BTreeMap::new(),
        }
    }

    fn name(&mut self, ino: u64, dir: u64, name: String) {
        self.names.entry(ino).or_default().insert((dir, name));
    }

    fn unname(&mut self, ino: u64, dir: u64, name: &str) {
        if let Some(names) = self.names.get_mut(&ino) {
            names.remove(&(dir, name.to_string()));
            if names.is_empty() {
                self.names.remove(&ino);
            }
        }
    }

    /// `name` in directory `dir`.
    fn join(&self, dir: u64, name: &str) -> String {
        match self.path(dir) {
            parent if parent.is_empty() => name.to_string(),
            parent => format!("{parent}/{name}"),
        }
    }

    /// An inode's path through its first name and its directories' names,
    /// or `#<inode>` for one the watch has yet to see named.
    fn path(&self, ino: u64) -> String {
        let mut parts = Vec::new();
        let mut current = ino;
        while current != self.top {
            match self.names.get(&current).and_then(|names| names.first()) {
                // A watch shows a directory's names below it, so parents
                // never loop; the bound guards against a malformed stream.
                Some((dir, name)) if parts.len() <= self.names.len() => {
                    parts.push(name.as_str());
                    current = *dir;
                }
                _ => return format!("#{ino}"),
            }
        }
        parts.push(&self.shown);
        parts
            .iter()
            .rev()
            .filter(|part| !part.is_empty())
            .copied()
            .collect::<Vec<_>>()
            .join("/")
    }
}

async fn fsck_sqlite(path: &std::path::Path) -> Result<(), String> {
    use inodedb_storage::Storage;
    let failed = |error: inodedb_storage::StorageError| format!("{}: {error}", path.display());
    let storage = inodedb_storage_sqlite::Sqlite::open_existing(path).map_err(failed)?;
    let pairs = inodedb_storage::dump(&storage).await.map_err(failed)?;
    let mut problems = inodedb_core::fsck::check(
        pairs
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    );
    let version = pairs
        .iter()
        .find(|(key, _)| *key == inodedb_core::keys::head())
        .and_then(|(_, value)| inodedb_core::writer::Head::decode(value).ok())
        .map_or(0, |head| head.version);
    // The lock keeps servers out, so the log matches the snapshot.
    let mut log = inodedb_core::fsck::LogCheck::default();
    let mut from = 0;
    loop {
        let entries = storage.read_log(from, 1_024).await.map_err(failed)?;
        for (version, bytes) in &entries {
            log.entry(*version, bytes);
            from = version + 1;
        }
        if entries.len() < 1_024 {
            break;
        }
    }
    problems.extend(log.finish(version));
    report(
        &mut std::io::stdout().lock(),
        pairs.len() as u64,
        version,
        &problems,
    )
}

async fn run(cli: Cli) -> Result<(), String> {
    if let Command::Fsck { sqlite: Some(path) } = &cli.command {
        return fsck_sqlite(path).await;
    }
    let key = cli.key.clone().ok_or("set INODEDB_KEY or pass --key")?;
    let db = Client::connect(cli.endpoint.clone(), key)
        .await
        .map_err(|error| format!("{}: {error}", cli.endpoint))?;
    let mut out = std::io::stdout().lock();
    match cli.command {
        Command::Ls {
            long: long_form,
            path,
        } => {
            let listing = db
                .list_with(
                    &path,
                    &List {
                        attrs: long_form,
                        ..List::default()
                    },
                )
                .await
                .map_err(fail(&path))?;
            for entry in &listing.entries {
                writeln!(out, "{}", entry_line(entry, long_form))
                    .map_err(|error| error.to_string())?;
            }
        }
        Command::Stat { paths } => {
            for (path, result) in paths
                .iter()
                .zip(db.stat_many(paths.iter()).await.map_err(fail("stat"))?)
            {
                let inode = result.map_err(fail(path))?;
                writeln!(out, "{}", long(path, &inode)).map_err(|error| error.to_string())?;
            }
        }
        Command::Cat { path } => {
            let value = db.read(&path).await.map_err(fail(&path))?;
            out.write_all(&value.value)
                .map_err(|error| error.to_string())?;
        }
        Command::Put {
            path,
            value,
            create,
            replace,
        } => {
            let value = match value {
                Some(value) => value.into_bytes(),
                None => {
                    let mut value = Vec::new();
                    tokio::io::stdin()
                        .read_to_end(&mut value)
                        .await
                        .map_err(|error| error.to_string())?;
                    value
                }
            };
            let mut op = Op::put(&path, value);
            if create {
                op = op.create();
            }
            if replace {
                op = op.replace();
            }
            applied(db.commit([op]).await.map_err(fail(&path))?, &[path])?;
        }
        Command::Mkdir { parents, paths } => {
            let ops = paths.iter().map(|path| {
                if parents {
                    Op::mkdir(path).parents()
                } else {
                    Op::mkdir(path)
                }
            });
            applied(db.commit(ops).await.map_err(fail("mkdir"))?, &paths)?;
        }
        Command::Mv {
            from,
            to,
            no_replace,
            exchange,
        } => {
            let mut op = Op::rename(&from, &to);
            if no_replace {
                op = op.no_replace();
            }
            if exchange {
                op = op.exchange();
            }
            applied(db.commit([op]).await.map_err(fail(&from))?, &[from])?;
        }
        Command::Rm {
            recursive,
            force,
            paths,
        } => {
            let ops = paths.iter().map(|path| {
                let op = if recursive {
                    Op::remove_all(path)
                } else {
                    Op::unlink(path)
                };
                if force { op.if_exists() } else { op }
            });
            applied(db.commit(ops).await.map_err(fail("rm"))?, &paths)?;
        }
        Command::Rmdir { paths } => {
            applied(
                db.commit(paths.iter().map(Op::rmdir))
                    .await
                    .map_err(fail("rmdir"))?,
                &paths,
            )?;
        }
        Command::Ln { from, to } => {
            applied(
                db.commit([Op::link(&from, &to)])
                    .await
                    .map_err(fail(&from))?,
                &[to],
            )?;
        }
        Command::Watch { path, from, count } => {
            let top = db.stat(&path).await.map_err(fail(&path))?;
            let mut names = Names::new(top.handle.ino, path.trim_end_matches('/'));
            let mut watch = db.watch(&path, from).await.map_err(fail(&path))?;
            let mut lines = Vec::new();
            let mut printed = 0;
            while count.is_none_or(|count| printed < count) {
                let Some(change) = watch.next().await.map_err(fail(&path))? else {
                    break;
                };
                for effect in &change.entries {
                    let name = String::from_utf8_lossy(&effect.name).into_owned();
                    let sign = match (&effect.before, &effect.after) {
                        (None, _) => '+',
                        (_, None) => '-',
                        _ => '~',
                    };
                    lines.push(format!("{sign} {}", names.join(effect.dir.ino, &name)));
                    if let Some(before) = &effect.before {
                        names.unname(before.handle.ino, effect.dir.ino, &name);
                    }
                    if let Some(after) = &effect.after {
                        names.name(after.handle.ino, effect.dir.ino, name);
                    }
                }
                for effect in &change.inodes {
                    if let (Some(before), Some(after)) = (&effect.before, &effect.after)
                        && after.kind == Kind::File
                        && after.life == inodedb::Life::Live
                        && before != after
                    {
                        lines.push(format!("~ {}", names.path(after.handle.ino)));
                    }
                }
                for line in lines.drain(..) {
                    if count.is_some_and(|count| printed == count) {
                        break;
                    }
                    writeln!(out, "{} {line}", change.version)
                        .and_then(|()| out.flush())
                        .map_err(|error| error.to_string())?;
                    printed += 1;
                }
            }
        }
        #[cfg(feature = "mount")]
        Command::Mount { dir } => mount::mount(db, dir).await?,
        #[cfg(not(feature = "mount"))]
        Command::Mount { .. } => {
            return Err("built without mount; rebuild with --features mount".into());
        }
        Command::Fsck { .. } => {
            let checked = db.check().await.map_err(fail("fsck"))?;
            report(&mut out, checked.keys, checked.version, &checked.problems)?;
        }
        Command::Key(KeyCommand::Mint { root, grant, ttl }) => {
            let grant = match grant {
                GrantArg::Read => Grant::Read,
                GrantArg::Write => Grant::Write,
                GrantArg::Admin => Grant::Admin,
            };
            let key = db
                .mint_key(&root, grant, ttl.unwrap_or_default())
                .await
                .map_err(fail("key mint"))?;
            writeln!(out, "{key}").map_err(|error| error.to_string())?;
        }
        Command::Key(KeyCommand::Revoke { key }) => {
            db.revoke_key(&key).await.map_err(fail("key revoke"))?;
        }
    }
    out.flush().map_err(|error| error.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("idb: {message}");
            ExitCode::FAILURE
        }
    }
}
