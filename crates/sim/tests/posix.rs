//! The POSIX subset against a real filesystem: random mkdir, put, link,
//! unlink, rmdir and rename sequences run on a temporary directory and on
//! InodeDB, and each op's error and the resulting trees must match. InodeDB
//! follows Linux, whose `rename(2)` checks errors in a different order from
//! macOS's, so the test runs on Linux.

#![cfg(target_os = "linux")]

use inodedb_core::Bytes;
use inodedb_core::ops::{Op, OpSpec, Outcome, PutMode, RenameFlag, Skip};
use inodedb_core::query::{Query, QueryResult};
use inodedb_core::types::{Error, Kind, Path};
use inodedb_sim::harness::Harness;
use inodedb_sim::prng::Prng;
use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

const NAMES: [&str; 3] = ["a", "b", "c"];

fn random_path(prng: &mut Prng) -> Vec<&'static str> {
    (0..prng.range(1..=3)).map(|_| *prng.pick(&NAMES)).collect()
}

fn to_path(names: &[&str]) -> Path {
    names.join("/").parse().expect("valid")
}

fn kind_error(error: &std::io::Error, op: &Op) -> Error {
    match error.kind() {
        ErrorKind::NotFound => Error::NotFound,
        ErrorKind::AlreadyExists => Error::Exists,
        ErrorKind::NotADirectory => Error::NotDir,
        ErrorKind::IsADirectory => Error::IsDir,
        ErrorKind::DirectoryNotEmpty => Error::NotEmpty,
        ErrorKind::InvalidInput => Error::Invalid,
        // Linking a directory.
        ErrorKind::PermissionDenied => Error::IsDir,
        _ => panic!("unmapped error {error:?} for {op:?}"),
    }
}

fn real(root: &std::path::Path, names: &[&str]) -> PathBuf {
    let mut path = root.to_path_buf();
    for name in names {
        path.push(name);
    }
    path
}

/// Runs an op on the real directory; None when it isn't one POSIX has.
fn run_real(root: &std::path::Path, op: &Op, names: &[Vec<&str>]) -> Option<Result<(), Error>> {
    let result = match op {
        Op::Mkdir { .. } => fs::create_dir(real(root, &names[0])),
        Op::Put { value, mode, .. } => {
            let path = real(root, &names[0]);
            let mut options = fs::OpenOptions::new();
            options.write(true).truncate(true);
            match mode {
                PutMode::Create => options.create_new(true),
                PutMode::Replace => options.create(false),
                PutMode::Upsert => options.create(true),
            };
            options
                .open(&path)
                .and_then(|mut file| file.write_all(value))
        }
        Op::Link { .. } => fs::hard_link(real(root, &names[0]), real(root, &names[1])),
        Op::Unlink { .. } => fs::remove_file(real(root, &names[0])),
        Op::Rmdir { .. } => fs::remove_dir(real(root, &names[0])),
        Op::Rename { flag, .. } => {
            let (from, to) = (real(root, &names[0]), real(root, &names[1]));
            match flag {
                RenameFlag::Replace => fs::rename(from, to),
                RenameFlag::NoReplace => renameat2(&from, &to, libc::RENAME_NOREPLACE),
                RenameFlag::Exchange => renameat2(&from, &to, libc::RENAME_EXCHANGE),
            }
        }
        _ => return None,
    };
    Some(result.map_err(|error| kind_error(&error, op)))
}

fn renameat2(from: &std::path::Path, to: &std::path::Path, flags: u32) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes()).expect("no NUL");
    let to = CString::new(to.as_os_str().as_bytes()).expect("no NUL");
    // SAFETY: both paths are NUL-terminated and outlive the call.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            flags,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Every path under the real root: its kind, contents and link count.
fn real_tree(root: &std::path::Path) -> BTreeMap<String, (Kind, Vec<u8>, u64)> {
    let mut tree = BTreeMap::new();
    let mut stack = vec![(root.to_path_buf(), String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("readable") {
            let entry = entry.expect("readable entry");
            let name = format!("{prefix}/{}", entry.file_name().to_string_lossy());
            let meta = fs::symlink_metadata(entry.path()).expect("stat");
            if meta.is_dir() {
                tree.insert(name.clone(), (Kind::Dir, Vec::new(), 0));
                stack.push((entry.path(), name));
            } else {
                tree.insert(
                    name,
                    (
                        Kind::File,
                        fs::read(entry.path()).expect("read"),
                        meta.nlink(),
                    ),
                );
            }
        }
    }
    tree
}

fn inodedb_tree(harness: &mut Harness) -> BTreeMap<String, (Kind, Vec<u8>, u64)> {
    let mut tree = BTreeMap::new();
    let mut stack = vec![String::new()];
    while let Some(prefix) = stack.pop() {
        let path: Path = if prefix.is_empty() {
            Path::root()
        } else {
            prefix.parse().expect("valid")
        };
        let (_, listing) = harness.query(Query::List {
            target: Target::Path(path),
            after: None,
            prefix: None,
            limit: usize::MAX,
            budget: usize::MAX,
            attrs: true,
            values: true,
        });
        let QueryResult::List { entries, .. } = listing else {
            panic!("{listing:?}");
        };
        for listed in entries {
            let name = format!("{prefix}/{}", String::from_utf8_lossy(&listed.name));
            let inode = listed.inode.expect("attrs");
            match inode.kind {
                Kind::Dir => {
                    tree.insert(name.clone(), (Kind::Dir, Vec::new(), 0));
                    stack.push(name);
                }
                Kind::File => {
                    let value = listed.value.expect("values").to_vec();
                    tree.insert(name, (Kind::File, value, u64::from(inode.nlink)));
                }
            }
        }
    }
    tree
}

#[test]
fn inodedb_matches_a_real_filesystem() {
    for seed in 0..400 {
        let root =
            std::env::temp_dir().join(format!("inodedb-posix-{}-{seed}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("a scratch directory");
        let mut harness = Harness::new();
        let mut prng = Prng::new(seed);
        for step in 0..100 {
            let mut names = vec![random_path(&mut prng), random_path(&mut prng)];
            // Often the second path holds the first, so renames and links
            // reach the source's own ancestors.
            if names[0].len() > 1 && prng.percent(25) {
                let len = prng.range(1..=names[0].len() as u64 - 1) as usize;
                names[1] = names[0][..len].to_vec();
            }
            let op = match prng.below(6) {
                0 => Op::Mkdir {
                    path: to_path(&names[0]),
                    parents: false,
                    if_absent: false,
                },
                1 => Op::Put {
                    path: to_path(&names[0]),
                    value: Bytes::from(format!("{seed}:{step}")),
                    mode: *prng.pick(&[PutMode::Create, PutMode::Replace, PutMode::Upsert]),
                    mtime: None,
                },
                2 => Op::Link {
                    from: to_path(&names[0]),
                    to: to_path(&names[1]),
                },
                3 => Op::Unlink {
                    path: to_path(&names[0]),
                    if_exists: false,
                },
                4 => Op::Rmdir {
                    path: to_path(&names[0]),
                    if_exists: false,
                },
                _ => Op::Rename {
                    from: to_path(&names[0]),
                    to: to_path(&names[1]),
                    flag: *prng.pick(&[
                        RenameFlag::Replace,
                        RenameFlag::NoReplace,
                        RenameFlag::Exchange,
                    ]),
                },
            };
            let Some(expected) = run_real(&root, &op, &names) else {
                continue;
            };
            let result = harness.ops([OpSpec::new(op.clone())]);
            let actual = match result.outcomes[0] {
                Outcome::Applied { .. } => Ok(()),
                Outcome::Skipped(Skip::Error(error)) => Err(error),
                other => panic!("{other:?}"),
            };
            assert_eq!(actual, expected, "seed {seed} step {step}: {op:?}");
            assert_eq!(
                inodedb_tree(&mut harness),
                real_tree(&root),
                "seed {seed} step {step}: {op:?}"
            );
        }
        assert!(harness.fsck().is_empty(), "{:?}", harness.fsck());
        fs::remove_dir_all(&root).expect("cleanup");
    }
}
