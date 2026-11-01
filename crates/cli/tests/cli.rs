//! `idb` against a server on the memory backend.

use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const KEY: &str = "0000000000000001.0123456789abcdef";

struct Shell {
    server: inodedb_server::Running,
    storage: inodedb_storage::Memory,
}

struct Ran {
    ok: bool,
    stdout: String,
    stderr: String,
}

impl Shell {
    async fn new() -> Shell {
        let storage = inodedb_storage::Memory::new();
        Shell {
            server: inodedb_server::start(storage.clone(), KEY, Default::default()).await,
            storage,
        }
    }

    async fn run_with(&self, args: &[&str], stdin: &[u8]) -> Ran {
        let mut child = Command::new(env!("CARGO_BIN_EXE_idb"))
            .args(args)
            .env("INODEDB_ENDPOINT", self.server.endpoint())
            .env("INODEDB_KEY", KEY)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("idb starts");
        let mut input = child.stdin.take().expect("a stdin pipe");
        input.write_all(stdin).await.expect("writes stdin");
        drop(input);
        let output = child.wait_with_output().await.expect("idb finishes");
        Ran {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    async fn run(&self, args: &[&str]) -> Ran {
        self.run_with(args, b"").await
    }

    /// Runs a command that must succeed and returns its output.
    async fn ok(&self, args: &[&str]) -> String {
        let ran = self.run(args).await;
        assert!(ran.ok, "idb {args:?} failed: {}", ran.stderr);
        ran.stdout
    }

    /// Runs a command that must fail and returns its message.
    async fn fails(&self, args: &[&str]) -> String {
        let ran = self.run(args).await;
        assert!(!ran.ok, "idb {args:?} succeeded: {}", ran.stdout);
        ran.stderr
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mkdir_put_cat_and_ls() {
    let sh = Shell::new().await;
    sh.ok(&["mkdir", "-p", "a/b"]).await;
    sh.ok(&["mkdir", "c", "d"]).await;
    assert!(sh.fails(&["mkdir", "c"]).await.contains("c: file exists"));
    sh.ok(&["put", "a/b/f", "hello"]).await;
    let ran = sh.run_with(&["put", "a/g"], b"from stdin").await;
    assert!(ran.ok, "{}", ran.stderr);
    assert_eq!(sh.ok(&["cat", "a/b/f"]).await, "hello");
    assert_eq!(sh.ok(&["cat", "a/g"]).await, "from stdin");
    assert_eq!(sh.ok(&["ls"]).await, "a/\nc/\nd/\n");
    assert_eq!(sh.ok(&["ls", "a"]).await, "b/\ng\n");
    let long = sh.ok(&["ls", "-l", "a"]).await;
    let lines: Vec<&str> = long.lines().collect();
    assert!(
        lines[0].starts_with('d') && lines[0].ends_with(" b"),
        "{long}"
    );
    assert!(
        lines[1].starts_with('-') && lines[1].contains(" 10 "),
        "{long}"
    );
    assert!(sh.fails(&["cat", "a"]).await.contains("is a directory"));
    assert!(
        sh.fails(&["ls", "none"])
            .await
            .contains("no such file or directory")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn put_modes() {
    let sh = Shell::new().await;
    sh.ok(&["put", "--create", "f", "1"]).await;
    assert!(
        sh.fails(&["put", "--create", "f", "2"])
            .await
            .contains("file exists")
    );
    assert!(
        sh.fails(&["put", "--replace", "g", "2"])
            .await
            .contains("no such file")
    );
    sh.ok(&["put", "--replace", "f", "3"]).await;
    assert_eq!(sh.ok(&["cat", "f"]).await, "3");
}

#[tokio::test(flavor = "multi_thread")]
async fn stat_prints_each_path() {
    let sh = Shell::new().await;
    sh.ok(&["mkdir", "d"]).await;
    sh.ok(&["put", "d/f", "abc"]).await;
    let out = sh.ok(&["stat", "d", "d/f"]).await;
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].starts_with('d') && lines[0].ends_with(" d"));
    assert!(lines[1].starts_with('-') && lines[1].contains("   3 ") && lines[1].ends_with(" d/f"));
    assert!(
        sh.fails(&["stat", "d", "none"])
            .await
            .contains("none: no such file")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mv_and_its_flags() {
    let sh = Shell::new().await;
    sh.ok(&["mkdir", "-p", "t/sub"]).await;
    sh.ok(&["put", "t/sub/f", "deep"]).await;
    sh.ok(&["mv", "t", "u"]).await;
    assert_eq!(sh.ok(&["cat", "u/sub/f"]).await, "deep");
    sh.ok(&["put", "x", "1"]).await;
    sh.ok(&["put", "y", "2"]).await;
    assert!(
        sh.fails(&["mv", "--no-replace", "x", "y"])
            .await
            .contains("file exists")
    );
    sh.ok(&["mv", "--exchange", "x", "y"]).await;
    assert_eq!(sh.ok(&["cat", "x"]).await, "2");
    sh.ok(&["mv", "x", "y"]).await;
    assert_eq!(sh.ok(&["cat", "y"]).await, "2");
    assert!(
        sh.fails(&["mv", "u", "u/sub/inside"])
            .await
            .contains("invalid argument")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rm_rmdir_and_ln() {
    let sh = Shell::new().await;
    sh.ok(&["mkdir", "-p", "r/s"]).await;
    sh.ok(&["put", "r/s/f", "x"]).await;
    sh.ok(&["ln", "r/s/f", "g"]).await;
    assert!(sh.ok(&["stat", "g"]).await.contains("   2 "), "two links");
    sh.ok(&["rm", "r/s/f"]).await;
    assert_eq!(sh.ok(&["cat", "g"]).await, "x");
    assert!(sh.fails(&["rm", "r/s/f"]).await.contains("no such file"));
    sh.ok(&["rm", "-f", "r/s/f"]).await;
    assert!(sh.fails(&["rm", "r"]).await.contains("is a directory"));
    assert!(sh.fails(&["rmdir", "r"]).await.contains("not empty"));
    sh.ok(&["rmdir", "r/s"]).await;
    sh.ok(&["mkdir", "-p", "r/s/t"]).await;
    sh.ok(&["rm", "-r", "r"]).await;
    assert_eq!(sh.ok(&["ls"]).await, "g\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_key_is_refused() {
    let sh = Shell::new().await;
    let output = Command::new(env!("CARGO_BIN_EXE_idb"))
        .args(["ls"])
        .env("INODEDB_ENDPOINT", sh.server.endpoint())
        .env("INODEDB_KEY", "0000000000000001.wrongwrongwrongwrong")
        .output()
        .await
        .expect("idb runs");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown or revoked key"));
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_mint_and_revoke() {
    let sh = Shell::new().await;
    sh.ok(&["mkdir", "-p", "t/a"]).await;
    sh.ok(&["put", "t/a/f", "x"]).await;
    let key = sh
        .ok(&[
            "key", "mint", "--root", "t", "--grant", "read", "--ttl", "30d",
        ])
        .await;
    let key = key.trim();
    let as_minted = |args: &'static [&'static str]| {
        let key = key.to_string();
        let endpoint = sh.server.endpoint();
        async move {
            Command::new(env!("CARGO_BIN_EXE_idb"))
                .args(args)
                .env("INODEDB_ENDPOINT", endpoint)
                .env("INODEDB_KEY", key)
                .output()
                .await
                .expect("idb runs")
        }
    };
    let listed = as_minted(&["ls"]).await;
    assert!(listed.status.success());
    assert_eq!(String::from_utf8_lossy(&listed.stdout), "a/\n");
    let refused = as_minted(&["mkdir", "b"]).await;
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("lacks the grant"));
    assert!(
        sh.fails(&["key", "mint", "--grant", "read", "--ttl", "3w"])
            .await
            .contains("unit")
    );
    sh.ok(&["key", "revoke", key]).await;
    let revoked = as_minted(&["ls"]).await;
    assert!(String::from_utf8_lossy(&revoked.stderr).contains("unknown or revoked key"));
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_prints_changes_as_paths() {
    let sh = Shell::new().await;
    sh.ok(&["mkdir", "t"]).await;
    let from = {
        // The next version is the mkdir's after this one.
        sh.ok(&["put", "t/seed", "x"]).await;
        let line = sh.ok(&["ls", "-l", "t"]).await;
        line.split_whitespace()
            .find_map(|word| word.strip_prefix('v'))
            .and_then(|version| version.parse::<u64>().ok())
            .expect("a version")
            + 1
    };
    sh.ok(&["mkdir", "t/d"]).await;
    sh.ok(&["put", "t/d/f", "1"]).await;
    sh.ok(&["put", "t/d/f", "2"]).await;
    sh.ok(&["rm", "t/seed"]).await;
    // Paths follow a renamed directory and a file's remaining link.
    sh.ok(&["mv", "t/d", "t/e"]).await;
    sh.ok(&["put", "t/e/f", "3"]).await;
    sh.ok(&["ln", "t/e/f", "t/g"]).await;
    sh.ok(&["rm", "t/e/f"]).await;
    sh.ok(&["put", "t/g", "4"]).await;
    let from = from.to_string();
    let printed = sh
        .ok(&["watch", "t", "--from", &from, "--count", "12"])
        .await;
    let lines: Vec<&str> = printed.lines().collect();
    let shapes: Vec<String> = lines
        .iter()
        .map(|line| line.split_once(' ').expect("a version").1.to_string())
        .collect();
    assert_eq!(
        shapes,
        // A link count's change shows as a change to the file.
        [
            "+ t/d", "+ t/d/f", "~ t/d/f", "- t/seed", "- t/d", "+ t/e", "~ t/e/f", "+ t/g",
            "~ t/g", "- t/e/f", "~ t/g", "~ t/g"
        ],
        "{printed}"
    );
    // Each line's version is its commit's.
    let versions: Vec<u64> = lines
        .iter()
        .map(|line| {
            line.split(' ')
                .next()
                .expect("a version")
                .parse()
                .expect("a number")
        })
        .collect();
    assert!(
        versions.windows(2).all(|pair| pair[0] <= pair[1]),
        "{printed}"
    );
    assert_eq!(
        versions[4], versions[5],
        "a rename's lines share its commit"
    );
}

/// Clears every inode record but the root's, which leaves every entry
/// naming nothing.
fn corruption(pairs: &inodedb_core::keys::Pairs) -> inodedb_core::keys::WriteSet {
    use inodedb_core::keys::{self, Parsed};
    let mut writes = keys::WriteSet::default();
    for (key, _) in pairs {
        if let Some(Parsed::Inode(ino)) = keys::parse(key)
            && ino != inodedb_core::types::ROOT
        {
            writes.clear(key.clone());
        }
    }
    writes
}

#[tokio::test(flavor = "multi_thread")]
async fn fsck_finds_a_planted_corruption() {
    let sh = Shell::new().await;
    sh.ok(&["mkdir", "-p", "a/b"]).await;
    sh.ok(&["put", "a/b/f", "v"]).await;
    assert!(sh.ok(&["fsck"]).await.contains(": sound"));
    let pairs = inodedb_storage::dump(&sh.storage).await.expect("dumps");
    let head = sh.storage.head_now();
    assert_eq!(
        sh.storage.commit_now(head.as_deref(), &corruption(&pairs)),
        inodedb_core::writer::CommitOutcome::Committed
    );
    let ran = sh.run(&["fsck"]).await;
    assert!(!ran.ok, "{}", ran.stdout);
    assert!(ran.stderr.contains("problems in"), "{}", ran.stderr);
    assert!(!ran.stdout.is_empty(), "each problem prints");
}

#[tokio::test(flavor = "multi_thread")]
async fn fsck_reads_a_sqlite_store_directly() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let path = dir.path().join("db.sqlite");
    let storage = inodedb_storage_sqlite::Sqlite::open(&path).expect("opens");
    let server = inodedb_server::start(storage, KEY, Default::default()).await;
    let db = inodedb::Client::connect(server.endpoint(), KEY)
        .await
        .expect("connects");
    db.commit([inodedb::Op::mkdir("a"), inodedb::Op::put("a/f", "v")])
        .await
        .expect("commits");
    drop(db);
    server.stop().await;
    let fsck = |path: &std::path::Path| {
        std::process::Command::new(env!("CARGO_BIN_EXE_idb"))
            .args(["fsck", "--sqlite"])
            .arg(path)
            .env_remove("INODEDB_KEY")
            .output()
            .expect("idb runs")
    };
    let sound = fsck(&path);
    assert!(
        sound.status.success(),
        "{}",
        String::from_utf8_lossy(&sound.stderr)
    );
    assert!(String::from_utf8_lossy(&sound.stdout).contains(": sound"));
    let storage = inodedb_storage_sqlite::Sqlite::open(&path).expect("reopens");
    let pairs = inodedb_storage::dump(&storage).await.expect("dumps");
    let mut txn = inodedb_storage::Storage::begin(&storage)
        .await
        .expect("begins");
    inodedb_storage::apply(&mut txn, &corruption(&pairs));
    inodedb_storage::Txn::commit(txn).await.expect("commits");
    drop(storage);
    let broken = fsck(&path);
    assert!(
        !broken.status.success(),
        "{}",
        String::from_utf8_lossy(&broken.stdout)
    );
    // A path that holds no store is refused, and left without one.
    let missing = dir.path().join("typo.sqlite");
    let refused = fsck(&missing);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("typo.sqlite"));
    assert!(!missing.exists() && !missing.with_extension("lock").exists());
}
