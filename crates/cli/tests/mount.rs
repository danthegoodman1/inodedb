//! `idb mount` through the kernel, on Linux with FUSE.
#![cfg(all(feature = "mount", target_os = "linux"))]

use std::path::Path;
use std::time::{Duration, Instant};

const KEY: &str = "0000000000000001.0123456789abcdef";

fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("lists")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mount_lists_and_reads_the_tree() {
    let server =
        inodedb_server::start(inodedb_storage::Memory::new(), KEY, Default::default()).await;
    let db = inodedb::Client::connect(server.endpoint(), KEY)
        .await
        .expect("connects");
    db.commit([
        inodedb::Op::mkdir("a"),
        inodedb::Op::put("a/f", "hello"),
        inodedb::Op::mkdir("a/b"),
        inodedb::Op::put("a/b/g", "world"),
    ])
    .await
    .expect("commits");
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let mnt = scratch.path().join("mnt");
    std::fs::create_dir(&mnt).expect("makes the mount point");
    let mut idb = std::process::Command::new(env!("CARGO_BIN_EXE_idb"))
        .arg("mount")
        .arg(&mnt)
        .env("INODEDB_ENDPOINT", server.endpoint())
        .env("INODEDB_KEY", KEY)
        .spawn()
        .expect("idb starts");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !mnt.join("a").exists() {
        assert!(Instant::now() < deadline, "the tree never appeared");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(names(&mnt), ["a"]);
    assert_eq!(names(&mnt.join("a")), ["b", "f"]);
    assert_eq!(
        std::fs::read_to_string(mnt.join("a/f")).expect("reads"),
        "hello"
    );
    assert_eq!(
        std::fs::read_to_string(mnt.join("a/b/g")).expect("reads"),
        "world"
    );
    assert_eq!(std::fs::metadata(mnt.join("a/f")).expect("stats").len(), 5);
    assert!(std::fs::metadata(mnt.join("a/b")).expect("stats").is_dir());
    assert!(
        std::fs::write(mnt.join("a/new"), "x").is_err(),
        "the view is read-only"
    );
    // A commit shows once the kernel's second of caching passes.
    db.commit([inodedb::Op::put("a/h", "later")])
        .await
        .expect("commits");
    std::thread::sleep(Duration::from_millis(1_100));
    assert_eq!(names(&mnt.join("a")), ["b", "f", "h"]);
    // A directory the kernel knows goes by its inode: renamed, and with a
    // new one in its old place, it still lists what it holds.
    let dir = std::fs::File::open(mnt.join("a/b")).expect("opens the directory");
    let held = Path::new("/proc/self/fd").join(std::os::fd::AsRawFd::as_raw_fd(&dir).to_string());
    let before = names(&held);
    let open = std::fs::File::open(held.join("g")).expect("opens");
    db.commit([
        inodedb::Op::rename("a/b", "c"),
        inodedb::Op::mkdir("a/b"),
        inodedb::Op::put("a/b/other", "x"),
    ])
    .await
    .expect("commits");
    std::thread::sleep(Duration::from_millis(1_100));
    assert_eq!(before, ["g"]);
    assert_eq!(names(&held), ["g"], "the open directory, now at c");
    drop(dir);
    assert_eq!(names(&mnt.join("c")), ["g"]);
    assert_eq!(names(&mnt.join("a/b")), ["other"]);
    assert_eq!(
        std::io::read_to_string(open).expect("reads what it opened"),
        "world"
    );
    // A large directory lists each name once.
    db.commit(
        std::iter::once(inodedb::Op::mkdir("big"))
            .chain((0..3_000).map(|index| inodedb::Op::put(format!("big/{index:05}"), ""))),
    )
    .await
    .expect("commits");
    let listed = names(&mnt.join("big"));
    assert_eq!(listed.len(), 3_000);
    assert_eq!(listed.first().map(String::as_str), Some("00000"));
    let unmounted = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mnt)
        .status()
        .expect("fusermount3 runs");
    assert!(unmounted.success());
    assert!(idb.wait().expect("idb exits").success());
}
