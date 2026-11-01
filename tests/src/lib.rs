//! The API's contract as tests against a running server, generic over the
//! storage backend.
//!
//! A backend's test file calls `api_conformance!` with an expression that
//! makes a `Backend`; each check becomes its own test. A backend whose
//! stores admit several servers also calls `standby_conformance!`.

use bytes::Bytes;
use inodedb::{Client, Cond, Error, FsError, Kind, List, Op, Outcome, Rejection, Skip, Target};
use inodedb_proto as pb;
use inodedb_server::service::Faults;
use inodedb_server::{Running, Settings};
use inodedb_storage_conformance::Backend;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tonic::Code;

pub use inodedb_storage_conformance::backends;

pub const KEY: &str = "0000000000000001.0123456789abcdef";

#[macro_export]
macro_rules! api_conformance {
    ($make:expr) => {
        $crate::api_conformance!(@tests $make;
            puts_and_reads, every_op, every_condition, conditions_skip_and_cascade, all_or_nothing_aborts,
            when_fences_the_batch, renaming_moves_a_subtree, listings_page_by_count,
            listings_page_by_bytes, listings_filter, many_reads_chunk, refuses_bad_requests,
            retries_answer_once, versions_advance, concurrent_commits_all_apply,
            durable_across_restart, hard_links_count, values_hold_to_the_limit,
            responses_fit_default_limits, lost_answers_apply_once, cut_listings_resume,
            floors_past_the_store_fail_fast, watch_streams_a_subtree, watch_replays_from_a_version,
            watch_reports_compaction, caches_follow_the_feed, keys_bound_their_reach,
            watch_ends_with_its_key, listings_follow_their_directory, shutdown_ends_streams,
            watch_stays_in_its_keys_tree, caches_stop_with_their_key, leases_hold_freed_records,
            leases_hold_the_log, caches_hold_a_lease, snapshots_hold_their_version,
            snapshots_hold_what_they_read, caches_start_under_steady_writes,
            claims_read_values_their_file_outlived, checks_take_a_root_admin_key);
    };
    (@tests $make:expr; $($check:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $check() {
                let mut backend = $make;
                $crate::$check(&mut backend).await;
            }
        )*
    };
}

#[macro_export]
macro_rules! standby_conformance {
    ($make:expr) => {
        $crate::api_conformance!(@tests $make;
            standbys_serve_reads_and_send_writes_on, takeovers_keep_every_acknowledged_batch);
    };
}

/// A server over a fresh store, and a client on it.
pub struct Fixture<S> {
    pub store: S,
    pub server: Running,
    pub db: Client,
}

pub async fn start<S: inodedb_storage::Storage + Clone>(store: S) -> Fixture<S> {
    start_with_faults(store, Arc::default()).await
}

pub async fn start_with_faults<S: inodedb_storage::Storage + Clone>(
    store: S,
    faults: Arc<Faults>,
) -> Fixture<S> {
    start_with(
        store,
        Settings {
            faults,
            ..Settings::default()
        },
    )
    .await
}

pub async fn start_with<S: inodedb_storage::Storage + Clone>(
    store: S,
    settings: Settings,
) -> Fixture<S> {
    let server = inodedb_server::start_with(store.clone(), KEY, settings).await;
    let db = Client::connect(server.endpoint(), KEY)
        .await
        .expect("connects");
    Fixture { store, server, db }
}

pub async fn fresh<B: Backend>(backend: &mut B) -> Fixture<B::S> {
    start(backend.fresh()).await
}

fn fs(result: Result<impl std::fmt::Debug, Error>) -> FsError {
    match result {
        Err(Error::Fs(error)) => error,
        other => panic!("expected a filesystem error, got {other:?}"),
    }
}

fn code(result: Result<impl std::fmt::Debug, Error>) -> Code {
    match result {
        Err(Error::Status(status)) => status.code(),
        other => panic!("expected a status, got {other:?}"),
    }
}

fn names(listing: &inodedb::Listing) -> Vec<String> {
    listing
        .entries
        .iter()
        .map(|entry| entry.name_lossy())
        .collect()
}

async fn puts(db: &Client, dir: &str, count: usize, value: impl Fn(usize) -> Bytes) {
    let mut done = 0;
    while done < count {
        let mut batch = db.batch();
        for index in done..(done + 500).min(count) {
            batch.push(Op::put(format!("{dir}/f{index:05}"), value(index)));
        }
        done += batch.len();
        assert!(batch.commit().await.expect("commits").all_applied());
    }
}

pub async fn puts_and_reads<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    let commit = db
        .commit([Op::mkdir("a/b").parents(), Op::put("a/b/f", "hello")])
        .await
        .expect("commits");
    assert!(commit.all_applied() && commit.committed, "{commit:?}");
    let file = db.stat("a/b/f").await.expect("stats");
    assert_eq!((file.kind, file.size, file.nlink), (Kind::File, 5, 1));
    assert_eq!(file.version, commit.version);
    let value = db.read("a/b/f").await.expect("reads");
    assert_eq!(&value.value[..], b"hello");
    assert_eq!(value.inode, file);
    let by_handle = db
        .read(Target::Handle(file.handle.clone()))
        .await
        .expect("reads by handle");
    assert_eq!(&by_handle.value[..], b"hello");
    assert!(db.stat("a").await.expect("stats").is_dir());
    assert_eq!(fs(db.read("a").await), FsError::IsDir);
    assert_eq!(fs(db.stat("a/missing").await), FsError::NotFound);
    assert_eq!(fs(db.stat("a/b/f/under").await), FsError::NotDir);
    db.commit([Op::set_mtime("a/b/f", 42)])
        .await
        .expect("commits");
    assert_eq!(db.stat("a/b/f").await.expect("stats").mtime, 42);
}

fn keyed<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {KEY}").parse().expect("a header"),
    );
    request
}

fn skipped(error: FsError) -> Outcome {
    Outcome::Skipped(Skip::Error(error))
}

/// Each op's effect and its errors, one batch per op.
pub async fn every_op<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    let one = |op: Op| {
        let db = db.clone();
        async move { db.commit([op]).await.expect("commits").outcomes.remove(0) }
    };
    let value = |path: &'static str| {
        let db = db.clone();
        async move { db.read(path).await.map(|value| value.value) }
    };
    // Mkdir.
    assert!(one(Op::mkdir("p")).await.applied());
    assert_eq!(one(Op::mkdir("p")).await, skipped(FsError::Exists));
    assert!(one(Op::mkdir("p").if_absent()).await.applied());
    assert_eq!(one(Op::mkdir("q/r")).await, skipped(FsError::NotFound));
    assert!(one(Op::mkdir("q/r").parents()).await.applied());
    // Put and its modes.
    assert!(one(Op::put("p/f", "1")).await.applied());
    assert!(one(Op::put("p/f", "2")).await.applied());
    assert_eq!(&value("p/f").await.expect("reads")[..], b"2");
    assert_eq!(
        one(Op::put("p/f", "3").create()).await,
        skipped(FsError::Exists)
    );
    assert_eq!(
        one(Op::put("p/new", "3").replace()).await,
        skipped(FsError::NotFound)
    );
    assert!(one(Op::put("p/f", "4").replace()).await.applied());
    assert!(one(Op::put("p/g", "5").create().mtime(7)).await.applied());
    assert_eq!(db.stat("p/g").await.expect("stats").mtime, 7);
    assert_eq!(one(Op::put("p", "x")).await, skipped(FsError::IsDir));
    assert_eq!(one(Op::put("p/f/x", "x")).await, skipped(FsError::NotDir));
    // SetAttr.
    assert!(one(Op::set_mode("p/f", 0o640)).await.applied());
    assert_eq!(db.stat("p/f").await.expect("stats").mode, 0o640);
    assert!(one(Op::set_mtime("p", 99)).await.applied());
    assert_eq!(db.stat("p").await.expect("stats").mtime, 99);
    assert_eq!(
        one(Op::set_mtime("none", 1)).await,
        skipped(FsError::NotFound)
    );
    // Link and Unlink.
    assert!(one(Op::link("p/f", "p/h")).await.applied());
    assert_eq!(one(Op::link("p/f", "p/g")).await, skipped(FsError::Exists));
    assert_eq!(
        one(Op::link("none", "p/i")).await,
        skipped(FsError::NotFound)
    );
    assert!(one(Op::unlink("p/h")).await.applied());
    assert_eq!(one(Op::unlink("p/h")).await, skipped(FsError::NotFound));
    assert!(one(Op::unlink("p/h").if_exists()).await.applied());
    assert_eq!(one(Op::unlink("p")).await, skipped(FsError::IsDir));
    // Rmdir.
    assert_eq!(one(Op::rmdir("p")).await, skipped(FsError::NotEmpty));
    assert_eq!(one(Op::rmdir("p/f")).await, skipped(FsError::NotDir));
    assert!(one(Op::rmdir("q/r")).await.applied());
    assert!(one(Op::rmdir("q/r").if_exists()).await.applied());
    assert_eq!(one(Op::rmdir("q/r")).await, skipped(FsError::NotFound));
    assert_eq!(one(Op::rmdir("")).await, skipped(FsError::Invalid));
    // Rename and its flags.
    assert!(one(Op::put("p/x", "x")).await.applied());
    assert!(one(Op::rename("p/x", "p/f")).await.applied());
    assert_eq!(&value("p/f").await.expect("reads")[..], b"x");
    assert_eq!(
        one(Op::rename("p/f", "p/g").no_replace()).await,
        skipped(FsError::Exists)
    );
    assert!(one(Op::rename("p/f", "p/y").no_replace()).await.applied());
    assert!(one(Op::rename("p/y", "p/g").exchange()).await.applied());
    assert_eq!(&value("p/y").await.expect("reads")[..], b"5");
    assert_eq!(&value("p/g").await.expect("reads")[..], b"x");
    assert_eq!(
        one(Op::rename("p/y", "p/none").exchange()).await,
        skipped(FsError::NotFound)
    );
    assert!(one(Op::mkdir("p/d")).await.applied());
    assert_eq!(one(Op::rename("p/g", "p/d")).await, skipped(FsError::IsDir));
    assert_eq!(
        one(Op::rename("p/d", "p/g")).await,
        skipped(FsError::NotDir)
    );
    assert_eq!(
        one(Op::rename("p", "p/d/p")).await,
        skipped(FsError::Invalid)
    );
    assert!(
        one(Op::rename("p/d", "q")).await.applied(),
        "a directory replaces an empty one"
    );
    // RemoveAll.
    assert!(one(Op::remove_all("p")).await.applied());
    assert_eq!(fs(db.stat("p").await), FsError::NotFound);
    assert_eq!(one(Op::remove_all("p")).await, skipped(FsError::NotFound));
    assert!(one(Op::remove_all("p").if_exists()).await.applied());
    assert!(
        db.list("")
            .await
            .expect("lists")
            .entries
            .iter()
            .all(|entry| entry.name_lossy() == "q")
    );
}

/// Each predicate, alone and joined.
pub async fn every_condition<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([
        Op::mkdir("dir"),
        Op::mkdir("empty"),
        Op::put("dir/file", "v"),
    ])
    .await
    .expect("commits");
    let version = db.stat("dir/file").await.expect("stats").version;
    let holds = |cond: Cond| {
        let db = db.clone();
        async move {
            let commit = db
                .commit([Op::set_mtime("dir", 1).if_(cond)])
                .await
                .expect("commits");
            match &commit.outcomes[0] {
                Outcome::Applied(_) => true,
                Outcome::Skipped(Skip::Cond(0)) => false,
                other => panic!("unexpected {other:?}"),
            }
        }
    };
    assert!(holds(Cond::exists("dir/file")).await);
    assert!(holds(Cond::exists("dir")).await);
    assert!(!holds(Cond::exists("none")).await);
    assert!(holds(Cond::absent("none")).await);
    assert!(!holds(Cond::absent("dir")).await);
    assert!(holds(Cond::is_file("dir/file")).await);
    assert!(!holds(Cond::is_file("dir")).await);
    assert!(!holds(Cond::is_file("none")).await);
    assert!(holds(Cond::is_dir("dir")).await);
    assert!(!holds(Cond::is_dir("dir/file")).await);
    assert!(holds(Cond::empty("empty")).await);
    assert!(!holds(Cond::empty("dir")).await);
    assert!(!holds(Cond::empty("dir/file")).await);
    assert!(holds(Cond::version_is("dir/file", version)).await);
    assert!(!holds(Cond::version_is("dir/file", version + 1)).await);
    assert!(!holds(Cond::version_is("none", version)).await);
    assert!(
        !holds(Cond::exists("dir/file/under")).await,
        "a path through a file names nothing"
    );
    // Joined conditions, and the index of the first that fails.
    let commit = db
        .commit([Op::mkdir("j")
            .if_(Cond::is_dir("dir"))
            .if_(Cond::absent("dir/file"))
            .if_(Cond::exists("none"))])
        .await
        .expect("commits");
    assert_eq!(commit.outcomes[0], Outcome::Skipped(Skip::Cond(1)));
    // A later op sees what earlier ops left.
    let mut batch = db.batch();
    batch.push(Op::unlink("dir/file"));
    batch.push(Op::rmdir("dir").if_(Cond::empty("dir")));
    batch.push(Op::mkdir("dir").if_(Cond::absent("dir")));
    batch.push(Op::put("dir/after", "a").if_(Cond::empty("dir")));
    assert!(batch.commit().await.expect("commits").all_applied());
}

/// A plain gRPC client, with gRPC's default 4 MiB message limit, reads
/// the largest answers.
pub async fn responses_fit_default_limits<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    let limit = inodedb_core::types::MAX_VALUE;
    db.commit([Op::mkdir("full")]).await.expect("commits");
    for half in 0..2 {
        let mut batch = db.batch();
        for index in half * 50..half * 50 + 50 {
            batch.push(Op::put(
                format!("full/f{index:03}"),
                vec![index as u8; limit],
            ));
        }
        assert!(batch.commit().await.expect("commits").all_applied());
    }
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let targets = (0..inodedb_server::service::MAX_READ_TARGETS)
        .map(|index| pb::ReadTarget {
            target: Some(Target::from(format!("full/f{index:03}")).into_pb()),
            value_version: None,
        })
        .collect();
    let read = rpc
        .read(keyed(pb::ReadRequest { targets, at: None }))
        .await
        .expect("the largest read fits")
        .into_inner();
    assert!(
        read.results
            .iter()
            .all(|result| matches!(result.kind, Some(pb::read_result::Kind::Value(_))))
    );
    let mut pages = rpc
        .list(keyed(pb::ListRequest {
            path: Bytes::from_static(b"full"),
            attrs: true,
            values: true,
            ..pb::ListRequest::default()
        }))
        .await
        .expect("lists")
        .into_inner();
    let mut entries = 0;
    while let Some(page) = pages.message().await.expect("each page fits") {
        entries += page.entries.len();
        if !page.more {
            break;
        }
    }
    assert_eq!(entries, 100);
}

pub async fn conditions_skip_and_cascade<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    let mut batch = db.batch();
    let made = batch.push(Op::mkdir("x"));
    batch.push(Op::put("x/f", "1").if_applied(made));
    let again = batch.push(Op::mkdir("x"));
    batch.push(Op::put("x/g", "2").if_applied(again));
    batch.push(Op::put("x/h", "3").if_(Cond::absent("x/f")));
    batch.push(
        Op::put("x/i", "4")
            .if_(Cond::is_file("x/f"))
            .if_(Cond::is_dir("x")),
    );
    let commit = batch.commit().await.expect("commits");
    assert!(commit.committed);
    assert!(commit.applied(made));
    assert!(commit.outcomes[1].applied());
    assert_eq!(
        commit.outcomes[2],
        Outcome::Skipped(Skip::Error(FsError::Exists))
    );
    assert_eq!(commit.outcomes[3], Outcome::Skipped(Skip::Cond(0)));
    assert_eq!(commit.outcomes[4], Outcome::Skipped(Skip::Cond(0)));
    assert!(commit.outcomes[5].applied());
    assert_eq!(fs(db.stat("x/g").await), FsError::NotFound);
    assert_eq!(fs(db.stat("x/h").await), FsError::NotFound);
    assert_eq!(&db.read("x/i").await.expect("reads").value[..], b"4");
}

pub async fn all_or_nothing_aborts<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    let before = db.stat("").await.expect("stats root");
    let mut batch = db.batch().all_or_nothing();
    batch.push(Op::mkdir("y"));
    batch.push(Op::put("y/f", "1"));
    batch.push(Op::rmdir("missing"));
    let commit = batch.commit().await.expect("commits");
    assert!(!commit.committed, "{commit:?}");
    assert!(commit.outcomes.iter().all(|outcome| !outcome.applied()));
    assert_eq!(
        commit.outcomes[2],
        Outcome::Skipped(Skip::Error(FsError::NotFound))
    );
    assert_eq!(fs(db.stat("y").await), FsError::NotFound);
    assert_eq!(
        db.stat("").await.expect("stats root"),
        before,
        "nothing changed"
    );
}

pub async fn when_fences_the_batch<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::put("owner", "me")]).await.expect("commits");
    let owner = db.stat("owner").await.expect("stats").version;
    let mut stale = db.batch().when(Cond::version_is("owner", owner + 1000));
    stale.push(Op::mkdir("z"));
    stale.push(Op::mkdir("w"));
    let commit = stale.commit().await.expect("commits");
    assert!(!commit.committed);
    assert_eq!(commit.outcomes, vec![Outcome::Skipped(Skip::When(0)); 2]);
    let mut current = db.batch().when(Cond::version_is("owner", owner));
    current.push(Op::mkdir("z"));
    assert!(current.commit().await.expect("commits").all_applied());
}

pub async fn renaming_moves_a_subtree<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("d/1/2/3").parents(), Op::put("d/1/2/3/f", "deep")])
        .await
        .expect("commits");
    puts(&db, "d/1", 600, |index| Bytes::from(index.to_string())).await;
    let dir = db.stat("d").await.expect("stats");
    let commit = db.commit([Op::rename("d", "e")]).await.expect("commits");
    assert!(commit.all_applied());
    assert_eq!(fs(db.stat("d").await), FsError::NotFound);
    assert_eq!(
        db.stat("e").await.expect("stats").handle,
        dir.handle,
        "the same inode"
    );
    assert_eq!(
        &db.read("e/1/2/3/f").await.expect("reads").value[..],
        b"deep"
    );
    assert_eq!(db.list("e/1").await.expect("lists").entries.len(), 601);
    assert_eq!(
        &db.read("e/1/f00599").await.expect("reads").value[..],
        b"599"
    );
    let commit = db
        .commit([Op::rename("e/1/2", "e/1/2/3/inside")])
        .await
        .expect("commits");
    assert_eq!(
        commit.outcomes[0],
        Outcome::Skipped(Skip::Error(FsError::Invalid)),
        "no cycles"
    );
}

pub async fn listings_page_by_count<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("big")]).await.expect("commits");
    puts(&db, "big", 2500, |index| Bytes::from(format!("v{index}"))).await;
    let listing = db.list("big").await.expect("lists");
    let expected: Vec<String> = (0..2500).map(|index| format!("f{index:05}")).collect();
    assert_eq!(names(&listing), expected, "every entry once, in name order");
    assert!(
        listing
            .entries
            .iter()
            .all(|entry| entry.kind == Kind::File && entry.value.is_none())
    );
    let with = db.list_with_values("big").await.expect("lists");
    for (index, entry) in with.entries.iter().enumerate() {
        assert_eq!(entry.value.as_deref(), Some(format!("v{index}").as_bytes()));
        assert_eq!(
            entry.inode.as_ref().map(|inode| inode.size),
            Some(format!("v{index}").len() as u64)
        );
    }
}

pub async fn listings_page_by_bytes<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("blobs")]).await.expect("commits");
    let mut batch = db.batch();
    for index in 0..90u8 {
        batch.push(Op::put(
            format!("blobs/b{index:03}"),
            vec![index; 60 * 1024],
        ));
    }
    assert!(batch.commit().await.expect("commits").all_applied());
    let listing = db
        .list_with_values("blobs")
        .await
        .expect("lists past one message");
    assert_eq!(listing.entries.len(), 90);
    for (index, entry) in listing.entries.iter().enumerate() {
        let value = entry.value.as_ref().expect("a value");
        assert!(value.len() == 60 * 1024 && value.iter().all(|&byte| byte == index as u8));
    }
}

pub async fn listings_filter<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("l")]).await.expect("commits");
    db.commit(
        [
            "l/apple",
            "l/apricot",
            "l/banana",
            "l/blueberry",
            "l/cherry",
        ]
        .map(|path| Op::put(path, "")),
    )
    .await
    .expect("commits");
    db.commit([Op::mkdir("l/avocado")]).await.expect("commits");
    let prefix = |prefix: &str| List {
        prefix: Some(Bytes::copy_from_slice(prefix.as_bytes())),
        ..List::default()
    };
    let listing = db.list_with("l", &prefix("a")).await.expect("lists");
    assert_eq!(names(&listing), ["apple", "apricot", "avocado"]);
    assert_eq!(listing.entries[2].kind, Kind::Dir);
    let after = List {
        after: Some(Bytes::from_static(b"apricot")),
        limit: 2,
        ..List::default()
    };
    assert_eq!(
        names(&db.list_with("l", &after).await.expect("lists")),
        ["avocado", "banana"]
    );
    assert!(
        db.list_with("l", &prefix("z"))
            .await
            .expect("lists")
            .entries
            .is_empty()
    );
    assert_eq!(fs(db.list("l/apple").await), FsError::NotDir);
    assert_eq!(fs(db.list("missing").await), FsError::NotFound);
}

pub async fn many_reads_chunk<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("m")]).await.expect("commits");
    puts(&db, "m", 150, |index| Bytes::from(vec![b'x'; index])).await;
    let mut targets: Vec<(Target, Option<u64>)> = (0..150)
        .map(|index| (Target::from(format!("m/f{index:05}")), None))
        .collect();
    targets.push((Target::from("m/none"), None));
    let results = db.read_many(targets).await.expect("reads");
    assert_eq!(results.len(), 151);
    for (index, result) in results.iter().take(150).enumerate() {
        assert_eq!(result.as_ref().expect("a value").value.len(), index);
    }
    assert!(matches!(results[150], Err(Error::Fs(FsError::NotFound))));
    let stats = db
        .stat_many((0..130).map(|index| format!("m/f{index:05}")))
        .await
        .expect("stats");
    assert!(
        stats
            .iter()
            .all(|result| result.as_ref().is_ok_and(|inode| inode.kind == Kind::File))
    );
}

pub async fn refuses_bad_requests<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    let stranger = Client::connect(server.endpoint(), "admin.wrong")
        .await
        .expect("connects");
    assert_eq!(code(stranger.stat("").await), Code::Unauthenticated);
    assert_eq!(
        code(stranger.commit([Op::mkdir("in")]).await),
        Code::Unauthenticated
    );
    let mut anonymous = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let unkeyed = anonymous.stat(pb::StatRequest::default()).await;
    assert_eq!(
        unkeyed.map(|_| ()).map_err(|status| status.code()),
        Err(Code::Unauthenticated)
    );
    let unkeyed = anonymous.commit(pb::CommitRequest::default()).await;
    assert_eq!(
        unkeyed.map(|_| ()).map_err(|status| status.code()),
        Err(Code::Unauthenticated)
    );
    assert_eq!(code(db.stat("a//b").await), Code::InvalidArgument);
    assert!(
        matches!(
            db.commit([Op::mkdir("..")]).await,
            Err(Error::Rejected(Rejection::Malformed))
        ),
        "an op the server can't read"
    );
    let mut selfish = db.batch();
    selfish.push(Op::mkdir("s"));
    selfish.push(Op::mkdir("t").if_applied(inodedb::OpRef::from_index(1)));
    assert!(
        matches!(
            selfish.commit().await,
            Err(Error::Rejected(Rejection::Malformed))
        ),
        "an op names itself"
    );
    let mut huge = db.batch();
    for index in 0..140 {
        huge.push(Op::put(format!("h{index}"), vec![0u8; 64 * 1024]));
    }
    assert!(matches!(
        huge.commit().await,
        Err(Error::Rejected(Rejection::TooLarge))
    ));
    assert_eq!(fs(db.stat("h0").await), FsError::NotFound);
    let mut crowded = db.batch();
    for index in 0..10_001 {
        crowded.push(Op::mkdir(format!("c{index}")));
    }
    assert!(matches!(
        crowded.commit().await,
        Err(Error::Rejected(Rejection::TooManyOps))
    ));
}

/// A commit whose answer is lost applies once: the client retries with the
/// same request id and hears the first answer.
pub async fn lost_answers_apply_once<B: Backend>(backend: &mut B) {
    let faults = Arc::new(Faults::default());
    let Fixture {
        db,
        server: _server,
        ..
    } = start_with_faults(backend.fresh(), faults.clone()).await;
    faults.lost_commit_answers.store(3, Ordering::Release);
    let commit = db
        .commit([Op::put("once", "1").create(), Op::mkdir("dir")])
        .await
        .expect("commits after retries");
    assert_eq!(
        faults.lost_commit_answers.load(Ordering::Acquire),
        0,
        "every answer was lost once"
    );
    assert!(
        commit.all_applied(),
        "the retries heard the first answer: {commit:?}"
    );
    let next = db.commit([Op::mkdir("after")]).await.expect("commits");
    assert_eq!(
        next.version,
        commit.version + 1,
        "the retries took no versions"
    );
}

/// A listing cut off midway picks up after its last entry, with nothing
/// missed or repeated.
pub async fn cut_listings_resume<B: Backend>(backend: &mut B) {
    let faults = Arc::new(Faults::default());
    let Fixture {
        db,
        server: _server,
        ..
    } = start_with_faults(backend.fresh(), faults.clone()).await;
    db.commit([Op::mkdir("big")]).await.expect("commits");
    puts(&db, "big", 2500, |index| Bytes::from(format!("v{index}"))).await;
    faults.cut_listings.store(2, Ordering::Release);
    let listing = db.list("big").await.expect("lists through the cuts");
    assert_eq!(
        faults.cut_listings.load(Ordering::Acquire),
        0,
        "both listings were cut"
    );
    let expected: Vec<String> = (0..2500).map(|index| format!("f{index:05}")).collect();
    assert_eq!(names(&listing), expected);
}

/// A read asking for a version the store never reached fails at once, and
/// leaves the client as it was.
pub async fn floors_past_the_store_fail_fast<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("a")]).await.expect("commits");
    let before = db.version();
    assert_eq!(code(db.wait_for(1 << 40).await), Code::OutOfRange);
    assert_eq!(db.version(), before, "the floor stays");
    assert!(db.stat("a").await.expect("stats").is_dir());
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let ahead = rpc
        .list(keyed(pb::ListRequest {
            path: Bytes::new(),
            at: Some(pb::ReadAt { at_least: 1 << 40 }),
            ..pb::ListRequest::default()
        }))
        .await
        .expect("opens")
        .into_inner()
        .message()
        .await;
    assert_eq!(
        ahead.map(|_| ()).map_err(|status| status.code()),
        Err(Code::OutOfRange)
    );
    db.wait_for(before).await.expect("a version the store has");
    server.stop().await;
}

/// The same request id answers once, however many times it arrives.
pub async fn retries_answer_once<B: Backend>(backend: &mut B) {
    let Fixture { server, db, .. } = fresh(backend).await;
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let request = |id: u8| {
        let mut request = tonic::Request::new(pb::CommitRequest {
            ops: vec![Op::put("once", "1").create().into_pb()],
            request_id: Bytes::from(vec![id; 16]),
            ..pb::CommitRequest::default()
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {KEY}").parse().expect("a header"),
        );
        request
    };
    let first = rpc.commit(request(7)).await.expect("commits").into_inner();
    db.commit([Op::put("other", "x")]).await.expect("commits");
    let again = rpc.commit(request(7)).await.expect("commits").into_inner();
    assert_eq!(again, first, "a retry gets the first answer");
    let fresh_id = rpc.commit(request(8)).await.expect("commits").into_inner();
    assert!(matches!(
        fresh_id.outcomes[0].kind,
        Some(pb::outcome::Kind::Skipped(pb::Skipped {
            reason: Some(pb::skipped::Reason::Error(code))
        })) if code == pb::Error::Exists as i32
    ));
}

pub async fn versions_advance<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    let mut last = db.version();
    for index in 0..20 {
        let commit = db
            .commit([Op::put(format!("v{index}"), "x")])
            .await
            .expect("commits");
        assert!(commit.version > last, "versions rise");
        last = commit.version;
        assert_eq!(db.version(), last);
    }
    let unchanged = db
        .commit([Op::mkdir("v0").if_absent(), Op::mkdir("dir").if_absent()])
        .await
        .expect("commits");
    assert!(unchanged.committed, "the second mkdir changes something");
    let noop = db
        .commit([Op::mkdir("dir").if_absent()])
        .await
        .expect("commits");
    assert!(!noop.committed && noop.all_applied());
    assert_eq!(
        noop.version, unchanged.version,
        "a batch that changes nothing takes no version"
    );
    db.wait_for(noop.version).await.expect("already there");
}

pub async fn concurrent_commits_all_apply<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("c")]).await.expect("commits");
    let tasks: Vec<_> = (0..400)
        .map(|index| {
            let db = db.clone();
            tokio::spawn(async move { db.commit([Op::put(format!("c/f{index:05}"), "x")]).await })
        })
        .collect();
    let mut versions = Vec::new();
    for task in tasks {
        let commit = task.await.expect("joins").expect("commits");
        assert!(commit.all_applied());
        versions.push(commit.version);
    }
    versions.sort_unstable();
    versions.dedup();
    assert_eq!(versions.len(), 400, "each batch takes its own version");
    assert_eq!(db.list("c").await.expect("lists").entries.len(), 400);
}

pub async fn durable_across_restart<B: Backend>(backend: &mut B) {
    let Fixture { store, server, db } = fresh(backend).await;
    db.commit([
        Op::mkdir("keep/deep").parents(),
        Op::put("keep/deep/f", "kept"),
    ])
    .await
    .expect("commits");
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let request = || {
        let mut request = tonic::Request::new(pb::CommitRequest {
            ops: vec![Op::put("keep/once", "1").create().into_pb()],
            request_id: Bytes::from(vec![9u8; 16]),
            ..pb::CommitRequest::default()
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {KEY}").parse().expect("a header"),
        );
        request
    };
    let first = rpc.commit(request()).await.expect("commits").into_inner();
    let version = db.version();
    drop((db, rpc));
    server.stop().await;
    let Fixture { server, db, .. } = start(backend.reopen(store)).await;
    assert_eq!(
        &db.read("keep/deep/f").await.expect("reads").value[..],
        b"kept"
    );
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    assert_eq!(
        rpc.commit(request()).await.expect("commits").into_inner(),
        first,
        "the retry is recognized"
    );
    let next = db
        .commit([Op::put("keep/next", "n")])
        .await
        .expect("commits");
    assert!(next.version > version, "versions continue past the restart");
}

pub async fn hard_links_count<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::put("f", "shared"), Op::mkdir("d")])
        .await
        .expect("commits");
    assert!(
        db.commit([Op::link("f", "d/g")])
            .await
            .expect("commits")
            .all_applied()
    );
    let (f, g) = (
        db.stat("f").await.expect("stats"),
        db.stat("d/g").await.expect("stats"),
    );
    assert_eq!((f.handle.clone(), f.nlink), (g.handle, 2));
    db.commit([Op::unlink("f")]).await.expect("commits");
    let g = db.read("d/g").await.expect("reads");
    assert_eq!((&g.value[..], g.inode.nlink), (&b"shared"[..], 1));
    let commit = db.commit([Op::link("d", "e")]).await.expect("commits");
    assert_eq!(
        commit.outcomes[0],
        Outcome::Skipped(Skip::Error(FsError::IsDir))
    );
}

pub async fn values_hold_to_the_limit<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    let limit = inodedb_core::types::MAX_VALUE;
    let full: Vec<u8> = (0..limit).map(|index| (index % 251) as u8).collect();
    assert!(
        db.commit([Op::put("full", full.clone())])
            .await
            .expect("commits")
            .all_applied()
    );
    assert_eq!(&db.read("full").await.expect("reads").value[..], &full[..]);
    let commit = db
        .commit([Op::put("over", vec![0u8; limit + 1])])
        .await
        .expect("commits");
    assert_eq!(
        commit.outcomes[0],
        Outcome::Skipped(Skip::Error(FsError::TooLarge))
    );
    let empty = db
        .commit([Op::put("empty", Bytes::new())])
        .await
        .expect("commits");
    assert!(empty.all_applied());
    assert_eq!(db.read("empty").await.expect("reads").value.len(), 0);
}

async fn watch(server: &Running, path: &str, from: u64) -> tonic::Streaming<pb::Change> {
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    rpc.watch(keyed(pb::WatchRequest {
        path: Bytes::copy_from_slice(path.as_bytes()),
        from,
    }))
    .await
    .expect("watches")
    .into_inner()
}

/// The next change with effects, skipping progress reports.
async fn next_change(changes: &mut tonic::Streaming<pb::Change>) -> pb::Change {
    loop {
        let change = tokio::time::timeout(std::time::Duration::from_secs(10), changes.message())
            .await
            .expect("a change in time")
            .expect("the stream holds")
            .expect("the stream goes on");
        if change.compacted || !change.entries.is_empty() || !change.inodes.is_empty() {
            return change;
        }
    }
}

fn names_changed(change: &pb::Change) -> Vec<String> {
    change
        .entries
        .iter()
        .map(|entry| String::from_utf8_lossy(&entry.name).into_owned())
        .collect()
}

/// A subtree's watcher sees what happens inside it and nothing outside,
/// in version order, with renames across its edge as entries and exits.
pub async fn watch_streams_a_subtree<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("w"), Op::mkdir("x/inner").parents()])
        .await
        .expect("commits");
    let watched = db.stat("w").await.expect("stats").handle.ino;
    let outside = db.stat("x").await.expect("stats").handle.ino;
    let mut changes = watch(&server, "w", 0).await;
    let first = db.commit([Op::put("w/f", "1")]).await.expect("commits");
    let change = next_change(&mut changes).await;
    assert_eq!(change.version, first.version);
    assert_eq!(names_changed(&change), ["f"]);
    db.commit([Op::put("x/g", "outside")])
        .await
        .expect("commits");
    db.commit([Op::rename("x", "w/x")]).await.expect("commits");
    let entered = next_change(&mut changes).await;
    assert_eq!(
        names_changed(&entered),
        ["x"],
        "the outside put never arrives"
    );
    assert_eq!(
        entered
            .entered
            .iter()
            .map(|handle| handle.ino)
            .collect::<Vec<_>>(),
        [outside],
        "a directory from outside enters"
    );
    db.commit([Op::put("w/x/inner/h", "deep")])
        .await
        .expect("commits");
    let deep = next_change(&mut changes).await;
    assert_eq!(
        names_changed(&deep),
        ["h"],
        "changes beneath an entered directory arrive"
    );
    db.commit([Op::rename("w/x", "x")]).await.expect("commits");
    let left = next_change(&mut changes).await;
    assert_eq!(names_changed(&left), ["x"]);
    assert!(
        left.entries[0].after.is_none(),
        "a directory moved out arrives as a removal"
    );
    assert_eq!(
        left.entries[0].dir.as_ref().map(|dir| dir.ino),
        Some(watched)
    );
    db.commit([Op::put("x/inner/later", "outside again")])
        .await
        .expect("commits");
    db.commit([Op::mkdir("w/last")]).await.expect("commits");
    assert_eq!(names_changed(&next_change(&mut changes).await), ["last"]);
}

/// A watch from an earlier version replays every change since, then
/// follows new ones.
pub async fn watch_replays_from_a_version<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("r")]).await.expect("commits");
    let from = db
        .commit([Op::put("r/a", "1")])
        .await
        .expect("commits")
        .version;
    db.commit([Op::put("r/b", "2")]).await.expect("commits");
    db.commit([Op::put("r/c", "3")]).await.expect("commits");
    let mut changes = watch(&server, "r", from).await;
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.extend(names_changed(&next_change(&mut changes).await));
    }
    assert_eq!(seen, ["a", "b", "c"]);
    db.commit([Op::put("r/d", "4")]).await.expect("commits");
    assert_eq!(names_changed(&next_change(&mut changes).await), ["d"]);
}

/// Once truncation passes a position, a watch from it reports compaction.
pub async fn watch_reports_compaction<B: Backend>(backend: &mut B) {
    let settings = Settings {
        retention: inodedb_server::feed::Retention {
            min: std::time::Duration::ZERO,
            every: std::time::Duration::from_millis(20),
            ..Default::default()
        },
        ..Settings::default()
    };
    let Fixture { db, server, .. } = start_with(backend.fresh(), settings).await;
    db.commit([Op::mkdir("t")]).await.expect("commits");
    db.commit([Op::put("t/a", "1")]).await.expect("commits");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut changes = watch(&server, "t", 1).await;
    let change = next_change(&mut changes).await;
    assert!(change.compacted && change.version == 1, "{change:?}");
    // A watch from now still works, under a lease whose position keeps
    // what it has yet to read.
    let at = db.version();
    let _lease = db.lease(at, at).await.expect("opens");
    let mut changes = watch(&server, "t", 0).await;
    db.commit([Op::put("t/b", "2")]).await.expect("commits");
    let change = next_change(&mut changes).await;
    assert_eq!(names_changed(&change), ["b"], "{change:?}");
}

/// A live lease keeps the entries past its position; once it ends,
/// truncation passes them.
pub async fn leases_hold_the_log<B: Backend>(backend: &mut B) {
    let pause = std::time::Duration::from_millis(200);
    let settings = Settings {
        retention: inodedb_server::feed::Retention {
            min: std::time::Duration::ZERO,
            every: std::time::Duration::from_millis(20),
            ..Default::default()
        },
        ..Settings::default()
    };
    let Fixture { db, server, .. } = start_with(backend.fresh(), settings).await;
    db.commit([Op::mkdir("t")]).await.expect("commits");
    let at = db.version();
    let lease = db.lease(at, at).await.expect("opens");
    db.commit([Op::put("t/a", "1")]).await.expect("commits");
    tokio::time::sleep(pause).await;
    let mut changes = watch(&server, "t", at + 1).await;
    let change = next_change(&mut changes).await;
    assert!(!change.compacted, "the lease holds the log past {at}");
    assert_eq!(names_changed(&change), ["a"]);
    lease.release().await;
    tokio::time::sleep(pause).await;
    let mut changes = watch(&server, "t", at + 1).await;
    assert!(next_change(&mut changes).await.compacted);
}

/// A cached client's lease holds what it frees until the cache is dropped.
pub async fn caches_hold_a_lease<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("t"), Op::put("t/f", "v")])
        .await
        .expect("commits");
    let cached = db.cached();
    caught_up(&cached, db.version()).await;
    let file = db.stat("t/f").await.expect("stats").handle;
    db.commit([Op::unlink("t/f")]).await.expect("commits");
    assert_eq!(db.claim_freed(10).await.expect("claims").entries, []);
    drop(cached);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let claim = db.claim_freed(10).await.expect("claims");
        if claim
            .entries
            .iter()
            .any(|entry| entry.handle.ino == file.ino)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "dropping the cache releases its lease"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A snapshot reads as of its version while the cache moves on, and the
/// cache's lease holds what it reads.
pub async fn snapshots_hold_their_version<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([Op::mkdir("t"), Op::put("t/a", "1"), Op::put("t/b", "2")])
        .await
        .expect("commits");
    let cached = db.cached();
    caught_up(&cached, db.version()).await;
    let snapshot = cached.snapshot().await.expect("opens");
    db.commit([Op::put("t/a", "3"), Op::unlink("t/b"), Op::mkdir("t/c")])
        .await
        .expect("commits");
    caught_up(&cached, db.version()).await;
    let names = |listing: inodedb::Listing| -> Vec<Bytes> {
        listing
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .collect()
    };
    assert_eq!(names(snapshot.list("t").await.expect("lists")), ["a", "b"]);
    assert_eq!(&snapshot.read("t/a").await.expect("reads").value[..], b"1");
    assert_eq!(&snapshot.read("t/b").await.expect("reads").value[..], b"2");
    assert!(matches!(
        snapshot.stat("t/c").await,
        Err(Error::Fs(FsError::NotFound))
    ));
    assert_eq!(names(cached.list("t").await.expect("lists")), ["a", "c"]);
    assert_eq!(&cached.read("t/a").await.expect("reads").value[..], b"3");
    let values = |listing: inodedb::Listing| -> Vec<Option<Bytes>> {
        listing
            .entries
            .into_iter()
            .map(|entry| entry.value)
            .collect()
    };
    assert_eq!(
        values(snapshot.list_with_values("t").await.expect("lists")),
        [Some(Bytes::from("1")), Some(Bytes::from("2"))]
    );
    assert_eq!(
        values(cached.list_with_values("t").await.expect("lists")),
        [Some(Bytes::from("3")), None]
    );
    assert!(snapshot.version() < cached.version());
}

/// The cache's heartbeats report its oldest snapshot, so a consumer
/// claims nothing the snapshot may read until it drops.
pub async fn snapshots_hold_what_they_read<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let Fixture {
        db,
        server: _server,
        ..
    } = start_with(
        store,
        Settings {
            writer: inodedb_core::writer::Config {
                lease_ttl: 300,
                ..Default::default()
            },
            ..Settings::default()
        },
    )
    .await;
    db.commit([Op::mkdir("t"), Op::put("t/f", "v")])
        .await
        .expect("commits");
    let cached = db.cached();
    caught_up(&cached, db.version()).await;
    let snapshot = cached.snapshot().await.expect("opens");
    db.commit([Op::unlink("t/f")]).await.expect("commits");
    caught_up(&cached, db.version()).await;
    // Several heartbeats pass.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(db.claim_freed(10).await.expect("claims").entries, []);
    assert_eq!(&snapshot.read("t/f").await.expect("reads").value[..], b"v");
    drop(snapshot);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while db.claim_freed(10).await.expect("claims").entries.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "a dropped snapshot holds nothing"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A cache starts while commits keep raising the release horizon: its
/// lease opens at a version the horizon can't have passed.
pub async fn caches_start_under_steady_writes<B: Backend>(backend: &mut B) {
    let faults = Arc::new(Faults::default());
    let Fixture { db, server, .. } = start_with_faults(backend.fresh(), faults.clone()).await;
    db.commit([Op::mkdir("t")]).await.expect("commits");
    // Commits land while each lease opening waits.
    faults.slow_lease_opens.store(1_000, Ordering::Release);
    // A client of its own, so the cache's client sees none of its commits.
    let writer = Client::connect(server.endpoint(), KEY)
        .await
        .expect("connects");
    let writes = tokio::spawn(async move {
        let mut index = 0u64;
        loop {
            let _ = writer.commit([Op::put("t/f", index.to_string())]).await;
            index += 1;
        }
    });
    let cached = db.cached();
    let started = tokio::time::timeout(std::time::Duration::from_secs(10), cached.snapshot()).await;
    writes.abort();
    assert!(
        matches!(started, Ok(Ok(_))),
        "the cache started under steady writes"
    );
}

/// A claim reads a replaced value through its own free entry, after the
/// file's later entry is acknowledged and its record gone.
pub async fn claims_read_values_their_file_outlived<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let Fixture {
        db,
        server: _server,
        ..
    } = start_with(
        store,
        Settings {
            writer: inodedb_core::writer::Config {
                claim_ttl: 200,
                ..Default::default()
            },
            ..Settings::default()
        },
    )
    .await;
    db.commit([Op::mkdir("t"), Op::put("t/f", "first")])
        .await
        .expect("commits");
    db.commit([Op::put("t/f", "second")])
        .await
        .expect("commits");
    db.commit([Op::unlink("t/f")]).await.expect("commits");
    let claim = db.claim_freed(10).await.expect("claims");
    let (replaced, file): (Vec<_>, Vec<_>) = claim
        .entries
        .iter()
        .cloned()
        .partition(|entry| entry.value_version.is_some());
    assert_eq!((replaced.len(), file.len()), (1, 1), "{:?}", claim.entries);
    db.ack_freed(claim.id, &file).await.expect("acknowledges");
    // The replaced value's claim runs out, and a new claim reads it.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let again = db.claim_freed(10).await.expect("claims");
    assert_eq!(again.entries.len(), 1);
    assert_eq!(again.entries[0].value.as_deref(), Some(&b"first"[..]));
    assert_eq!(again.entries[0].inode, None, "the file's record is gone");
}

async fn caught_up(cached: &inodedb::Cached, version: u64) {
    tokio::time::timeout(std::time::Duration::from_secs(10), cached.wait_for(version))
        .await
        .expect("the cache catches up in time")
        .expect("the cache runs");
}

/// A cached client answers from its cache once warm, and the feed keeps it
/// current through renames, writes and removals.
pub async fn caches_follow_the_feed<B: Backend>(backend: &mut B) {
    let Fixture {
        db,
        server: _server,
        ..
    } = fresh(backend).await;
    db.commit([
        Op::mkdir("t/p1").parents(),
        Op::put("t/p1/a", "one"),
        Op::put("t/p1/b", "two"),
    ])
    .await
    .expect("commits");
    let cached = db.cached();
    caught_up(&cached, db.version()).await;
    assert_eq!(
        &cached.read("t/p1/a").await.expect("reads").value[..],
        b"one"
    );
    let hits = cached.hits();
    assert_eq!(
        &cached.read("t/p1/b").await.expect("reads").value[..],
        b"two"
    );
    assert_eq!(cached.stat("t/p1").await.expect("stats").kind, Kind::Dir);
    assert!(cached.hits() > hits, "a warm cache answers locally");
    // Changes arrive through the feed.
    let moved = db
        .commit([
            Op::rename("t/p1", "t/p2"),
            Op::put("t/p2/a", "uno"),
            Op::unlink("t/p2/b"),
        ])
        .await
        .expect("commits");
    caught_up(&cached, moved.version).await;
    assert_eq!(fs(cached.stat("t/p1").await), FsError::NotFound);
    assert_eq!(
        &cached.read("t/p2/a").await.expect("reads").value[..],
        b"uno"
    );
    assert_eq!(fs(cached.stat("t/p2/b").await), FsError::NotFound);
    let listing = cached.list("t").await.expect("lists");
    assert_eq!(names(&listing), ["p2"]);
    assert_eq!(names(&cached.list("t/p2").await.expect("lists")), ["a"]);
    assert_eq!(
        cached.stat("t/p2/a").await.expect("stats"),
        db.stat("t/p2/a").await.expect("stats"),
        "the cache and the server agree"
    );
    // Handles from the cache work with the server, and names need not be
    // UTF-8.
    let handle = cached.stat("t/p2/a").await.expect("stats").handle;
    assert_eq!(
        &db.read(Target::Handle(handle)).await.expect("reads").value[..],
        b"uno"
    );
    let named = db
        .commit([Op::put(&b"t/p2/\xff"[..], "odd")])
        .await
        .expect("commits");
    caught_up(&cached, named.version).await;
    let fresh = db.cached();
    caught_up(&fresh, named.version).await;
    assert_eq!(
        &fresh.read(&b"t/p2/\xff"[..]).await.expect("reads").value[..],
        b"odd"
    );
    assert_eq!(fresh.list("t/p2").await.expect("lists").entries.len(), 2);
}

fn keyed_as<T>(key: &str, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {key}").parse().expect("a header"),
    );
    request
}

/// A key reaches only its root's tree, does only what its grant allows,
/// and stops working once revoked, or once its root goes.
/// A check reads the whole store, so only an admin key rooted at the tree's
/// root runs one.
pub async fn checks_take_a_root_admin_key<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("t"), Op::put("t/f", "v")])
        .await
        .expect("commits");
    let checked = db.check().await.expect("checks");
    assert_eq!(checked.problems, Vec::<String>::new());
    assert_eq!(checked.version, db.version());
    let day = std::time::Duration::from_secs(86_400);
    for (root, grant) in [("t", inodedb::Grant::Admin), ("", inodedb::Grant::Write)] {
        let key = db.mint_key(root, grant, day).await.expect("mints");
        let other = Client::connect(server.endpoint(), &key)
            .await
            .expect("connects");
        assert_eq!(
            code(other.check().await),
            Code::PermissionDenied,
            "{root:?} {grant:?}"
        );
    }
}

pub async fn keys_bound_their_reach<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([
        Op::mkdir("t/a").parents(),
        Op::put("t/a/f", "in"),
        Op::put("g", "out"),
    ])
    .await
    .expect("commits");
    let day = std::time::Duration::from_secs(86_400);
    let writer_key = db
        .mint_key("t", inodedb::Grant::Write, day)
        .await
        .expect("mints");
    let writer = Client::connect(server.endpoint(), &writer_key)
        .await
        .expect("connects");
    assert_eq!(&writer.read("a/f").await.expect("reads").value[..], b"in");
    assert_eq!(fs(writer.stat("g").await), FsError::NotFound);
    assert!(
        writer
            .commit([Op::put("a/h", "new")])
            .await
            .expect("commits")
            .all_applied()
    );
    assert_eq!(&db.read("t/a/h").await.expect("reads").value[..], b"new");
    // A handle works only with the key that received it, and within its
    // tree.
    let outside = db.stat("g").await.expect("stats").handle;
    assert_eq!(
        code(writer.stat(Target::Handle(outside)).await),
        Code::InvalidArgument
    );
    let inside = writer.stat("a/f").await.expect("stats").handle;
    assert_eq!(
        writer
            .stat(Target::Handle(inside.clone()))
            .await
            .expect("stats")
            .size,
        2
    );
    assert_eq!(
        code(db.stat(Target::Handle(inside)).await),
        Code::InvalidArgument
    );
    // Grants.
    let reader_key = db
        .mint_key("t/a", inodedb::Grant::Read, day)
        .await
        .expect("mints");
    let reader = Client::connect(server.endpoint(), &reader_key)
        .await
        .expect("connects");
    assert_eq!(reader.stat("f").await.expect("stats").size, 2);
    assert_eq!(
        code(reader.commit([Op::mkdir("x")]).await),
        Code::PermissionDenied
    );
    assert_eq!(
        code(writer.mint_key("a", inodedb::Grant::Read, day).await),
        Code::PermissionDenied
    );
    // An admin key mints within its own tree, and revoking it revokes what
    // it minted.
    let admin_key = db
        .mint_key("t", inodedb::Grant::Admin, day)
        .await
        .expect("mints");
    let admin = Client::connect(server.endpoint(), &admin_key)
        .await
        .expect("connects");
    let minted_key = admin
        .mint_key("a", inodedb::Grant::Read, day)
        .await
        .expect("mints");
    let minted = Client::connect(server.endpoint(), &minted_key)
        .await
        .expect("connects");
    assert_eq!(minted.stat("f").await.expect("stats").size, 2);
    assert_eq!(
        code(minted.commit([Op::mkdir("y")]).await),
        Code::PermissionDenied
    );
    assert_eq!(code(admin.revoke_key(&writer_key).await), Code::NotFound);
    db.revoke_key(&admin_key).await.expect("revokes");
    assert_eq!(code(admin.stat("").await), Code::Unauthenticated);
    assert_eq!(code(minted.stat("").await), Code::Unauthenticated);
    // Removing a key's root revokes it.
    db.commit([Op::remove_all("t/a")]).await.expect("commits");
    assert_eq!(code(reader.stat("").await), Code::Unauthenticated);
    assert_eq!(writer.list("").await.expect("lists").entries.len(), 0);
}

/// A stream ends when its key is revoked.
pub async fn watch_ends_with_its_key<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("w")]).await.expect("commits");
    let key = db
        .mint_key("w", inodedb::Grant::Read, std::time::Duration::ZERO)
        .await
        .expect("mints");
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let mut changes = rpc
        .watch(keyed_as(&key, pb::WatchRequest::default()))
        .await
        .expect("watches")
        .into_inner();
    db.commit([Op::put("w/f", "1")]).await.expect("commits");
    let change = next_change(&mut changes).await;
    assert_eq!(names_changed(&change), ["f"]);
    db.revoke_key(&key).await.expect("revokes");
    let ended = loop {
        match tokio::time::timeout(std::time::Duration::from_secs(10), changes.message())
            .await
            .expect("the stream ends in time")
        {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("the stream ended without saying why"),
            Err(status) => break status,
        }
    };
    assert_eq!(ended.code(), Code::Unauthenticated);
    // A stream can't start past the version its key was checked at.
    let other = db
        .mint_key("w", inodedb::Grant::Read, std::time::Duration::ZERO)
        .await
        .expect("mints");
    let future = rpc
        .watch(keyed_as(
            &other,
            pb::WatchRequest {
                from: db.version() + 5,
                ..pb::WatchRequest::default()
            },
        ))
        .await
        .map(|_| ())
        .expect_err("refused");
    assert_eq!(future.code(), Code::OutOfRange);
}

/// A key's stream shows only what lies in its tree: nothing from before
/// the watched directory entered it, and nothing once it leaves.
pub async fn watch_stays_in_its_keys_tree<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([
        Op::mkdir("secret/d").parents(),
        Op::mkdir("k"),
        Op::mkdir("other"),
    ])
    .await
    .expect("commits");
    db.commit([Op::put("secret/d/hidden", "x")])
        .await
        .expect("commits");
    db.commit([Op::unlink("secret/d/hidden")])
        .await
        .expect("commits");
    db.commit([Op::rename("secret/d", "k/d")])
        .await
        .expect("commits");
    let key = db
        .mint_key("k", inodedb::Grant::Read, std::time::Duration::ZERO)
        .await
        .expect("mints");
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let mut changes = rpc
        .watch(keyed_as(
            &key,
            pb::WatchRequest {
                path: Bytes::from_static(b"d"),
                from: 1,
            },
        ))
        .await
        .expect("watches")
        .into_inner();
    db.commit([Op::put("k/d/seen", "y")])
        .await
        .expect("commits");
    let mut names = Vec::new();
    while !names.contains(&"seen".to_string()) {
        names.extend(names_changed(&next_change(&mut changes).await));
    }
    assert!(
        !names.contains(&"hidden".to_string()),
        "replayed from outside the key: {names:?}"
    );
    db.commit([Op::rename("k/d", "other/d")])
        .await
        .expect("commits");
    db.commit([Op::put("other/d/outside", "z")])
        .await
        .expect("commits");
    let ended = loop {
        match tokio::time::timeout(std::time::Duration::from_secs(10), changes.message())
            .await
            .expect("the stream ends in time")
        {
            Ok(Some(change)) => assert!(
                !names_changed(&change).contains(&"outside".to_string()),
                "a change outside the key's tree"
            ),
            Ok(None) => panic!("the stream ended without saying why"),
            Err(status) => break status,
        }
    };
    assert_eq!(ended.code(), Code::NotFound);
}

/// A cached client whose key is revoked stops answering.
pub async fn caches_stop_with_their_key<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("t"), Op::put("t/f", "x")])
        .await
        .expect("commits");
    let key = db
        .mint_key("t", inodedb::Grant::Read, std::time::Duration::ZERO)
        .await
        .expect("mints");
    let reader = Client::connect(server.endpoint(), &key)
        .await
        .expect("connects");
    let cached = reader.cached();
    caught_up(&cached, db.version()).await;
    assert_eq!(&cached.read("f").await.expect("reads").value[..], b"x");
    db.revoke_key(&key).await.expect("revokes");
    let refused = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match cached.stat("f").await {
                Err(Error::Status(status)) => break status,
                _ => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("the cache stops");
    assert_eq!(refused.code(), Code::Unauthenticated);
}

/// A listing that resumes by the directory's handle goes on with that
/// directory, though a rename has put another at its path.
pub async fn listings_follow_their_directory<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("d"), Op::put("d/a", "1"), Op::put("d/b", "2")])
        .await
        .expect("commits");
    let mut rpc = pb::inode_db_client::InodeDbClient::connect(server.endpoint())
        .await
        .expect("connects");
    let first = rpc
        .list(keyed(pb::ListRequest {
            path: Bytes::from_static(b"d"),
            limit: 1,
            ..pb::ListRequest::default()
        }))
        .await
        .expect("lists")
        .into_inner()
        .message()
        .await
        .expect("a page")
        .expect("a page");
    assert_eq!(first.entries.len(), 1);
    db.commit([
        Op::rename("d", "old"),
        Op::mkdir("d"),
        Op::put("d/z", "new"),
    ])
    .await
    .expect("commits");
    let rest = rpc
        .list(keyed(pb::ListRequest {
            path: Bytes::from_static(b"d"),
            dir: first.dir.clone(),
            after: Some(first.entries[0].name.clone()),
            ..pb::ListRequest::default()
        }))
        .await
        .expect("lists")
        .into_inner()
        .message()
        .await
        .expect("a page")
        .expect("a page");
    let names: Vec<&[u8]> = rest.entries.iter().map(|entry| &entry.name[..]).collect();
    assert_eq!(names, [b"b"]);
    assert_eq!(rest.dir, first.dir);
    let listing = db.list("d").await.expect("lists");
    assert_ne!(Some(listing.dir.ino), first.dir.map(|dir| dir.ino));
}

/// Shutdown ends open streams instead of waiting out its grace.
pub async fn shutdown_ends_streams<B: Backend>(backend: &mut B) {
    let Fixture { db, server, .. } = fresh(backend).await;
    db.commit([Op::mkdir("w")]).await.expect("commits");
    let cached = db.cached();
    caught_up(&cached, db.version()).await;
    let _changes = watch(&server, "w", 0).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), server.stop())
        .await
        .expect("the server stops promptly");
}

/// Freed records stay readable while a lease holds them, and a consumer
/// claims and acknowledges them once released.
pub async fn leases_hold_freed_records<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let Fixture { db, server, .. } = start_with(
        store,
        Settings {
            writer: inodedb_core::writer::Config {
                lease_ttl: 2_000,
                ..Default::default()
            },
            ..Settings::default()
        },
    )
    .await;
    db.commit([Op::mkdir("t"), Op::mkdir("u"), Op::put("t/f", "data")])
        .await
        .expect("commits");
    let handle = db.stat("t/f").await.expect("stats").handle;
    let at = db.version();
    let mut lease = db.lease(at, at).await.expect("opens");
    db.commit([Op::unlink("t/f")]).await.expect("commits");
    assert_eq!(db.claim_freed(10).await.expect("claims").entries, []);
    assert_eq!(
        &db.read(Target::Handle(handle.clone()))
            .await
            .expect("reads")
            .value[..],
        b"data",
        "a freed file reads by handle while the lease holds it"
    );
    assert!(matches!(
        db.lease(at - 1, at).await,
        Err(Error::Status(status)) if status.code() == Code::FailedPrecondition
    ));
    lease.beat(db.version(), db.version()).await.expect("beats");
    // A consumer rooted elsewhere sees nothing of t's.
    let day = std::time::Duration::from_secs(86_400);
    let elsewhere = Client::connect(
        server.endpoint(),
        db.mint_key("u", inodedb::Grant::Write, day)
            .await
            .expect("mints"),
    )
    .await
    .expect("connects");
    assert_eq!(elsewhere.claim_freed(10).await.expect("claims").entries, []);
    let claim = db.claim_freed(10).await.expect("claims");
    assert_eq!(claim.entries.len(), 1);
    let entry = &claim.entries[0];
    assert_eq!(entry.handle, handle);
    assert_eq!(entry.value.as_deref(), Some(&b"data"[..]));
    assert_eq!(
        entry.inode.as_ref().map(|inode| inode.life),
        Some(inodedb::Life::Freed)
    );
    db.ack_freed(claim.id, &claim.entries)
        .await
        .expect("acknowledges");
    db.ack_freed(claim.id, &claim.entries)
        .await
        .map(|_| ())
        .or_else(|error| match error {
            Error::Status(status) if status.code() == Code::NotFound => Ok(()),
            other => Err(other),
        })
        .expect("a repeat acknowledges nothing more");
    assert_eq!(fs(db.read(Target::Handle(handle)).await), FsError::NotFound);
    // A lease runs out without heartbeats.
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    assert!(matches!(
        lease.beat(db.version(), db.version()).await,
        Err(Error::LeaseExpired)
    ));
}

/// Servers that share a store, with a short writer lease so takeovers come
/// quickly.
fn standby_settings() -> Settings {
    Settings {
        writer: inodedb_core::writer::Config {
            writer_lease: 600,
            renew_every: 100,
            takeover_margin: 200,
            ..Default::default()
        },
        standby: true,
        ..Settings::default()
    }
}

/// A standby answers reads at a version its client has seen, and sends
/// the rest on to the writer, which the client then talks to.
pub async fn standbys_serve_reads_and_send_writes_on<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let writer = inodedb_server::start_with(store.clone(), KEY, standby_settings()).await;
    let db = Client::connect(writer.endpoint(), KEY)
        .await
        .expect("connects");
    let first = db
        .commit([Op::mkdir("t"), Op::put("t/f", "1")])
        .await
        .expect("commits once the first server takes the empty store");
    let standby = inodedb_server::start_with(store.clone(), KEY, standby_settings()).await;
    let via = Client::connect(standby.endpoint(), KEY)
        .await
        .expect("connects");
    via.wait_for(first.version)
        .await
        .expect("the standby reaches the commit");
    assert_eq!(&via.read("t/f").await.expect("reads").value[..], b"1");
    assert_eq!(
        via.list("t").await.expect("lists").entries.len(),
        1,
        "listings at a version"
    );
    assert_eq!(via.endpoint(), standby.endpoint(), "the standby answered");
    // The newest state is the writer's to give.
    let latest = Client::connect(standby.endpoint(), KEY)
        .await
        .expect("connects");
    assert_eq!(&latest.read("t/f").await.expect("reads").value[..], b"1");
    assert_eq!(
        latest.endpoint(),
        writer.endpoint(),
        "a read of the newest state goes to the writer"
    );
    let second = via
        .commit([Op::put("t/f", "2")])
        .await
        .expect("commits through the writer");
    assert_eq!(
        via.endpoint(),
        writer.endpoint(),
        "the client follows NotWriter"
    );
    assert!(second.version > first.version);
    // A cache follows the feed through the standby.
    let cache = Client::connect(standby.endpoint(), KEY)
        .await
        .expect("connects");
    cache.wait_for(second.version).await.expect("reaches it");
    let cached = cache.cached();
    caught_up(&cached, second.version).await;
    assert_eq!(&cached.read("t/f").await.expect("reads").value[..], b"2");
}

/// The writer dies while clients commit; a standby takes over, and every
/// batch a client heard commit is there, once.
pub async fn takeovers_keep_every_acknowledged_batch<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let first = inodedb_server::start_with(store.clone(), KEY, standby_settings()).await;
    let db = Client::connect(first.endpoint(), KEY)
        .await
        .expect("connects");
    db.commit([Op::mkdir("t")]).await.expect("commits");
    let standby = inodedb_server::start_with(store.clone(), KEY, standby_settings()).await;
    let acked = Arc::new(std::sync::Mutex::new(Vec::new()));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut writers = Vec::new();
    for client in 0..4 {
        // Clients start at the standby, as behind a service address, and
        // follow it to the writer.
        let db = Client::connect(standby.endpoint(), KEY)
            .await
            .expect("connects");
        let (acked, running) = (acked.clone(), running.clone());
        writers.push(tokio::spawn(async move {
            let mut index = 0;
            while running.load(Ordering::Acquire) {
                let name = format!("t/c{client}-{index}");
                if let Ok(commit) = db.commit([Op::put(&name, "v").create()]).await {
                    acked
                        .lock()
                        .expect("no panics")
                        .push((name, commit.version));
                }
                index += 1;
            }
        }));
    }
    let count = || acked.lock().expect("no panics").len();
    while count() < 20 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // The writer dies mid-group.
    drop(first);
    let before = count();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while count() < before + 20 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "commits resume after the takeover"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    running.store(false, Ordering::Release);
    for writer in writers {
        writer.await.expect("the client task ends");
    }
    let check = Client::connect(standby.endpoint(), KEY)
        .await
        .expect("connects");
    let acked = acked.lock().expect("no panics").clone();
    let listing = check.list("t").await.expect("lists");
    assert_eq!(
        check.endpoint(),
        standby.endpoint(),
        "the standby took over: it answered a read of the newest state"
    );
    let names: std::collections::BTreeSet<String> = listing
        .entries
        .iter()
        .map(|entry| format!("t/{}", String::from_utf8_lossy(&entry.name)))
        .collect();
    for (name, version) in &acked {
        assert!(
            names.contains(name),
            "{name}, acknowledged at {version}, is gone"
        );
    }
    let versions: std::collections::BTreeSet<u64> = acked.iter().map(|(_, v)| *v).collect();
    assert_eq!(versions.len(), acked.len(), "each batch committed once");
}
