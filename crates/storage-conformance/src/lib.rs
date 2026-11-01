//! The storage contract as tests generic over a backend.
//!
//! A backend's test file calls `conformance!` with an expression that makes
//! a `Backend`; each check becomes its own test.

pub mod backends;

use inodedb_core::keys::{self, KeyRange, KvRead, KvResult, KvWrite, Pairs, WriteSet};
use inodedb_core::writer::CommitOutcome;
use inodedb_storage::{CommitError, MAX_KEY, MAX_TRANSACTION, MAX_VALUE, Storage, Txn};
use std::time::Duration;

/// Makes stores for the checks.
pub trait Backend {
    type S: Storage + Clone;

    /// A new, empty store.
    fn fresh(&mut self) -> Self::S;

    /// Closes a store and opens it again from what it made durable.
    fn reopen(&mut self, store: Self::S) -> Self::S;
}

#[macro_export]
macro_rules! conformance {
    ($make:expr) => {
        $crate::conformance!(@tests $make;
            byte_order, ranges_and_limits, gets_and_clears, read_your_writes,
            uncommitted_writes_stay_hidden, write_skew_fails, scans_conflict_with_inserts,
            reading_the_head_fences, log_order_and_truncation, wait_past_wakes,
            limits_hold, durable_across_reopen, commit_and_resolve);
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

fn key(text: &[u8]) -> Vec<u8> {
    let mut key = vec![0x02];
    key.extend_from_slice(text);
    key
}

async fn put_all<S: Storage>(store: &S, pairs: &[(&[u8], &[u8])]) {
    let mut txn = store.begin().await.expect("begin");
    for (k, v) in pairs {
        txn.set(&key(k), v);
    }
    txn.commit().await.expect("commit");
}

async fn scan_all<S: Storage>(store: &S, range: KeyRange, limit: usize) -> Pairs {
    match store
        .snapshot_read(vec![KvRead::Scan { range, limit }])
        .await
        .expect("read")
        .pop()
    {
        Some(KvResult::Scan(pairs)) => pairs,
        other => panic!("{other:?}"),
    }
}

async fn get<S: Storage>(store: &S, k: &[u8]) -> Option<Vec<u8>> {
    match store
        .snapshot_read(vec![KvRead::Get(key(k))])
        .await
        .expect("read")
        .pop()
    {
        Some(KvResult::Get(value)) => value,
        other => panic!("{other:?}"),
    }
}

fn all() -> KeyRange {
    KeyRange::prefix(vec![0x02])
}

pub async fn byte_order<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let keys: [&[u8]; 6] = [b"\xff", b"\x01\xff", b"\x01", b"\x01\x00", b"\x02", b""];
    put_all(&store, &keys.map(|k| (k, &b"v"[..]))).await;
    let scanned: Vec<Vec<u8>> = scan_all(&store, all(), usize::MAX)
        .await
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| key(k)).collect();
    expected.sort();
    assert_eq!(scanned, expected, "scans order bytes lexicographically");
}

pub async fn ranges_and_limits<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    put_all(
        &store,
        &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")],
    )
    .await;
    let range = KeyRange {
        start: key(b"b"),
        end: key(b"d"),
    };
    let pairs = scan_all(&store, range.clone(), usize::MAX).await;
    assert_eq!(
        pairs,
        vec![(key(b"b"), b"2".to_vec()), (key(b"c"), b"3".to_vec())],
        "half-open"
    );
    assert_eq!(scan_all(&store, range, 1).await.len(), 1, "limits");
    assert!(
        scan_all(
            &store,
            KeyRange {
                start: key(b"x"),
                end: key(b"z")
            },
            9
        )
        .await
        .is_empty()
    );
}

pub async fn gets_and_clears<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    put_all(&store, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]).await;
    assert_eq!(get(&store, b"a").await, Some(b"1".to_vec()));
    let mut txn = store.begin().await.expect("begin");
    txn.clear(&key(b"a"));
    txn.clear_range(KeyRange {
        start: key(b"b"),
        end: key(b"c"),
    });
    txn.set(&key(b"c"), b"30");
    txn.commit().await.expect("commit");
    assert_eq!(get(&store, b"a").await, None);
    assert_eq!(get(&store, b"b").await, None);
    assert_eq!(get(&store, b"c").await, Some(b"30".to_vec()));
}

pub async fn read_your_writes<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    put_all(&store, &[(b"a", b"1"), (b"b", b"2")]).await;
    let mut txn = store.begin().await.expect("begin");
    txn.set(&key(b"c"), b"3");
    txn.clear(&key(b"a"));
    assert_eq!(txn.get(&key(b"c")).await.expect("get"), Some(b"3".to_vec()));
    assert_eq!(txn.get(&key(b"a")).await.expect("get"), None);
    let pairs = txn.scan(all(), usize::MAX).await.expect("scan");
    assert_eq!(
        pairs,
        vec![(key(b"b"), b"2".to_vec()), (key(b"c"), b"3".to_vec())]
    );
    txn.clear_range(all());
    assert!(txn.scan(all(), usize::MAX).await.expect("scan").is_empty());
    txn.set(&key(b"z"), b"26");
    assert_eq!(
        txn.get(&key(b"z")).await.expect("get"),
        Some(b"26".to_vec())
    );
}

pub async fn uncommitted_writes_stay_hidden<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let mut writer = store.begin().await.expect("begin");
    writer.set(&key(b"a"), b"1");
    let mut reader = store.begin().await.expect("begin");
    assert_eq!(reader.get(&key(b"a")).await.expect("get"), None);
    assert_eq!(get(&store, b"a").await, None);
    drop(writer);
    assert_eq!(
        get(&store, b"a").await,
        None,
        "a dropped transaction writes nothing"
    );
}

pub async fn write_skew_fails<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    put_all(&store, &[(b"x", b"0"), (b"y", b"0")]).await;
    let mut first = store.begin().await.expect("begin");
    let mut second = store.begin().await.expect("begin");
    first.get(&key(b"x")).await.expect("get");
    second.get(&key(b"y")).await.expect("get");
    first.set(&key(b"y"), b"1");
    second.set(&key(b"x"), b"1");
    first.commit().await.expect("the first commits");
    assert_eq!(second.commit().await, Err(CommitError::Conflict));
}

pub async fn scans_conflict_with_inserts<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let mut scanner = store.begin().await.expect("begin");
    assert!(
        scanner
            .scan(all(), usize::MAX)
            .await
            .expect("scan")
            .is_empty()
    );
    put_all(&store, &[(b"phantom", b"1")]).await;
    scanner.set(&[0x03], b"elsewhere");
    assert_eq!(scanner.commit().await, Err(CommitError::Conflict));
}

pub async fn reading_the_head_fences<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let mut old = store.begin().await.expect("begin");
    old.get(&keys::head()).await.expect("get");
    let mut new = store.begin().await.expect("begin");
    new.set(&keys::head(), b"epoch 2");
    new.commit().await.expect("commit");
    old.set(&key(b"stale"), b"write");
    assert_eq!(old.commit().await, Err(CommitError::Conflict));
}

pub async fn log_order_and_truncation<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    for version in [1u64, 2] {
        let mut txn = store.begin().await.expect("begin");
        txn.append_log(version, format!("entry {version}").as_bytes());
        txn.commit().await.expect("commit");
    }
    let mut txn = store.begin().await.expect("begin");
    for version in [3u64, 4, 5] {
        txn.append_log(version, format!("entry {version}").as_bytes());
    }
    txn.commit().await.expect("commit");
    let from_three: Vec<u64> = store
        .read_log(3, usize::MAX)
        .await
        .expect("log")
        .into_iter()
        .map(|(version, _)| version)
        .collect();
    assert_eq!(from_three, [3, 4, 5]);
    assert_eq!(store.read_log(1, 2).await.expect("log").len(), 2);
    let large = vec![9u8; MAX_VALUE * 2];
    let mut txn = store.begin().await.expect("begin");
    txn.append_log(6, &large);
    txn.commit().await.expect("commit");
    assert_eq!(
        store.read_log(6, 1).await.expect("log"),
        vec![(6, large)],
        "an entry larger than a value reads back whole"
    );
    store.truncate_log(4).await.expect("truncate");
    let left: Vec<u64> = store
        .read_log(0, usize::MAX)
        .await
        .expect("log")
        .into_iter()
        .map(|(version, _)| version)
        .collect();
    assert_eq!(left, [4, 5, 6]);
}

pub async fn wait_past_wakes<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let waiter = {
        let store = store.clone();
        tokio::spawn(async move { store.wait_past(1).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut txn = store.begin().await.expect("begin");
    txn.append_log(1, b"one");
    txn.commit().await.expect("commit");
    let mut txn = store.begin().await.expect("begin");
    txn.append_log(2, b"two");
    txn.commit().await.expect("commit");
    let woke = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("woke in time")
        .expect("joined")
        .expect("ok");
    assert!(woke >= 2);
    assert_eq!(
        store.wait_past(1).await.expect("ok"),
        2,
        "a version already past answers at once"
    );
}

pub async fn limits_hold<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let at_limit = |len: usize| {
        let mut key = vec![0x02];
        key.resize(len, b'k');
        key
    };
    let mut txn = store.begin().await.expect("begin");
    txn.set(&at_limit(MAX_KEY), &vec![1; MAX_VALUE]);
    txn.commit()
        .await
        .expect("keys and values at their limits commit");
    let mut txn = store.begin().await.expect("begin");
    txn.set(&at_limit(MAX_KEY + 1), b"v");
    assert_eq!(txn.commit().await, Err(CommitError::TooLarge));
    let mut txn = store.begin().await.expect("begin");
    txn.set(&key(b"big"), &vec![1; MAX_VALUE + 1]);
    assert_eq!(txn.commit().await, Err(CommitError::TooLarge));
    let mut txn = store.begin().await.expect("begin");
    for index in 0..(MAX_TRANSACTION / MAX_VALUE + 1) {
        txn.set(&key(format!("{index:06}").as_bytes()), &vec![1; MAX_VALUE]);
    }
    assert_eq!(txn.commit().await, Err(CommitError::TooLarge));
}

pub async fn durable_across_reopen<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    put_all(&store, &[(b"a", b"1")]).await;
    let mut txn = store.begin().await.expect("begin");
    txn.set(&key(b"b"), b"2");
    txn.append_log(1, b"entry");
    txn.commit().await.expect("commit");
    let store = backend.reopen(store);
    assert_eq!(get(&store, b"a").await, Some(b"1".to_vec()));
    assert_eq!(get(&store, b"b").await, Some(b"2".to_vec()));
    assert_eq!(
        store.read_log(0, 9).await.expect("log"),
        vec![(1, b"entry".to_vec())]
    );
    assert_eq!(
        store.wait_past(0).await.expect("ok"),
        1,
        "a reopened store knows its latest version"
    );
}

pub async fn commit_and_resolve<B: Backend>(backend: &mut B) {
    let store = backend.fresh();
    let mut writes = WriteSet::default();
    writes
        .writes
        .push(KvWrite::Set(keys::head(), b"h1".to_vec()));
    writes.log.push((1, b"one".to_vec()));
    assert_eq!(
        inodedb_storage::commit(&store, None, &writes).await,
        CommitOutcome::Committed
    );
    assert_eq!(
        inodedb_storage::commit(&store, None, &writes).await,
        CommitOutcome::Fenced,
        "head no longer matches"
    );
    let mut next = WriteSet::default();
    next.writes.push(KvWrite::Set(keys::head(), b"h2".to_vec()));
    assert_eq!(
        inodedb_storage::commit(&store, Some(b"h1"), &next).await,
        CommitOutcome::Committed
    );
    assert_eq!(
        inodedb_storage::resolve(&store).await.expect("resolve"),
        Some(b"h2".to_vec())
    );
}
