#![cfg(feature = "fdb")]

use inodedb_storage_conformance::backends::Fdb;
use inodedb_storage_conformance::conformance;

conformance!(Fdb::new());
conformance!(@tests Fdb::new(); a_writers_handle_reads_from_its_latest_commit);

/// FoundationDB charges a write of a small key several times the key's
/// bytes: the key three times over when set, four when cleared, and a fixed
/// sum for each write. A transaction of small keys fits as
/// `WriteSet::charged` counts it, where the bytes of its keys and values
/// say little.
#[tokio::test(flavor = "multi_thread")]
async fn small_keys_fit_a_transaction_as_they_are_charged() {
    use inodedb_core::keys::{KvWrite, WriteSet};
    use inodedb_storage::{CommitError, Storage, Txn};
    use inodedb_storage_conformance::Backend;
    let mut backend = Fdb::new();
    let store = backend.fresh();
    let key = |index: usize| {
        let mut key = vec![0x02];
        key.extend_from_slice(&[7; 17]);
        key.extend_from_slice(&(index as u64).to_be_bytes());
        key
    };
    for clear in [false, true] {
        let writes = |count: usize| WriteSet {
            writes: (0..count)
                .map(|index| match clear {
                    false => KvWrite::Set(key(index), Vec::new()),
                    true => KvWrite::Clear(key(index)),
                })
                .collect(),
            log: Vec::new(),
        };
        let commit = |writes: WriteSet| {
            let store = store.clone();
            async move {
                let mut txn = store.begin().await.expect("begin");
                for write in &writes.writes {
                    match write {
                        KvWrite::Set(key, value) => txn.set(key, value),
                        KvWrite::Clear(key) => txn.clear(key),
                        KvWrite::ClearRange(range) => txn.clear_range(range.clone()),
                    }
                }
                txn.commit().await
            }
        };
        // Charged 6 MB, the writes commit under this store's longer
        // prefix too.
        let mut count = 10_000;
        while writes(count + 1_000).charged() <= 6_000_000 {
            count += 1_000;
        }
        assert!(writes(count).bytes() < 1_000_000);
        assert_eq!(commit(writes(count)).await, Ok(()), "{count} small keys");
        // Three times as many hold 3 MB of keys at most, and FoundationDB
        // refuses them.
        assert!(writes(3 * count).bytes() < 3_000_000);
        assert!(writes(3 * count).charged() > 10_000_000);
        assert_eq!(
            commit(writes(3 * count)).await,
            Err(CommitError::TooLarge),
            "{} small keys",
            3 * count
        );
    }
}

/// Prints what FoundationDB charges for a set and a clear of keys of a few
/// lengths, from the most of each one transaction takes; `BENCHMARKS.md`
/// records a run.
///
///     scripts/fdb cargo test --release -p inodedb-storage-conformance --features fdb --test fdb what_foundationdb_charges_small_keys -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement"]
async fn what_foundationdb_charges_small_keys() {
    use inodedb_storage::{Storage, Txn};
    use inodedb_storage_conformance::Backend;
    let mut backend = Fdb::new();
    let store = backend.fresh();
    let key_of = |len: usize, index: usize| {
        let mut key = vec![0x02];
        key.extend_from_slice(&(index as u64).to_be_bytes());
        key.resize(len, 7);
        key
    };
    println!("| Key bytes | A set | A clear |");
    println!("| ---: | ---: | ---: |");
    for len in [26usize, 34, 100] {
        let mut charge = [0, 0];
        for clear in [false, true] {
            let (mut fit, mut over) = (10_000usize, 200_000usize);
            while over - fit > 200 {
                let count = (fit + over) / 2;
                let mut txn = store.begin().await.expect("begin");
                for index in 0..count {
                    match clear {
                        false => txn.set(&key_of(len, index), b""),
                        true => txn.clear(&key_of(len, index)),
                    }
                }
                match txn.commit().await {
                    Ok(()) => fit = count,
                    Err(_) => over = count,
                }
            }
            charge[usize::from(clear)] = 10_000_000 / fit;
        }
        println!("| {len} | {} bytes | {} bytes |", charge[0], charge[1]);
    }
}
