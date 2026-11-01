//! How long writes stop when the writer dies and a standby takes over.
//!
//!     cargo run --release -p inodedb-tests --example failover [--features fdb] [-- ROUNDS]
//!
//! A writer and a standby share a store. Clients that know only the
//! standby's address commit as fast as answers come back, following
//! `NotWriter` to the writer; the writer is killed mid-load, and the gap
//! between the last answer before and the first answer after is the
//! failover time.

use inodedb::{Client, Op};
use inodedb_core::writer::Config;
use inodedb_storage::Storage;
use inodedb_storage_conformance::Backend;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const KEY: &str = "0000000000000001.0123456789abcdef";
const CLIENTS: usize = 8;

/// The time from the writer's death to the first commit after it, and the
/// longest a client went between answers.
async fn round<S: Storage + Clone>(store: S, config: &Config) -> (Duration, Duration) {
    let settings = || inodedb_server::Settings {
        writer: config.clone(),
        standby: true,
        ..inodedb_server::Settings::default()
    };
    let writer = inodedb_server::start_with(store.clone(), KEY, settings()).await;
    let db = Client::connect(writer.endpoint(), KEY)
        .await
        .expect("connects");
    db.commit((0..CLIENTS).map(|client| Op::mkdir(format!("c{client}"))))
        .await
        .expect("makes the directories");
    let standby = inodedb_server::start_with(store, KEY, settings()).await;
    let answers: Arc<Mutex<Vec<Instant>>> = Arc::default();
    let until = Arc::new(Mutex::new(None::<Instant>));
    let tasks: Vec<_> = (0..CLIENTS)
        .map(|client| {
            let (answers, until, endpoint) = (answers.clone(), until.clone(), standby.endpoint());
            tokio::spawn(async move {
                let db = Client::connect(endpoint, KEY).await.expect("connects");
                let (mut index, mut longest, mut last) = (0u64, Duration::ZERO, Instant::now());
                while until
                    .lock()
                    .expect("no panics")
                    .is_none_or(|until| Instant::now() < until)
                {
                    db.commit([Op::put(format!("c{client}/f{index}"), vec![7u8; 100])])
                        .await
                        .expect("commits");
                    let now = Instant::now();
                    longest = longest.max(now - last);
                    last = now;
                    answers.lock().expect("no panics").push(now);
                    index += 1;
                }
                longest
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let killed = Instant::now();
    drop(writer);
    let resumed = loop {
        tokio::time::sleep(Duration::from_millis(5)).await;
        if let Some(&first) = answers
            .lock()
            .expect("no panics")
            .iter()
            .find(|answer| **answer > killed + Duration::from_millis(5))
        {
            break first;
        }
    };
    *until.lock().expect("no panics") = Some(Instant::now() + Duration::from_millis(300));
    let mut longest = Duration::ZERO;
    for task in tasks {
        longest = longest.max(task.await.expect("joins"));
    }
    standby.stop().await;
    (resumed - killed, longest)
}

async fn measure<B: Backend>(name: &str, backend: &mut B, rounds: usize) {
    let configs = [
        ("default", Config::default()),
        (
            "600 ms lease",
            Config {
                writer_lease: 600,
                renew_every: 100,
                takeover_margin: 200,
                ..Config::default()
            },
        ),
    ];
    for (label, config) in &configs {
        let mut gaps = Vec::new();
        let mut longest = Duration::ZERO;
        for _ in 0..rounds {
            let (gap, client) = round(backend.fresh(), config).await;
            gaps.push(gap);
            longest = longest.max(client);
        }
        gaps.sort_unstable();
        println!(
            "| {name} | {label} | {} | {:.2} | {:.2} | {:.2} |",
            config.takeover_wait(),
            gaps[0].as_secs_f64(),
            gaps[gaps.len() / 2].as_secs_f64(),
            longest.as_secs_f64(),
        );
    }
}

#[tokio::main]
async fn main() {
    let rounds = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(3);
    println!(
        "| Backend | Lease | Takeover wait ms | Fastest s | Median s | Longest client gap s |"
    );
    println!("| --- | --- | ---: | ---: | ---: | ---: |");
    measure(
        "memory",
        &mut inodedb_storage_conformance::backends::Mem,
        rounds,
    )
    .await;
    #[cfg(feature = "fdb")]
    measure(
        "FoundationDB",
        &mut inodedb_storage_conformance::backends::Fdb::new(),
        rounds,
    )
    .await;
}
