# Development Plan

## Overarching Goal

Build the InodeDB that `DESIGN.md` describes: a transactional metadata store over a tree of inodes, with O(1) directory rename, per-op conditions, coherent caches, subtree keys, and freeing that waits for readers. One writer runs its logic as a deterministic core, proven in a simulator before it runs over gRPC and real storage. Each core feature lands with the simulator model and properties that test it. The server track runs the same core over SQLite, RocksDB and FoundationDB, and passes the storage and API conformance suites on each.

Non-goals until a phase names them: more than one writer, sharding, offline key attenuation, per-tenant release horizons, a storage engine of our own, enforcing POSIX permissions, symlinks, and writes through the FUSE view.

## Implementation Principles

- `DESIGN.md` is the design contract. A change that alters the design updates `DESIGN.md` in the same commit.
- The core does no I/O, reads no clocks and starts no threads (`AGENTS.md`). The server and the simulator carry out its actions.
- InodeDB's semantics live once, in `core`. Backends implement only the `Storage` trait.
- Every core feature ships with its simulator model, a property that fails on a wrong answer, and a planted bug that some layer catches.
- Build the smallest implementation that meets the phase gate. Add abstraction when a later phase needs it.
- Execution order: Phase 0, Phase 1, Phase 2, S1, Phase 3, Phase 4, Phase 5, Phase 6, S2, S3, Phase 7, Phase 8.

## Testing Strategy

- **Unit tests** for pure logic: key encoding, name and path validation, cache eviction, MAC handles.
- **Simulator:** `cargo test` runs a fixed seed range, and CI runs the commit hash as a seed. A phase closes with a 10,000-seed sweep and no failures; Phase 7 sweeps 100,000.
- **Planted bugs:** `scripts/mutants` applies known bugs to a scratch copy and reports which layer catches each. Every phase adds its own and must catch all of them.
- **Regression tests:** a bug the simulator finds becomes a test in `crates/sim/tests` that runs its seed, with the seed and failing commit in the commit message.
- **Storage conformance:** `storage-conformance` passes on every backend, from Phase 2 on.
- **API conformance:** the suite in `tests/` passes against a server on each backend, from S1 on.
- **POSIX differential:** the POSIX subset matches a real filesystem on Linux, from Phase 1 on.
- **Code review:** each phase ends with `/code-review high`. Findings are fixed or recorded in the phase ledger before the phase closes.

## Phase 0: Scaffold

Goal:
A workspace with a deterministic core, a simulator, planted-bug tooling and CI.

Scope:
- 0A Git repository and Cargo workspace (edition 2024, pinned toolchain) with `core` and `sim`.
- 0B `clippy.toml` in `core` and `sim` forbidding unordered collections, clocks and threads.
- 0C Simulator entry point: the PRNG (xoshiro256** through SplitMix64, with `Prng::stream`), seeds from numbers or commit hashes, `--seeds N` sweeps on every core, and a regression-seed test harness.
- 0D `scripts/mutants`, adapted from s3-accelerator.
- 0E CI: fmt, clippy with `-D warnings`, tests, and a simulator run seeded by the commit hash.

Out of scope:
- Any inode logic (Phase 1).

Completion gate:
CI passes on `main`.

Testing plan:
- PRNG stability and bounds unit tests; a mutants run against a trivial planted bug.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 0A: Git repository and workspace | `.git`, `Cargo.toml` (edition 2024), `rust-toolchain.toml` (1.98.0), `crates/core`, `crates/sim`. |
| Complete | Work | 0B: Clippy restrictions | `crates/core/clippy.toml`, `crates/sim/clippy.toml`; `cargo clippy --workspace --all-targets --features rocksdb -- -D warnings` is clean. |
| Complete | Work | 0C: Simulator entry point and PRNG | `crates/sim/src/{main,prng}.rs`; `prng::tests::{sequence_is_stable, below_stays_in_bounds, streams_are_independent}`; `cargo run --release -p inodedb-sim -- --seeds N`. |
| Complete | Work | 0D: Planted-bug tooling | `scripts/mutants`; "the PRNG ignores its bound" is caught by the sim tests. |
| Complete | Work | 0E: CI | `.github/workflows/ci.yml` on `github.com/danthegoodman1/inodedb` runs fmt, clippy, every test with RocksDB, `scripts/fdb`, and the commit hash's seed plus a 2,000-seed sweep from it. |
| Complete | Gate | CI passes on `main` | Run 41396101260 passed on 6a7cb2f. |
| Complete | Risk | RocksDB API tests abort at exit | Run 41384872814 on 9407362: `tests/tests/rocksdb.rs` passed, then printed `pthread lock: Invalid argument` and died of SIGABRT. A stack from an `LD_PRELOAD` shim: `rocksdb_close`, `UnlockFile`, `port::Mutex::Lock` on a Tokio worker that `shutdown_background` left running while `exit` destroyed RocksDB's static mutex. `inodedb_server::end_runtime` now joins the runtime's end on a thread of its own, before a dropped server returns. `api::dropping_a_server_closes_its_store`; M"a dropped server leaves its tasks running"; 500 runs of the test that ran last in the failed run, no aborts. |

## Phase 1: The Tree

Goal:
The core applies batches of POSIX-style ops to an inode tree in storage, one at a time, and the simulator checks every op's outcome against a naive path model.

Scope:
- 1A `storage` crate: the trait from `DESIGN.md` and the memory backend, without fault hooks.
- 1B Data model and key layout: inode records, entries, back-pointers, values under their versions, the root, the inode id counter, and name and path validation.
- 1C Sans-I/O execution: path resolution from a root inode; a batch run returns a write set and response, or the keys it lacks, and runs again once they arrive.
- 1D Ops: `Mkdir` (`Parents`, `IfAbsent`), `Put` (`Create`, `Replace`, `Upsert`, mtime), `Link`, `Unlink` (`IfExists`), `Rmdir`, `Rename` (`Replace`, `NoReplace`, `Exchange`, cycle checks in both directions, renames onto the same inode), `SetAttr`, POSIX directory mtime, and `Invalid` for ops on the caller's root.
- 1E Batches: per-op outcomes and cascades, conditions (path predicates and `Applied(i)`), `when`, `all_or_nothing`, batches that apply nothing taking no version, batch-level errors, and limits counting every derived write.
- 1F Versions and the log: the writer's u64 counter, one log entry per commit with effects carrying prior and new state, entries split across keys.
- 1G Free-queue entries written for freed inodes and replaced values (release comes in Phase 6).
- 1H `fsck` over storage.
- 1I Simulator: one writer over the memory backend; clients issuing random batches over a small namespace, so conditions and collisions are frequent; the naive model, which evaluates every op itself.
- 1J POSIX differential: mkdir, rename and its flags, link, unlink and rmdir, with their errors, against a real filesystem on Linux.

Out of scope:
- Group commit, faults, request ids (Phase 3).
- Caches and `Watch` (Phase 4).
- Keys and handles (Phase 5).
- `RemoveAll`, leases, release, snapshots (Phase 6).

Completion gate:
All scope items have tests; a 10,000-seed sweep passes; `scripts/mutants` catches every Phase 1 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests for key encoding and name validation.
- Scenario tests for each op's errors, each rename rule, and cascades through `Applied`.
- Simulator properties 1 (responses and op outcomes match the model at their version), 3 (a failed `when` or a skipped op in an `all_or_nothing` batch applies nothing) and 5 (`fsck` after every commit).
- Planted bugs: skipping the cycle check; skipping its reverse for `Exchange`; keeping a file's link count on unlink; an op that applies although its `Applied` dependency was skipped; an `all_or_nothing` batch applying part of itself; a rename onto another link to the same file unlinking it; a replaced value left out of the free queue.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Storage trait and memory backend | `crates/storage/src/{lib,memory,occ}.rs`. |
| Complete | Work | 1B: Data model and key layout | `crates/core/src/{keys,codec,types}.rs`; `keys::tests::{keys_parse_back, entries_scan_in_name_order_within_their_directory, prefix_end_carries}`, `types::tests::{names_follow_the_rules, paths_parse, records_round_trip}`, `codec::tests`. |
| Complete | Work | 1C: Sans-I/O execution and path resolution | `crates/core/src/{exec,view,overlay}.rs`; `ops.rs::a_restarted_writer_reads_what_it_needs`. |
| Complete | Work | 1D: Ops and rename rules | `crates/sim/tests/ops.rs`: `mkdir_reports_each_error`, `put_creates_replaces_and_frees_the_old_value`, `hardlinks_share_a_file_and_count_links`, `rmdir_needs_an_empty_directory`, `rename_follows_rename_2`, `renames_onto_the_same_file_change_nothing`, `exchange_checks_cycles_both_ways`, `directory_mtime_follows_its_entries`. |
| Complete | Work | 1E: Batch semantics and limits | `ops.rs`: `skips_cascade_through_state_and_applied`, `a_failed_when_applies_nothing`, `all_or_nothing_applies_nothing_when_any_op_skips`, `a_batch_that_changes_nothing_takes_no_version`, `malformed_batches_are_rejected`. |
| Complete | Work | 1F: Versions and log entries | `crates/core/src/log.rs`: one entry per commit with prior and new state and ancestor chains; `log::tests::entries_round_trip`. FoundationDB splits an entry across keys (`crates/storage-fdb`), and `storage-conformance::log_order_and_truncation` reads an entry of twice the value limit back whole on every backend. |
| Complete | Work | 1G: Free-queue entries | `keys::free_range`; `put_creates_replaces_and_frees_the_old_value`; checked by `fsck`. |
| Complete | Work | 1H: `fsck` | `crates/core/src/fsck.rs` checks directories, files, back-pointers, reachability, request records, keys, each entry against its directory's version, and the log (`LogCheck`: entries decode, run without gaps, end at the head), after every simulated commit and at each run's end. `crates/sim/tests/fsck.rs`: 29 tests, each breaking one invariant in a sound store. |
| Complete | Work | 1I: Simulator and naive model | `crates/sim/src/{lib,model,generate}.rs`; properties 1 and 5 checked on every seed. |
| Complete | Test | 1J: POSIX differential | `crates/sim/tests/posix.rs::inodedb_matches_a_real_filesystem` (400 seeds of 100 ops) passes under `scripts/posix` (Linux in Docker); CI's Linux job runs it with the workspace tests (0E). |
| Complete | Test | Planted bugs for Phase 1 | The seven in the testing plan are in `scripts/mutants`; every Phase 0–3 bug (36 of 36) was caught. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p inodedb-sim -- --seeds 10000`: 0 failures. |
| Complete | Gate | Code review | `/code-review high` on the design and the core; findings fixed, the scope checks on reads by handle in Phase 5, free-queue holders in Phase 6, and the gaps in 1F and 1H above. |

## Phase 2: Storage Contract and Local Backends

Goal:
The storage contract is a test suite, and the memory, SQLite and RocksDB backends pass it.

Scope:
- 2A `storage-conformance`: the contract's tests from `DESIGN.md`, generic over `Storage`.
- 2B Memory backend fault hooks: unknown outcomes, latency, crashes, and fence conflicts.
- 2C SQLite backend: rusqlite with SQLite bundled, WAL mode, `synchronous = FULL`, one writing process.
- 2D RocksDB backend behind the `rocksdb` feature: the log in its own column family, the WAL synced per commit, the lock file.
- 2E FoundationDB's limits enforced on every backend.
- 2F CI runs the suite on memory (faults off and on), SQLite and RocksDB.

Out of scope:
- FoundationDB (S2).

Completion gate:
The suite passes on all three backends in CI; the Phase 1 simulator tests still pass; `/code-review` findings are resolved.

Testing plan:
- The conformance suite itself, including close-and-reopen durability.
- Planted bugs: a scan whose end bound is inclusive; a limit left unenforced; a scan that misses a conflicting insert.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Conformance suite | `crates/storage-conformance/src/lib.rs`: 15 checks, from `byte_order` to `durable_across_reopen` and `commit_and_resolve`. |
| Complete | Work | 2B: Memory fault hooks | `memory::Fault::{UnknownApplied, UnknownDropped, Unavailable}`, each tested in `crates/storage-conformance/tests/memory.rs`. The simulator's driver supplies latency and crashes; fence conflicts come from the head compare (`reading_the_head_fences`). |
| Complete | Work | 2C: SQLite backend | `crates/storage-sqlite`; `crates/storage-conformance/tests/sqlite.rs`. |
| Complete | Work | 2D: RocksDB backend | `crates/storage-rocksdb`; `crates/storage-conformance/tests/rocksdb.rs` with `--features rocksdb`. |
| Complete | Work | 2E: Limits on every backend | `limits_hold` on all three backends. |
| Complete | Test | 2F: Conformance in CI | `ci.yml` runs `cargo test --workspace --features rocksdb` and `scripts/fdb`; run 41396101260 passed. |
| Complete | Test | Planted bugs for Phase 2 | "a scan includes its end key", "a transaction over the size limit commits", "a scan misses an insert into its range": all caught. |
| Complete | Gate | Code review | Reviewed with the core; fixes: a commit error after the write reports `Unknown`, and reads run in `spawn_blocking`. |

## Phase S1: gRPC Server and CLI

Goal:
The `inodedb` binary serves batches and reads over gRPC on SQLite or RocksDB, and the API conformance suite passes against it.

Scope:
- S1A `proto` crate: the service from `DESIGN.md`. RPCs later phases fill answer `Unimplemented`.
- S1B Server: tonic on tokio, a TOML config naming the backend, a writer task running batches one at a time, `Stat`, `Read`, and `List` with paging, a name prefix and optional attributes and values, all responses under 4 MiB.
- S1C A static admin key from the config, rooted at the tree's root, until Phase 5 replaces it.
- S1D `client` crate without a cache: connection, the batch builder with op references, reads.
- S1E `cli`: `ls`, `stat`, `put`, `mkdir`, `mv`, `rm`, reading `INODEDB_ENDPOINT` and `INODEDB_KEY`.
- S1F `tests/`: API conformance against a server on SQLite and on RocksDB, in CI.

Out of scope:
- Group commit and request ids (Phase 3), `Watch` (Phase 4), real keys (Phase 5), FoundationDB (S2).

Completion gate:
API conformance passes on both backends in CI; `/code-review` findings are resolved.

Testing plan:
- Server unit tests for config parsing and response paging.
- API conformance covering every op, every condition, cascades and `all_or_nothing`.
- Planted bugs: a listing page that drops its last entry; a response over 4 MiB; a request without the admin key accepted.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | S1A: Proto | `proto/inodedb.proto`, `crates/proto`; `Lease`, `ClaimFreed`, `AckFreed`, `MintKey` and `RevokeKey` answer `Unimplemented`. |
| Complete | Work | S1B: Server and writer task | `crates/server/src/{lib,service,engine,config,convert}.rs`; `config::tests::{parses_a_minimal_config, rejects_unknown_keys, rejects_unsafe_values}`; API checks `listings_page_by_count`, `listings_page_by_bytes`, `listings_filter`, `responses_fit_default_limits` on every backend. |
| Complete | Work | S1C: Static admin key | `crates/server/src/auth.rs`; `refuses_bad_requests`. |
| Complete | Work | S1D: Client without cache | `crates/client/src/{lib,ops,types}.rs`. |
| Complete | Work | S1E: CLI basics | `crates/cli` (`ls`, `stat`, `cat`, `put`, `mkdir`, `mv`, `rm`, `rmdir`, `ln`); `crates/cli/tests/cli.rs`. |
| Complete | Test | S1F: API conformance in CI | `tests/tests/{memory,sqlite,rocksdb}.rs` run `api_conformance!` in `ci.yml`; run 41396101260 passed. |
| Complete | Test | Planted bugs for S1 | "a listing page drops its last entry", "a listing page outgrows gRPC's default message limit", "a read answers more targets than a default message holds", "a request without a key is accepted", "a wrong key is accepted": all caught. |
| Complete | Gate | Code review | Review of the server, client and CLI; findings fixed. |

## Phase 3: Group Commit and Faults

Goal:
The writer commits batches in groups, and every acknowledged batch applies exactly once through storage faults, crashes and lost responses.

Scope:
- 3A Group commit in the core: one transaction in flight; batches run against committed state plus changes in flight; groups close at `group_max_bytes` and commit one at a time, in order; every group reads and sets the head record.
- 3B Answers: a batch hears back after its group commits; a batch that applies nothing waits for every group it was evaluated against.
- 3C Failed groups: the writer drops changes from the failed group on and runs those batches again.
- 3D `Unknown` outcomes resolved through the head record.
- 3E Request ids: version and result recorded for batches that apply anything, large results split across keys, 10-minute retention, retries answered from the record.
- 3F Restart: the writer resumes from the head record's version and the id counter.
- 3G Simulator faults: unknown outcomes, latency and crashes in storage; delay, loss, duplication, reordering and partitions on the network; writer crashes mid-group; clients retrying with request ids.
- 3H Server: group commit over SQLite and RocksDB; the client retries `Unavailable` and lost responses with the same request id.

Out of scope:
- Takeover by another server (S2).

Completion gate:
All scope items have tests; a 10,000-seed sweep passes; `scripts/mutants` catches every Phase 3 planted bug; `/code-review` findings are resolved.

Testing plan:
- Simulator properties 1 with real-time order, and 2 (exactly once).
- Scenarios: a group that fails mid-flight; an `Unknown` that committed and one that didn't; a group cut at `group_max_bytes`.
- Measurement: batches per second with group commit on SQLite and RocksDB, recorded in `BENCHMARKS.md`.
- Planted bugs: answering before the group commits; answering a batch that applied nothing early; dropping the request-id lookup; treating `Unknown` as failure; a second transaction in flight; keeping changes after a failed group.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Group commit | `crates/core/src/writer.rs` (`Group`, `MAX_GROUP_BYTES`); `groups.rs::batches_wait_for_their_group_and_groups_close_at_their_size`. |
| Complete | Work | 3B: Answer ordering | Held answers in `writer.rs`; planted bugs "a batch hears back before its group commits" and "a batch that changed nothing answers before the groups it saw commit" caught. |
| Complete | Work | 3C: Failed groups | `groups.rs::a_failed_group_runs_its_batches_again`. |
| Complete | Work | 3D: Unknown outcomes | `groups.rs`: `an_unknown_commit_that_applied_is_answered_as_committed`, `an_unknown_commit_that_did_not_apply_runs_again`, `a_resolve_that_finds_another_writers_commit_stops`. |
| Complete | Work | 3E: Request ids | `groups.rs`: `a_long_result_spans_record_parts`, `records_go_once_their_lifetime_ends`, `a_record_written_again_survives_its_old_records_deletion`, `a_retry_of_a_batch_that_changed_nothing_gets_the_first_answer`; retention is `request_ttl`, 10 minutes by default. |
| Complete | Work | 3F: Restart | `groups.rs::a_restarted_writer_answers_retries_from_records`; simulated writer crashes. |
| Complete | Work | 3G: Simulator faults | `crates/sim/src/lib.rs`: failed and unknown commits, stalls, loss, duplication, partitions, writer crashes, client retries; property 2 checked on every seed. |
| Complete | Work | 3H: Server group commit and client retries | `engine.rs` commit backoff; `client::Retry`; API checks `lost_answers_apply_once`, `retries_answer_once`, `concurrent_commits_all_apply`. |
| Complete | Test | Group commit measurement | `BENCHMARKS.md`, "Group commit". |
| Complete | Test | Planted bugs for Phase 3 | The testing plan's six plus five for request records and resolves: all caught. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p inodedb-sim -- --seeds 10000`, with faults and crashes: 0 failures. |
| Complete | Gate | Code review | A review of Phases 3 and 4 found ten defects, all fixed: a subscriber kept records behind a directory it lacked (`feed.rs::a_directory_that_returns_brings_nothing_stale`); the cached client filed a listing under the wrong directory, filled at the client's floor rather than the response's version, and lost non-UTF-8 names (`caches_follow_the_feed`); `List` pages and resumed listings re-resolved their path (`listings_follow_their_directory`); shutdown waited on open streams (`shutdown_ends_streams`); unreadable ops lacked the `malformed` tag (`refuses_bad_requests`); `Watch` could announce a version the writer had yet to record; unknown outcomes retried without backoff. |

## Phase 4: Changefeed and Caches

Goal:
Caches in the writer and in clients stay coherent through renames by applying the changefeed, and most reads cost no round trip.

Scope:
- 4A The cache in `core`: entries as a tree, negative entries, complete directories, inode records with back-pointers, values; budgets per kind of record, evicting what was written longest ago and sparing what reads in progress wrote.
- 4B The writer runs batches against its cache and reads storage only for misses.
- 4C `Watch`: effects routed by parent ancestry and back-pointers; `Entered` and removals for renames across the subtree's edge.
- 4D Truncation by `log.min_retention_ms`; `Compacted`; resync by paged listing and replay, skipping older effects.
- 4E Consistency: `Latest`, `AtLeast(v)` and `Cached`; every response carries its version; `wait_for` in the client.
- 4F Client cache kept current by `Watch`.
- 4G Server: the `Watch` RPC, and `wait_past` driving the tail.

Out of scope:
- Lease positions in truncation, and snapshots (Phase 6).

Completion gate:
All scope items have tests; a 10,000-seed sweep passes; `scripts/mutants` catches every Phase 4 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests for eviction order, sparing, and completeness lost with an evicted directory.
- Simulator: clients with caches, cut watch streams, truncation forcing resyncs; property 6 for caches, property 9 within retention.
- Measurement: cached path resolution for a six-level path, recorded in `BENCHMARKS.md`.
- Planted bugs: a cache keeping a name a commit removed; a negative entry kept after a create; an entered directory unreported, or keeping its cached listing; what leaves a subtree staying cached; a read moved forward across a forget; a read newer than the feed filled at once; chains before a commit taken from after it; compaction past the truncated log unreported; eviction dropping what a blocked batch read; forgetting the tree resetting the marks.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Cache | `crates/core/src/cache.rs` (`Budget`, `Queue`, `evict`); `cache::tests::{eviction_drops_what_was_written_longest_ago, eviction_spares_what_reads_in_progress_wrote, forgetting_the_tree_spares_reads_in_progress}`; `view::tests::layered_listings_match_merging_everything`. |
| Complete | Work | 4B: Writer against its cache | `groups.rs::a_warm_writer_reads_nothing` counts storage reads. |
| Complete | Work | 4C: `Watch` routing | `crates/core/src/feed.rs` (`route`, `Subscriber`), chains in `exec.rs` and `log.rs`; simulated subscribers on the root and on directories renamed in and out; API check `watch_streams_a_subtree`. |
| Complete | Work | 4D: Truncation and resync | `crates/server/src/feed.rs` (`truncate`, `Retention`); `watch_reports_compaction`; simulated cut streams (`feed_cut_percent`). |
| Complete | Work | 4E: Consistency levels | `Answer::NotYet` answered as `OutOfRange`; `Client::wait_for`; API check `floors_past_the_store_fail_fast`; simulated `at_least` floors. |
| Complete | Work | 4F: Client cache | `crates/client/src/cache.rs` (`Cached`); API check `caches_follow_the_feed`. |
| Complete | Work | 4G: Server `Watch` and tail | `crates/server/src/feed.rs::watch` with `wait_past`; API check `watch_replays_from_a_version`. |
| Complete | Test | Path resolution measurement | `BENCHMARKS.md`, "Path resolution". |
| Complete | Test | Planted bugs for Phase 4 | Eleven in `scripts/mutants`. A run of every mutant through Phase 5 caught 60 of 61; the miss, shutdown waiting on open streams, now keeps its sender alive and is caught by API conformance. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p inodedb-sim -- --seeds 10000`, with subscribers and small cache budgets: 0 failures. |
| Complete | Gate | Code review | A review of Phases 3 and 4 found ten defects, all fixed: a subscriber kept records behind a directory it lacked (`feed.rs::a_directory_that_returns_brings_nothing_stale`); the cached client filed a listing under the wrong directory, filled at the client's floor rather than the response's version, and lost non-UTF-8 names (`caches_follow_the_feed`); `List` pages and resumed listings re-resolved their path (`listings_follow_their_directory`); shutdown waited on open streams (`shutdown_ends_streams`); unreadable ops lacked the `malformed` tag (`refuses_bad_requests`); `Watch` could announce a version the writer had yet to record; unknown outcomes retried without backoff. |

## Phase 5: Keys and Handles

Goal:
Every request runs under a key rooted at a directory inode, and no key reads or writes outside its root, by path or by handle.

Scope:
- 5A Key records: hashed secret, root inode, grants, expiry, minting key.
- 5B `inodedb --init-key`, `MintKey` and `RevokeKey` with cascading revocation, `idb key mint` and `idb key revoke`; grants enforced per RPC; S1's static key removed.
- 5C Roots follow renames; a commit that frees a key's root, or detaches a directory at or above it, revokes the key and those minted from it.
- 5D Handles: a MAC over key and inode under a secret in storage; reads by handle checked against the key's root through back-pointers and parent ids.
- 5E `Watch` shows only the subscriber's tree, with handles for its key.

Out of scope:
- Scope checks on freed inodes (Phase 6); until then a file with no links is readable by handle only under keys rooted at the tree's root.

Completion gate:
All scope items have tests; a 10,000-seed sweep passes; `scripts/mutants` catches every Phase 5 planted bug; `/code-review` findings are resolved.

Testing plan:
- Simulator: keys with nested and disjoint roots, and renames moving directories across them; property 8.
- Measurement: the scope check on a read by handle, recorded in `BENCHMARKS.md`, against the design's estimate of about a microsecond.
- Planted bugs: a read by handle that skips the scope check; a wrong secret, an expired key, or a key revoked ahead of a batch accepted; a handle's MAC unchecked; a minted key outliving its minter; a key revoking keys it never minted; revocation leaving minted keys; a freed or detached root leaving its key; a stream outliving its key; `Link` resolving `to` from the tree's root.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: Key records | `crates/core/src/auth.rs` (`KeyRecord`, `Keys`, `authorize`, `MAX_KEYS`); `fsck::check_keys`; `keys.rs::keys_survive_a_restart`. |
| Complete | Work | 5B: Bootstrap, minting, revocation, grants | `--init-key` (`crates/server/src/lib.rs::init_key`); `MintKey` and `RevokeKey` in `service.rs`; `idb key mint` and `idb key revoke` with seven CLI tests; `keys.rs::grants_bound_what_a_key_does`; API check `keys_bound_their_reach`. |
| Complete | Work | 5C: Roots across renames and frees | `keys.rs::{a_key_follows_its_root_through_renames, freeing_or_detaching_a_root_revokes_its_keys, revoking_a_key_revokes_what_it_minted, a_revoked_id_never_mints_again}`. |
| Complete | Work | 5D: Handles and scope checks | HMAC handles in `crates/server/src/auth.rs`; `keys.rs::{a_read_by_handle_stays_within_the_key, a_freed_file_stays_within_the_keys_that_held_it}`; simulated keyed clients check property 8 (`keyed_percent`). |
| Complete | Work | 5E: Scoped `Watch` | `feed::route` takes the key's root; API checks `watch_stays_in_its_keys_tree` and `watch_ends_with_its_key`. |
| Complete | Test | Scope check measurement | `BENCHMARKS.md`, "Path resolution": a stat by handle under a key five levels up, and making and checking a handle. |
| Complete | Test | Planted bugs for Phase 5 | The testing plan's bugs and the review's, in `scripts/mutants`, all caught in the run through Phase 5. |
| Complete | Gate | 10,000-seed sweep | `inodedb-sim 0 --seeds 10000` with keyed clients: 0 failures. |
| Complete | Gate | Code review | Six findings, all fixed: a `Watch` from past the version its key was checked at kept streaming after revocation, so `Watch` refuses such a `from`; a `Watch` followed its directory out of its key's tree and replayed what happened before it entered, so `route` filters by the key's root and the stream ends (`watch_stays_in_its_keys_tree`); `--init-key` re-minted a revoked bootstrap key, so revoked ids keep tombstones (`a_revoked_id_never_mints_again`); revocation cost grew with the square of the keys, so key indexes make it linear, 64,000 keys in 52 ms (`BENCHMARKS.md`, "Revocation"; `revoking_a_key_revokes_thousands_minted_from_it`), with `MAX_KEYS` and a size check on key groups; a request id answered any key that sent it, so ids are scoped to keys (`a_request_id_answers_only_its_own_key`); the cached client kept answering after its key was refused, so it stops (`caches_stop_with_their_key`). |

## Phase 6: Leases, Freeing and Snapshots

Goal:
Freed inodes and replaced values outlive every reader that might use them, removing a subtree is O(1), and every read through a snapshot sees one version.

Scope:
- 6A Free-queue entries record the directories above the inode; records stay until acknowledged.
- 6B Leases: the `Lease` stream, heartbeats, `lease_ttl`, an oldest version that only moves forward, `LeaseExpired`, `TooOld` below the horizon, persistence in the next group, fresh TTLs when a writer starts.
- 6C Release and claiming: the horizon, `ClaimFreed` scoped by recorded ancestors, `AckFreed`, `claim_ttl`.
- 6D Reads by handle of freed inodes and old value versions until acknowledged, checked against recorded ancestors.
- 6E `RemoveAll`: the detach commit, revocation of keys rooted inside, background freeing in transactions of at most 1,000 inodes, links outside the subtree kept.
- 6F Truncation by live leases' positions.
- 6G Snapshots in the client: undo records, the read rule, misses waiting for the feed, old values by handle and version, `undo_bytes` and `SnapshotTooOld`.

Out of scope:
- Per-tenant release horizons (open question).

Completion gate:
All scope items have tests; a 10,000-seed sweep passes; `scripts/mutants` catches every Phase 6 planted bug; `/code-review` findings are resolved.

Testing plan:
- Simulator: replicas with long snapshots, consumers that claim entries and delete from a model of an external store, partitions that expire leases; properties 6 (snapshots), 7 and 9.
- Planted bugs: releasing below a lease; claiming outside the consumer's root; a snapshot that ignores its undo records; deleting a record before its entry is acknowledged; accepting a heartbeat that lowers the oldest version; a detach that leaves keys rooted inside it working.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A: Free-queue ancestors | `exec.rs::free` records them, `delta.rs::encode_ancestors` stores them; `fsck` requires every free entry and detach record to name the root; `leases.rs::freed_records_wait_for_every_lease`. |
| Complete | Work | 6B: Leases | `crates/core/src/lease.rs`; the `Lease` stream in `service.rs`; `leases.rs::{a_lease_starts_at_the_horizon_and_runs_out, a_lease_survives_its_writer}`; API check `leases_hold_freed_records`. The client's `Lease` resumes on a new stream and releases when dropped (`caches_hold_a_lease`). Simulated subscribers hold leases through heartbeat gaps (`lease_gap_percent`) and writer crashes. |
| Complete | Work | 6C: Release and claiming | `leases.rs::{claims_stay_under_the_consumers_root, a_claim_that_runs_out_returns_its_entries, a_replaced_value_goes_alone}`; the simulated consumer claims, lets claims lapse, and acknowledges, and every claim checks property 7. |
| Complete | Work | 6D: Reads of freed inodes and old values | `keys.rs::a_freed_file_stays_within_the_keys_that_held_it`; API checks `leases_hold_freed_records` and `snapshots_hold_their_version`, which reads an unlinked file through a snapshot. |
| Complete | Work | 6E: `RemoveAll` and background freeing | `exec.rs::{execute_reap, Exec::reap, Exec::prefetch}`; `ops.rs::{remove_all_detaches_a_subtree_then_frees_it, a_large_subtree_frees_in_bounded_commits}`, which frees 2,803 inodes from a cold cache in 4 commits and 14 storage reads; detach records hold the subtree's ancestors (`seeds.rs::seed_403`); eviction spares the reaper's reads (`seeds.rs::seed_130`). |
| Complete | Work | 6F: Truncation by lease positions | `Retention::keeps` and `feed::truncate` in `crates/server/src/feed.rs`, with `log.max_retention_ms`; `feed::tests::leases_keep_what_their_positions_need_up_to_the_maximum`; API check `leases_hold_the_log`; the simulator checks the writer's lease position against every live lease (property 9). |
| Complete | Work | 6G: Snapshots | `crates/core/src/snapshot.rs` and `Subscriber::run_at`; `snapshots.rs`, five scenarios; the client's `Snapshot` (API checks `snapshots_hold_their_version` and `snapshots_hold_what_they_read`); simulated subscribers read through snapshots and check each answer against the model at the snapshot's version (property 6). |
| Complete | Test | Planted bugs for Phase 6 | Twenty-eight in `scripts/mutants`, from "the horizon passes a live lease" to "a lease opens at the version the cache read", all caught: the sixteen added or changed for the review in one run, its two misses after `ops.rs::a_wide_directory_frees_over_several_commits` and the `slow_lease_opens` fault in `caches_start_under_steady_writes`. |
| Complete | Gate | 10,000-seed sweep | `inodedb-sim 0 --seeds 10000` and 10,000 seeds from a random start pass on the reviewed code. |
| Complete | Gate | Code review | Twelve findings, each fixed: (1) lease ids reused after a restart: the release record `s z` stores the next id; (2) a new writer's horizon fell back to stale lease records: the release record stores the horizon, and claims use only the stored one; (3) a snapshot past `undo_bytes` stopped pinning its lease: an ended snapshot pins until dropped; (4) a claim lost a replaced value once its file's record went: `Query::Kept` reads the kept value directly (`claims_read_values_their_file_outlived`); (5) each reap step listed a whole directory: listings stop at the step's remaining budget; (6) every request scanned every lease: `Leases` indexes oldest versions, positions and expiries; (7) every claim scanned the free queue from its start: claims resume from a cursor per root; (8) MAC pruning stopped while a snapshot was open: it keeps what the cache holds or an undo record names; (9) retried acknowledgements failed, and claim ids repeated across writers: acknowledged claims linger until they expire, and ids start from the writer's id; (10) reaping starved under steady load: it runs every 100 ms regardless; (11) reaps routed through the detached top's stale parent: detach records hold the ancestors at detach time; (12) heartbeats stopped while the Watch reconnected: a heartbeat task runs apart from the stream. |

## Phase S2: FoundationDB and Takeover

Goal:
InodeDB runs on FoundationDB with one fenced writer and warm standbys, and a takeover loses no acknowledged batch.

Scope:
- S2A `storage-fdb` behind the `fdb` feature: the log under `l` keys, `wait_past` through a watch on the head record, the limits; `scripts/fdb` starts a FoundationDB container.
- S2B The writer record: epoch, address, a lease renewed every second; takeover after `writer_lease` plus the margin; every transaction checks the epoch.
- S2C The old writer stops answering `writer_lease` after the start of its last successful renewal.
- S2D Standbys: tail the log, serve `AtLeast(v)` and `Watch`, answer the rest with `NotWriter`, take over with a warm cache.
- S2E Client: follows `NotWriter` to the address it names.
- S2F Simulator: takeovers while the old writer still runs, clock rate drift within the margin, fence conflicts from the memory backend.
- S2G `deploy/kubernetes`: a writer and standbys as a StatefulSet.

Out of scope:
- More than one writer.

Completion gate:
Storage and API conformance pass on FoundationDB in CI; a 10,000-seed sweep with takeovers passes; `scripts/mutants` catches every S2 planted bug; `/code-review` findings are resolved.

Testing plan:
- Simulator property 4, and property 2 across takeovers.
- An API test that kills the writer mid-group on FoundationDB and checks every acknowledged batch after takeover.
- Measurement: failover time and group commit rate on FoundationDB, recorded in `BENCHMARKS.md`.
- Planted bugs: skipping the epoch check; an old writer answering `Latest` after its lease; a takeover without the margin; a standby accepting a write.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | S2A: FoundationDB backend | `crates/storage-fdb` (prefix layout, chunked log, newest-version key for `wait_past`, a read round's keys fetched at once); `scripts/fdb` runs storage conformance (13 checks) and API conformance (42 checks, the standby checks among them) against FoundationDB 7.3.79 in a container, all passing. CI run 41335409728 on `danthegoodman1/inodedb` (commit `7e667fa`) passed every job: `test` (fmt, clippy, and every test on memory, SQLite and RocksDB, the POSIX test among them), `foundationdb` (both conformance suites and the mount test) and `simulate` (2,000 seeds). |
| Complete | Work | S2B: Writer record and fencing | The head record is the writer record (`writer.rs::Head`: id, epoch, beat, address); every commit expects the head its writer last wrote; a starting writer commits its claim before it answers (`Writer::started`, `send_claim`, `claimed`); idle writers renew every `renew_every` (`send_chore`); `standby.rs::Successor`; scenarios `standby.rs::{a_standby_takes_over_with_its_cache, a_standby_waits_while_the_writer_renews}`; storage conformance's head check. |
| Complete | Work | S2C: Fenced reads | `Writer::is_fresh` gates queries (`stale`), batches that changed nothing (`hold`), heartbeats and releases; `standby.rs::a_standby_takes_over_only_after_the_writers_lease_on_drifting_clocks` checks the margin against clocks 9% apart each way; the simulator checks property 4 on every answer (`Simulator::answer`, `landed`). |
| Complete | Work | S2D: Standbys | `standby.rs::Follower` (a cache of the whole tree fed by `Change::whole`, `AtLeast` reads, `NotWriter` for the newest state and for keys newer than its log, `hand_over` to a warm writer); the engine's `Role::{Follow, Write}` in `crates/server/src/engine.rs`, which polls the head every 100 ms, follows the log, takes over, and follows again once fenced; truncation runs only while writing within the lease; API checks `standbys_serve_reads_and_send_writes_on` and `takeovers_keep_every_acknowledged_batch` on memory and FoundationDB; the simulator's standbys run followers that answer reads and take over warm (`standby::a_standby_follows_links_through_renames`). |
| Complete | Work | S2E: Client redirect | `Client::redirect` follows `NotWriter` for every clone; `go_home` returns to the configured endpoint after a retriable failure; lease streams redirect too; connection failures that arrive as `Internal` retry (`transport`). Checked by both standby API checks. |
| Complete | Work | S2F: Takeover simulation | `crates/sim/src/lib.rs`: writers by generation with clocks drifting up to 9% (drawn at the extremes); standbys whose followers read the log, answer half the reads at a version storage holds, send unknown keys on to the writer, and hand their caches and waiting reads to the writers they start; storage stalls long enough for a takeover while the old writer runs; stats by inode, inside and outside keys' roots; clients that stay with the writer they reached; read-only clients; crashes that restart at once only when the dead writer held the store. Property 4 covers every answer drawn from a writer's state. |
| Complete | Work | S2G: Kubernetes example | `deploy/kubernetes/{Dockerfile, fdb.yaml, inodedb.yaml, README.md}`. Run on kind: three servers raced for the empty store and one won; writes through the service; the writer pod force-deleted; the next write landed on its first try 7.5 s later through the standby that took over; the restarted pod rejoined as a standby. |
| Complete | Test | Kill-mid-group API test | `tests/src/lib.rs::takeovers_keep_every_acknowledged_batch`: four clients commit through the standby's address, the writer's runtime is shut down mid-load (every task it started ends, as in a process kill), and every acknowledged batch is present once after the standby takes over; passes on memory and FoundationDB. |
| Complete | Test | Failover and commit rate measurement | `BENCHMARKS.md` "Failover" (6.1 s at the default lease, 0.9 s at 600 ms, on memory and FoundationDB) and FoundationDB rows in "Group commit" (about 5 ms a commit; grouping multiplies throughput by 46 at 128 clients). |
| Complete | Test | Planted bugs for S2 | Eleven in `scripts/mutants`, from "a commit skips the head check" to "a standby refuses a key newer than the log it has read"; all caught in the full run under Phase 7. |
| Complete | Gate | 10,000-seed sweep with takeovers | `inodedb-sim 0 --seeds 10000` and `inodedb-sim 7392011 --seeds 10000`: no failures. |
| Complete | Gate | Code review | A review of the fencing, takeover and standby code found twelve issues, all resolved: back-pointers a feed left stale, which let a standby read across a key's root and a warm writer leave a back-pointer behind (`cache.rs::relink`, also in `rebase`); wall-clock steps stretching leases (`engine.rs::now`, a monotonic timer from the wall time); keys refused while a role starts (`Unavailable`) or unknown to a standby (`NotWriter`); a restarting standby dropping its waiting reads; groups sized without heartbeats (`LEASE_BYTES`); a standby dropping reads that straddle any removal (whole-tree feeds keep no forget barrier) and rescanning waiting reads (`Waiting::reading`); truncation racing a role switch (one `Keep` watch, checked again before deleting); claims and acknowledgments without the lease gate; a zero takeover margin, an unreachable advertised address, readiness before serving (the listener binds once the engine is ready); stale comments; and an acknowledged file's value kept by a standby. Releasing the writer lease on a graceful shutdown stays unbuilt (`EVIDENCE.md`, "Gaps"). |

## Phase S3: Operations Tooling

Goal:
Operators can inspect, check and browse a tree from the command line.

Scope:
- S3A `idb fsck` against a live server or a backend directly.
- S3B The rest of the CLI in `DESIGN.md`: `watch`, `rm -r`, `key`.
- S3C `idb mount`: a read-only FUSE view on Linux and macOS.

Out of scope:
- Writes through FUSE.

Completion gate:
Every command in `DESIGN.md`'s CLI section exists with a test; `/code-review` findings are resolved.

Testing plan:
- CLI tests per command; `fsck` against a backend with a planted corruption; a FUSE test that lists and reads a tree on Linux in CI.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | S3A: `idb fsck` | The `Check` call (`proto/inodedb.proto`, `service.rs::check`, root admin keys only) over `inodedb_storage::dump` and the log, at most 1,000 problems, run once with a 30-minute deadline (`Client::check`); and `idb fsck --sqlite PATH` against an existing store directly (`Sqlite::open_existing`); `cli.rs::{fsck_finds_a_planted_corruption, fsck_reads_a_sqlite_store_directly}` and `api::checks_take_a_root_admin_key`. |
| Complete | Work | S3B: Remaining CLI commands | `idb watch` over the new `Client::watch`, printing paths by each inode's current names (`cli.rs::watch_prints_changes_as_paths`, through a directory rename and a removed link); `rm -r`, `key mint` and `key revoke` (`cli.rs::{rm_rmdir_and_ln, keys_mint_and_revoke}`); every command in `DESIGN.md`'s CLI section has a test. |
| Complete | Work | S3C: `idb mount` | `crates/cli/src/mount.rs` behind the `mount` feature (`fuser`, no C library on Linux): lookups and listings go by handle, an open directory lists once and an open file reads once, and calls give up after 5 seconds; `tests/mount.rs::a_mount_lists_and_reads_the_tree` lists and reads through the kernel, is refused a write, sees a later commit, lists an open directory after it is renamed and replaced, lists 3,000 names once each, and unmounts, run by `scripts/fdb cargo test -p inodedb-cli --features mount` and CI's `foundationdb` job. macOS needs macFUSE, which this machine lacks, so the feature is untested there. |
| Complete | Gate | Code review | A review of the CLI, `Check`, mount and the footprint benchmark found ten issues, all resolved: `Check` restarting its dump on timeout; `readdir` relisting a directory per call and paging by index; the mount resolving stale paths; `idb watch` printing stale paths; `fsck --sqlite` creating a store at a wrong path; an unbounded `Check` response; no test of `Check`'s root gate; `idb fsck` skipping the log; the mount blocking for two minutes on an outage; and the footprint's undo row echoing a constant, which hid that undo records held 288 bytes against 64 charged (slots now one queue: 162 bytes, charged 160). |

## Phase 7: Design Reconciliation

Goal:
`DESIGN.md` and the code agree, every behavior it names has a test and a planted bug, and the defaults rest on measurements.

Scope:
- 7A Walk each `DESIGN.md` section and record the test, property or planted bug behind each behavior.
- 7B Set defaults from measurements: writer lease, lease TTL, log retention, request-id retention, `group_max_bytes`, `undo_bytes`, cache budgets.
- 7C Resolve or restate each open question.

Completion gate:
Every behavior maps to evidence; a 100,000-seed sweep passes; `/code-review` findings are resolved.

Testing plan:
- A 100,000-seed sweep; a full `scripts/mutants` run.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 7A: Behavior-to-evidence map | `EVIDENCE.md` maps each behavior to its tests, properties and planted bugs, and lists the gaps left. The walk found fourteen places `DESIGN.md` and the code disagreed, now agreeing: CI's backends (FoundationDB and mount added to `ci.yml`, with a 2,000-seed sweep), the memory backend's hooks, the 5-second limit, `fsck`'s log check (built), the PRNG module, settings (`[cache]`, `writer.lease_ttl_ms` and `claim_ttl_ms`, `Client::cached_within`), request id zero, randomness into the core, the simulator's standbys (built) and storage faults, the oracle's independence, the POSIX test's filesystem, property 9's wording, and stream endings. `server/config::defaults_match_the_design` pins every default. |
| Complete | Work | 7B: Measured defaults | `BENCHMARKS.md` "Failover", "Footprint" and FoundationDB "Group commit"; `DESIGN.md` "Defaults" gives each default and its basis. The measurements moved two: the cache's inode budget fell from 4 Mi to 1 Mi and its directories from 1 Mi to 256 Ki, since a cached file costs 830 bytes; and the writer keeps only live request records' ids, 98 bytes each rather than 231 (`cache.rs::apply`). |
| Complete | Work | 7C: Open questions | `DESIGN.md` "Open questions": the defaults moved to "Defaults"; per-tenant horizons, offline attenuation, the ClickHouse client and scale beyond one writer stay open, each with its trade-off. |
| Complete | Gate | 100,000-seed sweep | `cargo run --release -p inodedb-sim -- 0 --seeds 100000`: no failures, on the final code. Its first run, before the last two fixes, failed two seeds, now regressions: `seeds::seed_97619` (a successor writer expired request records by other writers' clocks, so a retry ran twice) and `seeds::seed_75494` (a subscriber kept a removed directory's files cached, and one with a link elsewhere came back stale). |
| Complete | Gate | Full mutants run | `scripts/mutants`: all 116 caught. The latest full run caught 115; the miss, a starting writer expiring records by other writers' clocks, had been caught only by `seeds::seed_97619`, whose run read-ahead had changed, and `groups::a_starting_writer_keeps_records_a_lifetime_by_its_own_clock` now catches it (`--only`). Read-ahead likewise left `seeds::seed_541` short of the detached-subtree scope bug, which `groups::a_detached_subtree_stays_within_the_directories_above_it_as_it_detached` now catches. |
| Complete | Test | Operations through the API | `tests/examples/ops.rs` on an EC2 i7ie.6xlarge with every store on local NVMe and on gp3 at 80,000 IOPS (`scripts/ec2/{setup,bench}`); `BENCHMARKS.md` "Devices", "Operations" and "Group commit" give each result. Measuring found two slow paths, now fixed with a test and a planted bug each: listings waiting 40 ms on Nagle's algorithm (`server/lib::accepted_connections_send_at_once`), and batches behind a cache miss reading one round at a time (`groups::queued_batches_that_miss_the_cache_read_together`), which with FoundationDB's one-key-at-a-time reads held new-file puts to 445 a second at 64 clients. A Codex review of the fixes found read-ahead re-running cached batches each round, now capped at 1,024 ops a round (`groups::reading_ahead_runs_a_bounded_number_of_ops_a_round`). |
| Complete | Gate | Code review | A review of this phase's changes found no wrong answer in the core's changes and ten issues elsewhere, resolved: a standby with a small cache evicting what its reads fetched, so they never answered (`Subscriber::spare`); the simulator accepting any stat of an unnamed inode, which once tightened (the model records the directories above each name a node lost) found a scope leak in the core, now fixed: a read by inode inside a removed subtree walked up through the top's old parent, wherever it had moved (`query.rs::detached_within` over the detach record, `seeds::seed_541`); a monotonic timer that stops while the host sleeps (the writer now takes the later of it and the wall clock); a value under a version not yet reached cached as absent; a claim cursor advanced by a claim that answered `Unavailable`; shutdown ignored while the engine starts; a tail truncated during a check reported as missing; redirects repeated without backoff; `open_existing`'s doc; and TTLs past `u64` milliseconds. The small cache, the detached scope and the value version each have a test the unfixed code fails and a planted bug; the other fixes have no test, since they need a sleeping host, a race with truncation or misdirected servers. |

## Phase S4: Performance

Goal:
Cut storage round trips, wire bytes and failover time where measurements show them, without weakening any guarantee.

Scope:
- S4A Listings answered without a task for one page, each handle made once.
- S4B Handle MACs as one AES block, and each handle sent once.
- S4C A writer's storage handle; FoundationDB's reuses its latest commit's read version.
- S4D A tail of decoded log entries in memory for streams.
- S4E SQLite checkpoints on a thread of the store's own.
- S4F A clean shutdown releases the store to a standby.
- S4G A startup preload of the tree into the cache, entries first, up to its budget.
- S4J Cache budgets in bytes, each record charged at least what it holds, and SIEVE eviction.
- S4H Warm reads answered on the request's task, and a profile of what sets their ceiling.
- S4I A trace of one-client commit latency on Linux, and the fixes it names.
- S4K A gRPC transport over `h2` for the server and the client.
- S4L Bursts of engine commands, and groups committed from their write sets.
- S4M WAL files written at open and overwritten from then on, on SQLite and RocksDB.
- S4N A preload of values, and on standbys.
- S4O Shallow idle states held by the server, and mimalloc.
- S4P FoundationDB groups that skip the read of the head, and benchmarks with one process per role.

Out of scope:
- Merging cold reads across queries: with read versions reused, each FoundationDB round costs one trip, and merging would trade query latency for fewer transactions.

Completion gate:
Each change lands with tests and planted bugs, and `BENCHMARKS.md` reports its effect on the EC2 machine.

Testing plan:
- Storage conformance on every backend, `scripts/fdb`, simulator sweeps, `scripts/mutants`, and `tests/examples/ops.rs` before and after.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | S4A: One-page listings, handles made once | `crates/server/src/service.rs` `Pages`; M"a listing page drops its last entry" and the other listing mutants caught by API conformance; 100-entry listings about 30% faster on the Mac. |
| Complete | Work | S4B: AES handle MACs, handles sent once | `server/auth::a_handle_mac_is_one_aes_block` (checked against OpenSSL), `api::inodes_share_their_handle`; four mutants caught; `BENCHMARKS.md` "Path resolution": 30 ns to make and check, from 146 ns. |
| Complete | Work | S4C: Writer's handle, FoundationDB read versions | `sc::a_writers_handle_reads_its_commits` on every backend, `sc::a_writers_handle_reads_from_its_latest_commit` and the API suite on FoundationDB (`scripts/fdb`); the simulator's pinned reads over 10,000 seeds. On the Mac's FoundationDB container, puts went from 170/s to 330/s for one client and 7,000/s to 10,100/s for 64, cold stats from 9,500-14,400/s to 21,700-22,600/s. `scripts/mutants --fdb` holds eight planted bugs, all caught on a cluster. |
| Complete | Work | S4D: Stream tail | `server/feed::{the_tail_holds_the_newest_young_entries_without_gaps, the_tail_drops_its_oldest_entries_past_its_bound, a_stream_that_keeps_up_reads_no_storage}`, `server/engine::a_standbys_reads_fill_its_tail`, `api::streams_that_keep_up_read_no_log`; five mutants. |
| Complete | Work | S4E: SQLite checkpoint thread | `sqlite::a_thread_checkpoints_the_wal`; M"commits checkpoint the WAL themselves", M"commits never wake the checkpointing thread". On the Mac, puts from 64 clients went from 20,000-20,500/s to 26,900-27,000/s, their p99 from 10.2-10.4 ms to 3.4-3.9 ms; one client's puts slowed from 5,400/s to 4,800/s. On gp3, S4M's written WAL takes one client's put to 1.12 ms and 64 clients' to 11,972/s. |
| Complete | Work | S4F: Release on clean shutdown | `standby::a_released_store_passes_to_a_standby_at_once`, `core/standby::a_standby_takes_over_a_released_head_at_once`, `api-sb::a_clean_stop_hands_the_store_to_a_standby`; clean shutdowns in the swarm over 10,000 seeds; four mutants. |
| Complete | Work | S4G: Startup preload | `crates/core/src/preload.rs`; `preload::*` scripted scenarios, `api::preloaded_servers_answer_without_reads` on every backend, preloading writers in the swarm over 10,000 seeds; ten mutants caught. Entries load first and the preload stops at the first record the budget has no room for: in `preload::a_preload_stops_at_the_cache_budget_with_the_entries_first`, a tree whose inode records outgrow the budget resolves a path from the cache and reads only the file's record. `cache.preload` is on by default. On the Mac, a tree of 10,231 inodes preloads in 5-9 ms; 16 clients' first stats of 10,000 files run at 92,000-99,000/s as a preloading server starts, against 40,000-45,000/s cold on SQLite and RocksDB; one client's puts into a complete directory gain about 12%. S4N adds values and standbys. |
| Complete | Work | S4J: Byte budgets and SIEVE eviction | `crates/core/src/cache.rs` (`Budget::tree_bytes`, the charges, `Queue::next`); `core/cache::*` unit tests, `cache::records_reads_use_stay_cached_as_new_files_pass_through`, small tree budgets in the swarm over 10,000 seeds; five mutants caught. `BENCHMARKS.md` "Footprint": each record's charge covers what the allocator holds for it, and a cached empty file holds 910 bytes and is charged 983. On the Mac, `tests/examples/ops.rs` runs within noise of the commit before on memory, SQLite and RocksDB. The change exposed that M"a removed directory leaves what lies beneath it cached" rested on `seeds::seed_75494` alone, whose run the new eviction moved; `feed::a_file_beneath_a_removed_directory_is_read_again_when_it_returns` now scripts it. |
| Complete | Work | S4H: Warm reads on the request's task | A profile of 64 clients' stats on 05703a0: the engine's task took 3.7% of samples, and HTTP/2's layers over half of each side, so the engine set no ceiling. `Writer::answer_cached` and `Follower::answer_cached`, run under a shared lock by `Engine::query`, make the same checks as the engine: lease, floor, key, stopped and releasing writers. `server/engine::a_read_the_cache_holds_needs_no_engine_task`, half the swarm's reads through `answer_cached`, ten mutants. With S4K, on EC2 local NVMe: 64 clients' stats from 223,000/s to 438,000/s, one client's p50 from 49 µs to 23 µs, listings from about 35,000/s to about 175,000/s. |
| Complete | Test | S4I: One-client commit trace | The 1.35-1.6 ms on EC2 was idle-state wake-ups at each thread hop (about 92 µs a hop), XFS's journal at each sync, and two trips to a blocking thread. What the plan called a 0.32 ms device sync measured extent conversion: overwriting written blocks takes 17 µs on NVMe and 668 µs on gp3 (`BENCHMARKS.md` "Devices"). With S4L, S4M and S4O, one client's put takes 0.090 ms on RocksDB and 0.172 ms on SQLite on NVMe, and 0.758 ms and 1.124 ms on gp3. |
| Complete | Work | S4K: Transport | `crates/transport`: a reply's headers and data in one write, a request whose answer is ready polled inline, reconnection, attempt timeouts, keepalive. `transport::*`, `api::{a_stream_ends_when_its_client_leaves, a_stream_outlives_its_client_handle, clients_reconnect_to_a_restarted_server, lease_streams_send_every_beat}`; six mutants, with the transport's tests as a layer. |
| Complete | Work | S4L: Bursts and `commit_group` | `Storage::commit_group` commits a group from its write set in one trip, checked for the head, limits and conflicts; the engine takes up to 256 commands with one look at the clock and one trip through the writer's actions. `sc::{commit_group_checks_the_head, a_writers_groups_conflict_with_another_writer}` on every backend, `groups::batches_queued_before_a_drain_commit_together`, `server/engine::{a_burst_answers_as_a_standby_takes_over, a_burst_answers_as_a_standby_starts_over}`; eight mutants, two of them with `--rocksdb`. On EC2 NVMe, 64 clients' puts: memory 41,236/s to 71,769/s, RocksDB 40,304/s to 62,314/s, SQLite 24,938/s to 35,985/s. |
| Complete | Work | S4M: WALs overwritten in place | SQLite's WAL holds 128 MiB of zeros from the open, wraps at half its size, and syncs with `fdatasync`. RocksDB writes five WAL files at the open, a key in a family of its own holding them until all are written, and a flush comes due at three memtables' worth of live WAL. `sqlite::{commits_keep_overwriting_a_written_wal, commits_in_the_wal_survive_a_crash}`, `rocks::{the_wal_starts_written, a_store_opens_after_a_crash_amid_writing_its_wal}`; five mutants. A gp3 run found only two RocksDB files written at first: renames ran at 1.43 ms against puts' 0.75 ms, and 0.76 ms once fixed. |
| Complete | Work | S4N: Preload of values, and on standbys | `crates/core/src/preload.rs`, `crates/core/src/standby.rs`; `preload::*`, `api::preloaded_servers_answer_without_reads`; four mutants. On EC2 NVMe, 16 clients' stats run at 194,000-290,000/s as a server starts. |
| Complete | Work | S4O: Shallow idle states and mimalloc | `shallow_idle` holds `/dev/cpu_dma_latency` at a microsecond. `server/lib::shallow_idle_asks_for_a_microsecond`, `server/config::defaults_match_the_design`; one mutant. `BENCHMARKS.md` "Idle states": default states take one client's put on RocksDB from 0.090 ms to 0.798 ms, and FoundationDB's 64-client puts from 19,638/s to 9,755/s. |
| Complete | Work | S4P: FoundationDB groups and per-role benchmarks | A group after the handle's own commit runs at that commit's version with a read conflict on the head and no read of it. `sc::{a_writers_groups_conflict_with_another_writer, large_scans_arrive_whole}` on a cluster; eight mutants, caught with `--fdb` on EC2's cluster. `scripts/ec2/bench` runs eleven processes, one per role, and checks it. On EC2 NVMe, per-role, puts from 64 clients went from 12,828/s to 19,638/s, their p99 from 6.66 ms to 3.76 ms. The commit proxy's window sets one client's 1.0 ms: at 0.1 ms, a put took 0.49 ms. |
| Complete | Test | Full mutants run after S4J | `scripts/mutants` at 05703a0: all 145 caught. |
| Complete | Test | Full mutants run after S4P | All 175 caught. A full run on the final tree caught 173 and missed "a stopped writer answers from its cache" and "a releasing writer's cached reads go on", whose states the swarm rarely reaches; `standby::{a_fenced_writer_answers_nothing_from_its_cache, a_releasing_writer_answers_nothing_from_its_cache}` now catch both under `--only`. `--rocksdb`: all 6 caught. `--fdb`: all 8 caught. |
| Complete | Test | Simulator sweep after S4P | A first 100,000-seed sweep found seeds 927052834940686739 and 927052834940736596: the model refused a stat by inode that a link in a removed subtree rightly allowed, for a file with a name outside the key's root. The model is fixed, and `groups::a_detached_link_keeps_its_file_under_the_directories_above_it_until_freed` and both seeds pass. On the fixed model: 0 of 100,000 seeds from 11097099793066296151 failed. |
| Complete | Gate | EC2 benchmarks after S4A-S4P | `scripts/ec2/bench` on the final tree; `README.md`, `BENCHMARKS.md` and the charts show its numbers, and "Before and after" compares 05703a0 on the same machine. The memory backend's cold reads at half SQLite's were an idle-state effect: with shallow states, memory runs at 124,443/s and SQLite at 88,616/s. |

## Phase 8: ClickHouse Metadata Type

Goal:
A ClickHouse object-storage disk with `metadata_type` `inodedb` runs MergeTree with one writer and read replicas. This work lives in a ClickHouse checkout, and this plan tracks it.

Scope:
- 8A A C++ bridge to the Rust client and its cache, built in ClickHouse's Rust workspace.
- 8B `IMetadataStorage` and `IMetadataTransaction` over InodeDB, registered as `inodedb`, reading endpoint and key from the disk's config.
- 8C `tmp_*` directories staged locally and sent as one `all_or_nothing` batch at their rename.
- 8D The owner fence as a `when` on an owner file's version.
- 8E Replicas: snapshots, `refresh` through `wait_for`, removed parts reported by the changefeed.
- 8F Blob removal through `ClaimFreed` and `AckFreed`.
- 8G Small files stored inline, which needs a change to the disk's write path.

Out of scope:
- Changes to MergeTree's refresh beyond what 8E needs.

Completion gate:
ClickHouse's stateless tests pass with tables on the disk, and a server killed mid-insert and mid-merge starts with consistent parts.

Testing plan:
- Stateless tests on the disk; kill tests mid-insert and mid-merge; a replica that sees new and dropped parts within one refresh.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 8A: C++ bridge | Missing: bridge crate and a ClickHouse build. |
| Incomplete | Work | 8B: Metadata storage and transaction | Missing: classes and registration. |
| Incomplete | Work | 8C: Staged parts | Missing: staging layer and an insert test. |
| Incomplete | Work | 8D: Owner fence | Missing: a test that a fenced writer applies nothing. |
| Incomplete | Work | 8E: Replicas | Missing: a replica test with dropped parts. |
| Incomplete | Work | 8F: Blob removal | Missing: a test that blobs go only after acknowledgment. |
| Incomplete | Work | 8G: Inline small files | Missing: write-path change and a cold-load test. |
| Incomplete | Gate | Stateless tests and kill tests | Missing: test run results. |
