# Evidence

What checks each behavior `DESIGN.md` names: a test, a simulator property, or a planted bug in `scripts/mutants`. Keep it current as the design changes; a behavior without evidence goes under [Gaps](#gaps).

Notation:

- `ops::`, `groups::`, `keys::`, `leases::`, `snapshots::`, `feed::`, `standby::`, `posix::`, `seeds::` and `fsck::` are tests in `crates/sim/tests/<file>.rs`.
- `api::` is `tests/src/lib.rs` through `api_conformance!`, run on memory, SQLite and RocksDB, and on FoundationDB by `scripts/fdb`. `api-sb::` is `standby_conformance!`, on memory and FoundationDB.
- `sc::` is `crates/storage-conformance/src/lib.rs`, on every backend.
- `cli::` and `mount::` are `crates/cli/tests/*.rs`; `init_key::` is `crates/server/tests/init_key.rs`.
- `core/<module>::` and `server/<module>::` are unit tests in that crate's module.
- P*n* is simulator property *n* (`DESIGN.md`, "Simulator"); M"…" is a planted bug.

## Simulator properties

| Property | Where `crates/sim/src/lib.rs` checks it | Reach |
| --- | --- | --- |
| P1 Answers match the model | `check_commit`, `check_query` with `compare_answer`; ordering in `check_commit` and `answer` | Reads by path from writers and standbys; stats by inode, inside and outside the key's root |
| P2 Exactly once | `durable`, `reply`, `check_commit` | Retries, unknown outcomes, crashes, takeovers |
| P3 `when` and `all_or_nothing` | `Model::apply`, compared by `check_commit` and `durable` | `when_percent` up to 20, `atomic_percent` up to 30 |
| P4 Fencing | `landed` (no commit after a successor's); `answer` (nothing from a superseded writer's state) | Seeds with standbys and stalls |
| P5 `fsck` | `check_storage` after every commit; `fsck::check_with_log` at each run's end | Every seed |
| P6 Caches and snapshots | `probe`, `probe_snapshot` | Path reads; snapshots probed once caught up |
| P7 Release horizon | `lease_answer` | Consumer under the tree's root |
| P8 Key scope | `check_query`, `durable` (a commit revokes exactly the keys whose roots it removed), `answer` | Write keys minted by the root; stats by inode outside a key's root |
| P9 Truncation positions | `check_positions` against the simulator's record of live leases | The simulator truncates no log |

Faults the simulator injects: unknown outcomes, storage latency and stalls, writer crashes mid-group, network delay, loss, duplication and reordering of client requests, client cut-offs, cut watch streams, and clock drift of up to 9% against a margin of 20% of the lease.

## Data model and operations

| Behavior | Evidence |
| --- | --- |
| Ids never reused; inode, entry and value records | P1 (`Mapping::bind` is a bijection), P5, `core/types::records_round_trip` |
| Names and paths | `core/types::{names_follow_the_rules, paths_parse}`, `api::refuses_bad_requests`, `keys::a_key_resolves_paths_from_its_root` |
| Listings in byte order | `core/keys::entries_scan_in_name_order_within_their_directory`, `api::listings_page_by_count`, P1 |
| Handles: MAC, scope by ancestry, freed inodes and removed subtrees by recorded directories | `server/auth::a_handle_works_only_with_its_key`, `keys::{a_read_by_handle_stays_within_the_key, a_freed_file_stays_within_the_keys_that_held_it}`, `standby::a_standby_follows_links_through_renames`, `groups::a_detached_subtree_stays_within_the_directories_above_it_as_it_detached`, `seeds::{seed_541, seed_7638357803986486160}`, P8; M"a handle's MAC goes unchecked", M"a read by handle skips the scope check", M"a feed's changes leave cached links as they were", M"a read by inode inside a removed subtree walks up through its old parent" |
| Each op's errors and modes | `ops::*`, `api::every_op`, `cli::put_modes`, `posix::inodedb_matches_a_real_filesystem`, P1 |
| Renames: kinds, cycles both ways, onto the same file, Linux's error order | `ops::{rename_follows_rename_2, exchange_checks_cycles_both_ways, renames_onto_the_same_file_change_nothing}`, `api::renaming_moves_a_subtree`, `posix`; M"a rename skips the cycle check", M"an exchange skips the reverse cycle check", M"a rename onto another link to the same file unlinks it" |
| Directory mtime; freeing rules | `ops::directory_mtime_follows_its_entries`, P1, P5; M"a replaced value never reaches the free queue", M"an unlink keeps the file's link count" |

## Batches and versions

| Behavior | Evidence |
| --- | --- |
| Cascades, predicates, `Applied` | `ops::skips_cascade_through_state_and_applied`, `api::{every_condition, conditions_skip_and_cascade}`, `core/ops::applied_must_name_an_earlier_op`; M"an op applies though the op it names was skipped" |
| `when`, `all_or_nothing`, no version for no change | `ops::{a_failed_when_applies_nothing, all_or_nothing_applies_nothing_when_any_op_skips, a_batch_that_changes_nothing_takes_no_version}`, `api::{when_fences_the_batch, all_or_nothing_aborts}`, P3; M"an all-or-nothing batch applies part of itself" |
| Request ids, records spanning keys, expiry on the writer's own clock | `ops::a_retried_request_gets_its_first_answer`, `groups::{a_restarted_writer_answers_retries_from_records, a_long_result_spans_record_parts, records_go_once_their_lifetime_ends, a_starting_writer_keeps_records_a_lifetime_by_its_own_clock}`, `seeds::seed_97619`, `api::{retries_answer_once, lost_answers_apply_once}`, P2; M"a retry runs again although its record exists", M"request records never expire", M"a starting writer expires records by other writers' clocks" |
| Limits | `api::{refuses_bad_requests, values_hold_to_the_limit}`, `sc::limits_hold`; M"a transaction over the size limit commits" |
| Strict serializability; `Latest`, `AtLeast`, `OutOfRange` | P1, `api::floors_past_the_store_fail_fast`, `standby::a_standby_answers_reads_at_a_version_and_sends_the_rest_on`; M"a query reads below the version it asked for", M"a standby answers the newest state" |

## Changefeed, caches and snapshots

| Behavior | Evidence |
| --- | --- |
| One entry per commit, keyed by version, split across keys when large | `sc::log_order_and_truncation`, `core/log::entries_round_trip`, P5 |
| Watch: order, replay, renames, routing, key scope, endings | `api::{watch_streams_a_subtree, watch_replays_from_a_version, watch_stays_in_its_keys_tree, watch_ends_with_its_key, shutdown_ends_streams, watch_reports_compaction}`, `cli::watch_prints_changes_as_paths`, P6; M"a directory entering a subtree isn't reported", M"a stream outlives its key", M"a watch starts past the version its key was checked at" |
| Retention and truncation | `server/feed::leases_keep_what_their_positions_need_up_to_the_maximum`, `api::leases_hold_the_log`; M"truncation passes a live lease's position" |
| Caches: budgets, eviction, coherence, what leaves and enters | `core/cache::*`, `feed::*`, `seeds::seed_75494`, `groups::a_warm_writer_reads_nothing`, `api::{caches_follow_the_feed, caches_stop_with_their_key}`, P6; M"a cache keeps a name a commit removed", M"a cache answers after its key is refused", M"what leaves a subtree stays cached", M"a removed directory leaves what lies beneath it cached" |
| Snapshots: undo, kept states, budget, `SnapshotTooOld` | `snapshots::*`, `api::{snapshots_hold_their_version, snapshots_hold_what_they_read}`, P6; M"a snapshot ignores its undo records", M"undo records outgrow their budget" |

## Keys, leases and freeing

| Behavior | Evidence |
| --- | --- |
| Keys: root follows renames, grants, minting, revocation, expiry | `keys::*`, `api::keys_bound_their_reach`, `init_key::the_first_key_comes_from_the_key_file`, P8; M"freeing a key's root leaves the key", M"a minted key outlives its minter" |
| Leases: horizon, heartbeats, expiry, release record | `leases::*`, `core/lease::*`, `groups::a_lease_reopened_after_it_ran_out_stays_stored`, `api::leases_hold_freed_records`, P7; M"the horizon passes a live lease", M"a heartbeat lowers the oldest version", M"a reopened lease keeps the deletion of its earlier run" |
| Heartbeats reach storage within each commit's budget | `core/lease::heartbeats_reach_storage_a_budget_at_a_time`; M"a commit carries every pending heartbeat" |
| Claims: scope, expiry, reads through entries, fencing | `leases::{claims_stay_under_the_consumers_root, a_claim_that_runs_out_returns_its_entries}`, `api::claims_read_values_their_file_outlived`, P4; M"a writer whose lease lapsed answers the claim it scanned" |
| `RemoveAll`: detach, bounded frees, links outside | `ops::{remove_all_detaches_a_subtree_then_frees_it, a_large_subtree_frees_in_bounded_commits, a_wide_directory_frees_over_several_commits}`, `feed::freeing_a_subtree_reaches_those_that_saw_it`, `leases::a_subtrees_frees_go_to_consumers_above_it_as_it_detached`, `seeds::seed_403`; M"the reaper frees a file linked outside its subtree", M"a reap routes to no one who saw its subtree", M"a reaped record names what lies above its top now" |

## Storage and the writer

| Behavior | Evidence |
| --- | --- |
| Contract: order, isolation, conflicts, log, unknown outcomes, durability | `sc::*` on every backend; M"a scan misses an insert into its range", M"a commit skips the head check" |
| Group commit, one transaction in flight, answers after commit | `groups::*`, `api::concurrent_commits_all_apply`, P2; M"a batch hears back before its group commits" |
| Batches queued behind a miss read in its round, up to 1,024 ops | `groups::{queued_batches_that_miss_the_cache_read_together, reading_ahead_runs_a_bounded_number_of_ops_a_round}`; M"batches queued behind a miss read one at a time", M"reading ahead runs the whole queue every round" |
| Unknown outcomes resolve through the head | `groups::{an_unknown_commit_that_applied_is_answered_as_committed, an_unknown_commit_that_did_not_apply_runs_again}`; M"an unknown outcome counts as a failure" |
| Renewal, takeover after lease and margin, drifting clocks | `standby::*`, `core/standby::a_standby_waits_for_the_head_to_go_unchanged`, P4; M"a takeover waits without the margin", M"a writer answers reads after its lease" |
| Standbys: `AtLeast` reads, unknown keys to the writer, warm takeover, small caches, value versions ahead of the log | `standby::{a_standby_with_a_small_cache_answers_under_steady_writes, a_standby_waits_for_a_value_version_it_hasnt_reached}` and the rest of `standby::*`, `api-sb::*`, P1 and P5 through standbys in the swarm; M"a standby refuses a key newer than the log it has read", M"a standby evicts what its reads fetched before their queries run", M"a cache takes a value under a version it hasn't reached as absent" |
| Configuration and defaults | `server/config::{parses_a_minimal_config, rejects_unknown_keys, rejects_unsafe_values, defaults_match_the_design}` |

## API, client and CLI

| Behavior | Evidence |
| --- | --- |
| Listing pages, limits, resumption | `api::{listings_page_by_count, listings_page_by_bytes, cut_listings_resume, listings_follow_their_directory, responses_fit_default_limits}` |
| Replies sent at once, with `TCP_NODELAY` on accepted connections | `server/lib::accepted_connections_send_at_once`; M"accepted connections wait on Nagle's algorithm" |
| Error mapping, redirects, retries | `api::{refuses_bad_requests, keys_bound_their_reach}`, `api-sb::*`; M"a retried commit takes a new request id" |
| `Check`: root admin keys only, tree and log | `api::checks_take_a_root_admin_key`, `cli::{fsck_finds_a_planted_corruption, fsck_reads_a_sqlite_store_directly}`, `fsck::*`; M"any admin key checks the whole store", M"fsck misses a gap in the log" |
| CLI commands | `cli::*` |
| `idb mount` | `mount::a_mount_lists_and_reads_the_tree` on Linux (`scripts/fdb`, CI) |

## Gaps

- The simulator never truncates the log, and lease and claim calls bypass its network.
- The swarm's keys are write keys minted by the root with no expiry; read and admin grants, expiry and explicit revocation have scripted tests only.
- Changefeed progress changes and the "only committed versions" rule of `Watch` have no test.
- The client library has no unit tests; its cache restart after `Compacted`, heartbeat pacing and listing checks are covered only through `api::`.
- The snapshot miss path that waits for the feed never runs.
- Releasing the writer lease on a graceful shutdown is unbuilt, so a planned restart costs the lease plus the margin.
- `idb mount` on macOS is untested.
- The server's clocks across a sleeping host or a stepped wall clock, a check racing truncation, a claim cursor after an `Unavailable` claim, and redirect backoff have no test.
