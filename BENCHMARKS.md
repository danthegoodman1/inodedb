# Benchmarks

Measurements that guide InodeDB's design. Each names its command, so it can be run again after a change.

Three machines:

- **Metal,** for "Devices", "Operations", "Before and after", "Group commit", "Expiring records", "Failover" and "Path resolution": a Latitude.sh `f4.metal.medium` (AMD EPYC 4564P, 16 cores and 32 threads in two L3 groups, one NUMA node, 187 GiB) on Ubuntu 24.04 with Linux 6.8, its CPU governor at `performance`. Every store sits on one Micron 7450 NVMe drive, which has power-loss protection, under XFS. FoundationDB 7.3.79 runs one process per role on the `ssd-2` engine, with single replication and its data on the same drive: coordinator, cluster controller, master, commit proxy, GRV proxy, resolver, log, storage, ratekeeper, data distributor and consistency scan. `scripts/bench/setup` prepares the machine, and `scripts/bench/run` runs the benchmarks, checks that each role holds a process of its own, and records them in `fdb-roles.txt`. Every metal table comes from one sitting. Three passes across 30 minutes stayed within 3% of each other, and a last pass after the sitting within 2% of its start.
- **EC2,** for "Network storage" and "Idle states": an i7ie.6xlarge (Intel Xeon Platinum 8559C, 24 vCPUs, 192 GiB) on Ubuntu 24.04 with Linux 7.0, with a gp3 volume provisioned at gp3's maximum of 80,000 IOPS and 2,000 MiB/s. Those runs predate the lionturtle backend. Its speed varies between sittings: on the same code and kernel, two instances launched later that day ran about 15% slower across the board, and path resolution, which touches neither disk nor network, 34% slower. So compare numbers only within one sitting on one machine.
- **A Mac** for "Footprint", "Revocation" and "Freeing a subtree", which measure memory and CPU, and beside metal in "Path resolution": an Apple M3 Max (16 cores, 128 GB) on macOS 26.6.

Every run uses a release build.

## Devices

One 4 KiB write followed by a sync, one at a time, with `fio` for 15 seconds: the least each durable commit pays. What the sync must make durable sets its cost.

| 4 KiB write and sync | Metal p50 | Metal p99 | EC2 NVMe p50 | EC2 NVMe p99 | gp3 p50 | gp3 p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Written blocks, `fdatasync` | 24 µs | 27 µs | 17 µs | 24 µs | 668 µs | 807 µs |
| Written blocks, `fsync` | 24 µs | 54 µs | 17 µs | 44 µs | 1,352 µs | 1,630 µs |
| Appending to a file, `fdatasync` | 59 µs | 69 µs | 46 µs | 130 µs | 1,352 µs | 1,565 µs |
| New blocks of an allocated file, `fdatasync` | 93 µs | 129 µs | 126 µs | 167 µs | 1,466 µs | 1,761 µs |

- Overwriting a written block with `fdatasync` costs the device alone. Growing a file or converting an allocated extent also commits XFS's journal, which costs 2.5 to 4 times as much on the metal drive and twice as much on gp3. So SQLite and RocksDB write their logs' files when a store opens, lionturtle zero-fills each log segment ahead of its writes, and all three overwrite those blocks from then on. SQLite syncs with `fdatasync`.
- One run before the sitting read 15 µs for a written block. Every run from the sitting on read 24 µs, with or without FoundationDB on the drive.
- Provisioned IOPS set how many writes a gp3 volume takes at once, and each still crosses the network: its cheapest sync costs 39 times EC2 NVMe's.

## Operations

```console
scripts/bench/run
cargo run --release -p inodedb-tests --example ops --features rocksdb,fdb -- 5 memory,sqlite,rocksdb,lionturtle,fdb
```

Each operation through the gRPC API. The server runs on 8 threads and the clients on 8 more, each client on its own connection, waiting for each answer before sending the next. The tree holds 10,000 files of 100 bytes, and a directory of 100 for listings. A server restarted over the store starts cold: 16 clients stat each file once, then read each value once. A second restart measures the same stats as a preloading server starts. A put creates a new file. "Nine stats for each put" has each client do both, on one connection.

Operations a second:

| Operation | Clients | memory | SQLite | RocksDB | lionturtle | FoundationDB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| stat, cold cache | 16 | 183,578 | 86,425 | 102,108 | 99,603 | 33,514 |
| read a value, cold cache | 16 | 223,628 | 108,453 | 147,365 | 161,752 | 58,592 |
| stat, as a preloading server starts | 16 | 279,835 | 251,030 | 261,230 | 258,989 | 117,183 |
| stat | 64 | 391,408 | 386,452 | 405,771 | 400,452 | 401,548 |
| read a 100-byte value | 64 | 396,139 | 387,216 | 389,128 | 390,745 | 386,393 |
| list 100 entries | 64 | 174,657 | 176,427 | 172,275 | 165,375 | 169,371 |
| put a new 100-byte file | 64 | 84,763 | 37,048 | 76,684 | 85,133 | 21,513 |
| rename a file | 64 | 101,990 | 43,643 | 88,521 | 108,677 | 24,298 |
| nine stats for each put | 64 | 305,159 | 243,786 | 287,207 | 278,286 | 180,604 |

Latency in ms, p50 for one client and p99 for 64:

| Operation | Latency | memory | SQLite | RocksDB | lionturtle | FoundationDB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| stat | one client, p50 | 0.023 | 0.023 | 0.023 | 0.023 | 0.023 |
| read a 100-byte value | one client, p50 | 0.023 | 0.023 | 0.023 | 0.023 | 0.023 |
| list 100 entries | one client, p50 | 0.057 | 0.057 | 0.057 | 0.057 | 0.056 |
| put a new 100-byte file | one client, p50 | 0.048 | 0.171 | 0.113 | 0.105 | 1.004 |
| rename a file | one client, p50 | 0.062 | 0.156 | 0.112 | 0.096 | 1.004 |
| stat | 64 clients, p99 | 0.282 | 0.300 | 0.297 | 0.285 | 0.285 |
| read a 100-byte value | 64 clients, p99 | 0.287 | 0.288 | 0.290 | 0.294 | 0.280 |
| list 100 entries | 64 clients, p99 | 0.747 | 0.730 | 0.775 | 0.812 | 0.798 |
| put a new 100-byte file | 64 clients, p99 | 1.447 | 3.250 | 1.151 | 1.181 | 3.690 |
| rename a file | 64 clients, p99 | 1.035 | 1.866 | 0.942 | 1.062 | 3.513 |
| nine stats for each put | 64 clients, p99 | 0.497 | 2.047 | 0.742 | 0.684 | 2.963 |

- The writer answers warm reads from its cache, on the request's own task, so they cost the same on every backend: about 390,000 a second at 64 clients with p99 about 0.3 ms, and 23 µs for one client.
- A listing of 100 entries runs at 165,000 to 176,000 a second, and one client's takes 57 µs.
- A cold read costs a storage read: 86,000 to 102,000 a second on SQLite, RocksDB and lionturtle, 184,000 on memory, and 34,000 on FoundationDB, whose reads cross to its storage process. A server that preloads the tree as it starts serves the same stats at 117,000 to 280,000 a second.
- A write costs a durable commit, and a put first reads storage to learn its new name is absent. At one client, a put takes 0.105 ms on lionturtle, 0.113 ms on RocksDB, 0.171 ms on SQLite and 1.0 ms on FoundationDB, against 0.048 ms on memory.
- At 64 clients, writes gather into groups. lionturtle's 85,000 puts a second match the memory backend's, which mark the writer's own limit: one thread applies every batch.
- lionturtle and RocksDB ran alternately in three more passes, so four each. lionturtle led on puts at 64 clients by 11% and on renames by 21%, and took 9% and 12% less time for one client's put and rename. Reads ran level. RocksDB led "nine stats for each put" by 3%, and on renames its p99 at 64 clients ran 11% lower.
- FoundationDB's commit takes 1.00 ms at one client. That is its commit proxy's batching window: with `knob_commit_transaction_batch_interval_min` and `knob_commit_transaction_batch_interval_from_idle` at 0.0001, one client's put took 0.59 ms, and 64 clients' 25,000 a second against 22,900 in the same pair of runs. These runs leave FoundationDB's knobs at their defaults.

## Before and after

```console
git worktree add ../inodedb-05703a0 05703a0
(cd ../inodedb-05703a0 && cargo build --release -p inodedb-tests --examples --features rocksdb,fdb)
TMPDIR=/mnt/nvme/tmp ../inodedb-05703a0/target/release/examples/ops 5 memory,sqlite,rocksdb,fdb
```

The same benchmark on commit 05703a0 and on this tree, in the same sitting, using the FoundationDB cluster `scripts/bench/run` leaves running:

| Measure | memory | SQLite | RocksDB | FoundationDB |
| --- | ---: | ---: | ---: | ---: |
| stat, 64 clients, /s | 231,890 → 391,408 | 228,698 → 386,452 | 225,344 → 405,771 | 232,809 → 401,548 |
| list, 64 clients, /s | 53,457 → 174,657 | 58,064 → 176,427 | 60,304 → 172,275 | 51,822 → 169,371 |
| put, 64 clients, /s | 51,717 → 84,763 | 24,744 → 37,048 | 49,956 → 76,684 | 15,541 → 21,513 |
| rename, 64 clients, /s | 62,008 → 101,990 | 26,128 → 43,643 | 50,051 → 88,521 | 13,777 → 24,298 |
| cold stat, 16 clients, /s | 110,561 → 183,578 | 73,663 → 86,425 | 91,314 → 102,108 | 30,679 → 33,514 |
| stat, one client, p50 ms | 0.043 → 0.023 | 0.042 → 0.023 | 0.042 → 0.023 | 0.044 → 0.023 |
| list, one client, p50 ms | 0.091 → 0.057 | 0.091 → 0.057 | 0.091 → 0.057 | 0.092 → 0.056 |
| put, one client, p50 ms | 0.089 → 0.048 | 0.272 → 0.171 | 0.199 → 0.113 | 1.004 → 1.004 |
| put, 64 clients, p99 ms | 1.726 → 1.447 | 3.381 → 3.250 | 1.651 → 1.151 | 5.047 → 3.690 |

Where the time went, and what changed:

- **Warm reads.** Profiled on EC2 at 64 clients on 05703a0, the engine's task took 3.7% of all CPU samples, so it set no ceiling. HTTP/2 took over half of each side, in hyper's and tonic's layers and the tasks between them. `crates/transport` drives `h2` directly: a reply's headers and data go out in one write, and a request whose answer is ready needs no task. A read the cache holds answers on the request's task, under a shared lock on the role, with one look at the clock.
- **Listings.** A page the cache holds answers on the request's task too. That, with the transport, took listings from about 55,000 a second to about 175,000.
- **One client's commit.** On EC2 at default idle states, a commit on 05703a0 took 1.35 to 1.6 ms on RocksDB and SQLite, against a device sync of 17 µs: idle-state wake-ups at each hop, the file system's journal at each sync, and two trips to a blocking thread, one to open a transaction and one to commit it. Shallow idle states, logs written ahead of time, and `commit_group`, which commits a group from its write set in one trip, took that to 0.09 ms on RocksDB and 0.17 ms on SQLite there. The metal machine has no deep idle states to leave, so the table here shows the other changes alone.
- **Grouped commits.** The engine takes up to 256 commands at once, with one look at the clock and one trip through the writer's actions, so everything that queued during a commit joins the next group. On RocksDB, the backend drops concurrent-memtable machinery the one writer never needs; on FoundationDB, a group after the handle's own commit reads no head, since its read version covers it.
- **Memory.** The server and benchmarks allocate with mimalloc, and the tail decodes an entry only when a stream asks for it.

## Group commit

```console
scripts/bench/run
cargo run --release -p inodedb-tests --example commits --features rocksdb,fdb -- 3
```

Each client commits one 100-byte file per batch into its own directory, waiting for each answer before sending the next. "One per group" caps every group at one batch; "grouped" uses the default cap of 9 MB. Batches a second, with p50 and p99 in ms:

| Backend | Commit | Clients | Batches/s | p50 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| SQLite | one per group | 1 | 5,979 | 0.16 | 0.22 |
| SQLite | grouped | 1 | 6,218 | 0.16 | 0.21 |
| SQLite | one per group | 16 | 8,952 | 1.75 | 2.22 |
| SQLite | grouped | 16 | 29,226 | 0.53 | 0.77 |
| SQLite | one per group | 128 | 8,853 | 14.18 | 17.36 |
| SQLite | grouped | 128 | 31,417 | 4.29 | 6.50 |
| RocksDB | one per group | 1 | 9,332 | 0.11 | 0.15 |
| RocksDB | grouped | 1 | 9,506 | 0.10 | 0.13 |
| RocksDB | one per group | 16 | 16,583 | 0.94 | 1.20 |
| RocksDB | grouped | 16 | 52,937 | 0.29 | 0.53 |
| RocksDB | one per group | 128 | 16,091 | 7.93 | 9.11 |
| RocksDB | grouped | 128 | 36,992 | 3.41 | 5.13 |
| lionturtle | one per group | 1 | 10,893 | 0.09 | 0.11 |
| lionturtle | grouped | 1 | 10,061 | 0.10 | 0.13 |
| lionturtle | one per group | 16 | 19,251 | 0.81 | 1.03 |
| lionturtle | grouped | 16 | 56,123 | 0.27 | 0.56 |
| lionturtle | one per group | 128 | 17,481 | 7.25 | 10.22 |
| lionturtle | grouped | 128 | 40,315 | 3.11 | 4.85 |
| FoundationDB | one per group | 1 | 994 | 1.00 | 1.08 |
| FoundationDB | grouped | 1 | 993 | 1.00 | 1.08 |
| FoundationDB | one per group | 16 | 995 | 16.08 | 16.46 |
| FoundationDB | grouped | 16 | 7,949 | 2.01 | 2.20 |
| FoundationDB | one per group | 128 | 996 | 128.52 | 129.28 |
| FoundationDB | grouped | 128 | 29,525 | 4.25 | 6.77 |

- Grouping changes little for one client, which never has a second batch waiting.
- With one batch per group, every batch waits for its own durable commit, so 128 clients get at most 1.7 times one client's throughput.
- At 128 clients, grouping multiplies FoundationDB's throughput by 30, SQLite's by 3.5, and RocksDB's and lionturtle's by 2.3, since a sync there takes 24 µs and the device no longer sets the pace.
- At 128 clients, grouped commits reach 40,000 batches a second on lionturtle, 37,000 on RocksDB, 31,000 on SQLite and 30,000 on FoundationDB.

## Expiring records

```console
scripts/bench/run
cargo run --release -p inodedb-tests --example expiry --features rocksdb,fdb -- 60 60
```

Every batch leaves a request record, which lasts `request_ttl`: 10 minutes by default. Once records reach that age, each group deletes as many as it writes, so a server under steady load runs in this state for as long as the load lasts, and the benchmarks above run too briefly to reach it. Here records last 60 seconds. 64 clients each commit one file per batch for 60 seconds, then 60 more as records expire:

| Backend | Before, /s | p50 ms | p99 ms | While expiring, /s | p50 ms | p99 ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory | 43,716 | 1.47 | 2.26 | 41,481 | 1.53 | 2.66 | -5% |
| SQLite | 26,775 | 2.14 | 5.42 | 19,946 | 2.82 | 27.06 | -26% |
| RocksDB | 35,033 | 1.82 | 2.53 | 2,013 | 20.34 | 129.97 | -94% |
| lionturtle | 36,726 | 1.73 | 2.47 | 24,862 | 2.19 | 12.17 | -32% |
| FoundationDB | 18,967 | 3.37 | 3.99 | 16,144 | 3.95 | 4.78 | -15% |

- RocksDB falls 94%, with p99 at 130 ms. Each group clears each record's keys with a range delete, so each expired record leaves a range tombstone, and the likely cost is that reads, the head check of each commit among them, must consult those. lionturtle's lead over RocksDB in this state is 12 times.
- lionturtle has no range delete, so it scans each record's range and deletes what it finds. That costs it 32%, and takes its p99 from 2.5 ms to 12 ms.
- SQLite loses 26%, with p99 at 27 ms, FoundationDB 15% and memory 5%.
- A 10-minute lifetime leaves more records to delete, and older ones, which these 60 seconds don't measure.

## Network storage

```console
scripts/bench/run
```

On EC2, with every store on the instance's local NVMe, then on the gp3 volume. Grouping hides most of a sync's cost: gp3's syncs take 39 times as long, and cost RocksDB 61% of its puts, SQLite 67% and FoundationDB 48%.

Operations a second:

| Operation | Device | Clients | memory | SQLite | RocksDB | FoundationDB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| put a new 100-byte file | NVMe | 64 | 71,769 | 35,985 | 62,314 | 19,638 |
| put a new 100-byte file | gp3 | 64 | 70,188 | 11,972 | 24,504 | 10,292 |
| rename a file | NVMe | 64 | 81,029 | 40,099 | 64,141 | 18,048 |
| rename a file | gp3 | 64 | 79,201 | 12,543 | 27,345 | 10,757 |
| nine stats for each put | NVMe | 64 | 301,852 | 252,981 | 287,955 | 159,879 |
| nine stats for each put | gp3 | 64 | 305,683 | 79,909 | 178,007 | 114,473 |

Latency in ms, p50 for one client and p99 for 64:

| Operation | Device | Latency | memory | SQLite | RocksDB | FoundationDB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| put a new 100-byte file | NVMe | one client, p50 | 0.046 | 0.172 | 0.090 | 1.004 |
| put a new 100-byte file | gp3 | one client, p50 | 0.046 | 1.124 | 0.758 | 2.220 |
| rename a file | NVMe | one client, p50 | 0.043 | 0.165 | 0.095 | 1.006 |
| rename a file | gp3 | one client, p50 | 0.043 | 1.029 | 0.762 | 2.202 |
| put a new 100-byte file | NVMe | 64 clients, p99 | 1.258 | 3.753 | 1.405 | 3.764 |
| put a new 100-byte file | gp3 | 64 clients, p99 | 1.287 | 9.816 | 4.035 | 7.159 |
| rename a file | NVMe | 64 clients, p99 | 1.036 | 2.268 | 1.209 | 4.185 |
| rename a file | gp3 | 64 clients, p99 | 1.054 | 11.360 | 2.675 | 6.731 |
| nine stats for each put | NVMe | 64 clients, p99 | 0.545 | 2.025 | 0.790 | 3.483 |
| nine stats for each put | gp3 | 64 clients, p99 | 0.540 | 7.639 | 2.553 | 5.135 |

Batches a second through the group commit benchmark, with p50 and p99 in ms:

| Backend | Commit | Clients | NVMe | p50 | p99 | gp3 | p50 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite | one per group | 1 | 6,532 | 0.16 | 0.34 | 908 | 1.09 | 1.40 |
| SQLite | grouped | 1 | 6,458 | 0.16 | 0.25 | 907 | 1.09 | 1.45 |
| SQLite | one per group | 16 | 9,258 | 1.71 | 2.39 | 948 | 16.94 | 17.96 |
| SQLite | grouped | 16 | 27,212 | 0.57 | 0.96 | 4,390 | 3.69 | 4.66 |
| SQLite | one per group | 128 | 8,785 | 14.54 | 16.60 | 934 | 137.08 | 144.08 |
| SQLite | grouped | 128 | 25,794 | 5.12 | 6.77 | 15,872 | 8.14 | 16.24 |
| RocksDB | one per group | 1 | 10,913 | 0.08 | 0.28 | 1,325 | 0.75 | 0.88 |
| RocksDB | grouped | 1 | 11,930 | 0.08 | 0.16 | 1,333 | 0.75 | 0.92 |
| RocksDB | one per group | 16 | 18,597 | 0.84 | 1.08 | 1,415 | 11.29 | 12.22 |
| RocksDB | grouped | 16 | 34,232 | 0.46 | 0.77 | 10,075 | 1.58 | 1.91 |
| RocksDB | one per group | 128 | 16,770 | 7.49 | 9.57 | 1,414 | 91.03 | 92.93 |
| RocksDB | grouped | 128 | 30,582 | 4.27 | 5.53 | 23,554 | 5.69 | 8.00 |
| FoundationDB | one per group | 1 | 992 | 1.00 | 1.09 | 748 | 1.47 | 1.69 |
| FoundationDB | grouped | 1 | 994 | 1.00 | 1.09 | 736 | 1.49 | 1.67 |
| FoundationDB | one per group | 16 | 994 | 16.08 | 16.57 | 920 | 17.17 | 19.87 |
| FoundationDB | grouped | 16 | 7,936 | 2.01 | 2.25 | 4,576 | 3.48 | 3.94 |
| FoundationDB | one per group | 128 | 994 | 128.72 | 129.60 | 910 | 139.97 | 151.15 |
| FoundationDB | grouped | 128 | 24,883 | 5.03 | 7.74 | 16,783 | 7.53 | 9.88 |

- gp3 multiplies one client's put by 8.4 on RocksDB, 6.5 on SQLite and 2.2 on FoundationDB.
- At 128 clients on gp3, where each sync costs most, grouping multiplies throughput by 17 to 18.

## Idle states

```console
scripts/bench/run
```

Measured on EC2, whose cores offer C1, C1E and C6, with C6 taking 190 µs to leave. The metal machine's kernel offers no idle states to manage, so these runs alone show what they cost. A thread woken on a core in C6 pays that before it runs, so each hop between threads cost about 92 µs on average with the instance's default states, and a request makes several. The server asks for shallow idle states by holding `/dev/cpu_dma_latency` at a microsecond, which forbids C1E and C6 while it runs (`shallow_idle`, on by default; it needs root or a device rule, and says so in its log without them). The benchmark's server runs in-process, so `scripts/bench/run` sets the same limit through sysfs, then makes a last pass on local NVMe with the default states.

Default states, then C1 alone:

| Measure | memory | SQLite | RocksDB | FoundationDB |
| --- | ---: | ---: | ---: | ---: |
| stat, one client, p50 ms | 0.032 → 0.023 | 0.032 → 0.023 | 0.032 → 0.023 | 0.032 → 0.023 |
| list, one client, p50 ms | 0.071 → 0.062 | 0.074 → 0.062 | 0.070 → 0.061 | 0.076 → 0.062 |
| stat, 64 clients, /s | 328,826 → 440,140 | 309,778 → 438,301 | 352,105 → 436,641 | 422,208 → 436,267 |
| put, one client, p50 ms | 0.316 → 0.046 | 1.013 → 0.172 | 0.798 → 0.090 | 3.095 → 1.004 |
| put, 64 clients, /s | 57,784 → 71,769 | 25,578 → 35,985 | 31,509 → 62,314 | 9,755 → 19,638 |
| cold stat, 16 clients, /s | 49,936 → 124,443 | 67,008 → 88,616 | 75,113 → 102,762 | 8,249 → 28,107 |

- A read the cache holds wakes no thread inside the server, so one client's stat takes only 1.4 times as long under the default states. A commit hops to a blocking thread and back, so a put on RocksDB takes 9 times as long.
- FoundationDB's processes wake each other on every commit, so the default states cost it half its commit throughput and three times its latency.
- The default states explain why the memory backend's cold reads once ran at half SQLite's on this instance and at twice SQLite's on a Mac. They take memory's cold stats from 124,443 a second to 49,936, and SQLite's only from 88,616 to 67,008: memory's threads do the least work per read, so they sleep most.

## Failover

```console
cargo run --release -p inodedb-tests --example failover --features fdb -- 5
```

A writer and a standby share a store. Eight clients that know only the standby's address commit one file per batch, following `NotWriter` to the writer; the writer's server stops mid-load, five times for each lease, as a killed process does: its runtime ends at once, closing its connections without releasing its lease. Failover is the time from the kill to the first answer after it; the client gap is the longest any client waited between answers. A host or network failure leaves connections open, so clients there also wait out their attempt timeout before they look for the new writer.

| Backend | Lease | Takeover wait ms | Fastest s | Median s | Longest client gap s |
| --- | --- | ---: | ---: | ---: | ---: |
| memory | default | 6,000 | 6.05 | 6.23 | 9.43 |
| memory | 600 ms | 800 | 0.81 | 0.91 | 3.57 |
| FoundationDB | default | 6,000 | 6.13 | 6.18 | 9.76 |
| FoundationDB | 600 ms | 800 | 0.91 | 0.93 | 3.83 |

- Failover is the takeover wait, the lease plus the margin, and about 200 ms more: the standby reads the head every 100 ms, then reads the store and commits its claim. Its cache is warm, so the first batches after the takeover read little.
- A client's gap runs about 3 seconds past failover, since it backs off between tries, up to 2 seconds a try, while the standby still sends it to the dead writer.
- The lease trades failover against clock trust: the margin must cover how far two servers' clocks drift apart over a lease, and the writer renews every second however idle.
- A clean stop releases the store, so a standby takes over at once.

## Footprint

```console
cargo run --release -p inodedb-tests --example footprint
```

What state costs. Log and storage bytes come from the harness's writer over the memory backend; the tree's directories are three deep, and each batch carries a request id. Memory counts the bytes allocations ask for, for the writer alone, over 200,000 batches each adding one item, against storage that keeps nothing, beside what its cache charges for them. Each cached record is 200,000 of its kind filled into an empty cache. The undo is what a subscriber holding a snapshot open keeps beyond one without, over 10,000 replaced values, beside what the `undo_bytes` budget charges for it.

| Batch | Log entry bytes | Tree and record bytes |
| --- | ---: | ---: |
| put a new 100-byte file | 57 | 223 |
| replace its value | 43 | 202 |
| mkdir | 63 | 84 |
| rename a file across directories | 74 | 53 |
| unlink a file | 67 | 42 |

| State | Held | Charged |
| --- | ---: | ---: |
| Live request record, in the writer's memory | 98 | none |
| Cached empty file: inode, entry, back-pointer and value | 910 | 983 |
| Snapshot undo per replaced value | 162 | 160 |

| Cached record | Held | Charged |
| --- | ---: | ---: |
| Inode record | 247 | 250 |
| Empty directory | 188 | 570 |
| Directory of one 7-byte name | 651 | 657 |
| 7-byte name in a large directory | 86 | 87 |
| File's one link, by a 7-byte name | 446 | 447 |
| Empty value | 188 | 200 |
| 100-byte value | 288 | 300 |

- The log stays small: varint-coded effects and ancestor chains, no values. Even an hour of it at 1,000 batches a second is about 216 MB.
- The writer keeps only a live record's id, and a retry reads the record from storage, so a large batch's result costs no memory while its record lives.
- A cached file holds 910 bytes, mostly map nodes and eviction bookkeeping, and the cache charges 983, so the default `tree_bytes` of 1 GiB holds about a million files in at most 1 GiB.
- Each charge covers what its record holds. A directory's first name allocates a node of its map, which the directory's charge covers, so an empty directory is charged three times what it holds.
- The undo budget charges what a record holds, so 64 MiB of `undo_bytes` is 64 MiB of memory.

## Path resolution

```console
cargo run --release -p inodedb-tests --example resolve
```

A six-level path, every level a directory of 1,001 cached entries, resolved through the cache 2,000,000 times; then the same file read by handle, and a handle's MAC made and checked, 2,000,000 times each.

| Operation | Mac, at 05703a0 | Metal |
| --- | ---: | ---: |
| Resolve the path to its inode | 197 ns | 152 ns |
| `Stat` through the query engine | 194 ns | 157 ns |
| `Stat` by handle under a key rooted five levels up | 75 ns | 67 ns |
| Make a handle's MAC and check it | 30 ns | 109 ns |

Resolution walks one map per level, so its cost grows with depth and the logarithm of each directory's size, and a rename costs the same whatever lies beneath. A read by handle skips resolution and follows the file's back-pointer and its directories' parents up to the key's root instead, well under the design's estimate of a microsecond. The server makes a MAC for each handle it sends and checks one for each it receives: one AES-256 block.

## Revocation

```console
cargo run --release -p inodedb-tests --example revoke
```

Keys minted under one admin key, all revoked in one commit, through the harness's writer over the memory backend: by revoking the admin key, and by removing the directory every key is rooted at.

| Keys | Revoke the minter | Remove their root |
| ---: | ---: | ---: |
| 4,000 | 2.9 ms | 3.1 ms |
| 16,000 | 11.9 ms | 12.8 ms |
| 64,000 | 52.4 ms | 58.0 ms |

Revocation follows each key's minted keys through an index, so its cost grows with the keys it revokes. A store holds at most 100,000 keys, so revoking all of them fits one commit.

## Freeing a subtree

```console
cargo test --release -p inodedb-sim --test ops a_large_subtree_frees_in_bounded_commits
```

A restarted writer, its cache empty, frees a detached subtree of 2,803 inodes: 2,500 files in one directory and 300 empty directories in another. The count is of storage round trips, each carrying any number of reads.

| Reads per round | Commits | Round trips |
| --- | ---: | ---: |
| Each directory's entries, then everything freeing them needs | 4 | 14 |
| One fact, as the freeing first finds it missing | 4 | 5,611 |

The writer frees subtrees only when it has nothing else to do, but a batch that arrives meanwhile waits until the commit being prepared has read what it needs. At about a millisecond per FoundationDB round trip, gathering each directory's reads keeps that wait to a few milliseconds rather than seconds.
