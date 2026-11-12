# Benchmarks

Measurements that guide InodeDB's design. Each names its command, so it can be run again after a change.

Four machines:

- **Metal,** for "Devices", "Operations", "Before and after", "Group commit", "Expiring records", "Sustained puts", "Failover" and "Path resolution": a Latitude.sh `f4.metal.medium` (AMD EPYC 4564P, 16 cores and 32 threads in two L3 groups, one NUMA node, 187 GiB) on Ubuntu 24.04 with Linux 6.8, its CPU governor at `performance`. Every store sits on one Micron 7450 NVMe drive (MTFDKCC1T9TFR), which reports no volatile write cache, under XFS. FoundationDB 7.3.79 runs one process per role on the `ssd-2` engine, with single replication and its data on the same drive: coordinator, cluster controller, master, commit proxy, GRV proxy, resolver, log, storage, ratekeeper, data distributor and consistency scan. `scripts/bench/setup` prepares the machine, and `scripts/bench/run` runs the benchmarks, checks that each role holds a process of its own, and records them in `fdb-roles.txt`. Every metal table comes from one sitting of about three hours. Each backend runs in its own process: in one process, whichever backend ran second lost up to 3%. Controls after the sitting stayed within 3% of its start on stats and within 5% on puts.
- **EC2,** for "Network storage" and "Idle states": an i7ie.6xlarge (Intel Xeon Platinum 8559C, 24 vCPUs, 192 GiB) on Ubuntu 24.04 with Linux 7.0, with a gp3 volume provisioned at gp3's maximum of 80,000 IOPS and 2,000 MiB/s. Those runs predate the lionturtle backend. Its speed varies between sittings: on the same code and kernel, two instances launched later that day ran about 15% slower across the board, and path resolution, which touches neither disk nor network, 34% slower. So compare numbers only within one sitting on one machine.
- **A Mac** for "Footprint" and "Freeing a subtree", which measure memory and CPU, and beside metal in "Path resolution": an Apple M3 Max (16 cores, 128 GB) on macOS 26.6.
- **A Linux desktop** for "Revocation", "Detaching beside many keys" and "Freeing and claiming across tenants", which measure CPU and count reads, "What FoundationDB charges small keys", which counts, and the key rows of "Footprint": an Intel Core Ultra 9 285 (24 cores, 125 GiB) on Ubuntu 26.04 with Linux 7.0. The Footprint rows it shares with the Mac match byte for byte.

Every run uses a release build.

## Devices

One 4 KiB write followed by a sync, one at a time, with `fio` for 15 seconds: the least each durable commit pays. What the sync must make durable sets its cost.

| 4 KiB write and sync | Metal p50 | Metal p99 | EC2 NVMe p50 | EC2 NVMe p99 | gp3 p50 | gp3 p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Written blocks, `fdatasync` | 24 µs | 27 µs | 17 µs | 24 µs | 668 µs | 807 µs |
| Written blocks, `fsync` | 24 µs | 54 µs | 17 µs | 44 µs | 1,352 µs | 1,630 µs |
| Appending to a file, `fdatasync` | 59 µs | 66 µs | 46 µs | 130 µs | 1,352 µs | 1,565 µs |
| New blocks of an allocated file, `fdatasync` | 83 µs | 125 µs | 126 µs | 167 µs | 1,466 µs | 1,761 µs |

- Overwriting a written block with `fdatasync` costs the device alone. Growing a file or converting an allocated extent also commits XFS's journal, which costs 2.5 to 3.5 times as much on the metal drive and twice as much on gp3. So SQLite and RocksDB write their logs' files when a store opens, lionturtle writes each log segment whole before the segment takes writes, and all three overwrite those blocks from then on. SQLite syncs with `fdatasync`.
- Provisioned IOPS set how many writes a gp3 volume takes at once, and each still crosses the network: its cheapest sync costs 39 times EC2 NVMe's.

## Operations

```console
scripts/bench/run
cargo run --release -p inodedb-tests --example ops --features rocksdb,fdb -- 5 lionturtle
```

Each operation through the gRPC API, with each cell the median of three passes. The server runs on 8 threads and the clients on 8 more, each client on its own connection, waiting for each answer before sending the next. The tree holds 10,000 files of 100 bytes, and a directory of 100 for listings. A server restarted over the store starts cold: 16 clients stat each file once, then read each value once. A second restart measures the same stats as a preloading server starts. A put creates a new file. "Nine stats for each put" has each client do both, on one connection.

Operations a second:

| Operation | Clients | memory | SQLite | RocksDB | lionturtle | FoundationDB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| stat, cold cache | 16 | 179,437 | 84,046 | 105,306 | 110,027 | 40,948 |
| read a value, cold cache | 16 | 188,656 | 112,587 | 151,234 | 162,883 | 68,018 |
| stat, as a preloading server starts | 16 | 248,228 | 232,035 | 236,555 | 255,144 | 223,718 |
| stat | 64 | 383,880 | 392,433 | 400,626 | 390,760 | 400,025 |
| read a 100-byte value | 64 | 390,314 | 387,962 | 390,105 | 388,694 | 389,710 |
| list 100 entries | 64 | 168,008 | 173,103 | 168,283 | 165,466 | 169,647 |
| put a new 100-byte file | 64 | 95,960 | 36,680 | 84,118 | 92,673 | 22,851 |
| rename a file | 64 | 103,663 | 41,603 | 90,673 | 101,991 | 21,894 |
| nine stats for each put | 64 | 303,729 | 249,206 | 290,512 | 288,500 | 175,669 |

Latency in ms, p50 for one client and p99 for 64:

| Operation | Latency | memory | SQLite | RocksDB | lionturtle | FoundationDB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| stat | one client, p50 | 0.023 | 0.023 | 0.023 | 0.023 | 0.022 |
| read a 100-byte value | one client, p50 | 0.023 | 0.023 | 0.023 | 0.023 | 0.023 |
| list 100 entries | one client, p50 | 0.058 | 0.058 | 0.058 | 0.058 | 0.058 |
| put a new 100-byte file | one client, p50 | 0.051 | 0.164 | 0.108 | 0.093 | 1.005 |
| rename a file | one client, p50 | 0.048 | 0.150 | 0.103 | 0.094 | 1.004 |
| stat | 64 clients, p99 | 0.310 | 0.282 | 0.289 | 0.282 | 0.290 |
| read a 100-byte value | 64 clients, p99 | 0.293 | 0.294 | 0.290 | 0.282 | 0.286 |
| list 100 entries | 64 clients, p99 | 0.809 | 0.758 | 0.780 | 0.815 | 0.787 |
| put a new 100-byte file | 64 clients, p99 | 1.053 | 3.460 | 1.032 | 1.143 | 3.221 |
| rename a file | 64 clients, p99 | 1.015 | 2.053 | 0.961 | 1.048 | 3.478 |
| nine stats for each put | 64 clients, p99 | 0.477 | 2.003 | 0.817 | 0.629 | 3.021 |

- The writer answers warm reads from its cache, on the request's own task, so they cost the same on every backend: about 390,000 a second at 64 clients with p99 about 0.3 ms, and 23 µs for one client.
- A listing of 100 entries runs at 165,000 to 173,000 a second, and one client's takes 58 µs. A listing's rate varies by up to 11% between passes on one backend, so the gaps between these medians are noise.
- A cold read costs a storage read: 84,000 to 110,000 a second on SQLite, RocksDB and lionturtle, 179,000 on memory, and 41,000 on FoundationDB, whose reads cross to its storage process. A server that preloads the tree as it starts serves the same stats at 224,000 to 255,000 a second.
- A write costs a durable commit, and a put first reads storage to learn its new name is absent. At one client, a put takes 0.093 ms on lionturtle, 0.108 ms on RocksDB, 0.164 ms on SQLite and 1.0 ms on FoundationDB, against 0.051 ms on memory.
- At 64 clients, writes gather into groups. lionturtle's 92,700 puts a second come within 4% of the memory backend's, and its 102,000 renames within 2%.
- lionturtle and RocksDB ran alternately in six more passes, with each going first in three. lionturtle won every pass on puts and renames: by 10% and 15% at 64 clients, and by 11% and 13% at one client. It also led cold stats in every pass, by 4%. Warm reads, listings and "nine stats for each put" ran level, within 1% over the six passes with no steady winner, and the stats of a preloading server swung 10% either way between passes.
- RocksDB keeps the lower p99 on writes at 64 clients: 1.06 ms against 1.15 ms on puts, and 0.94 ms against 1.02 ms on renames. lionturtle's p50 runs lower: 0.64 ms against 0.72 ms, and 0.57 ms against 0.66 ms.
- That p99 comes from the writer's side of a group's cycle, since lionturtle's commits are the shorter at every percentile. A temporary probe timed each commit over 30 seconds of puts at 64 clients: lionturtle's took 178 to 182 µs at the median against RocksDB's 264 to 266, 275 to 280 µs at p99 against 405 to 425, and 357 to 363 µs at p99.9 against 644 to 889. lionturtle's slowest commit took 3.5 ms. RocksDB's ran 16 to 26 ms several times a run, and one waited 206 ms on RocksDB's own write delay. One client's commit took 42 to 46 µs on lionturtle against 58 to 59.
- FoundationDB's commit takes 1.00 ms at one client. That is its commit proxy's batching window: with `knob_commit_transaction_batch_interval_min` and `knob_commit_transaction_batch_interval_from_idle` at 0.0001, one client's put took 0.58 ms, and 64 clients' 25,600 a second against 21,500 in the same pair of runs. These runs leave FoundationDB's knobs at their defaults.

## Before and after

```console
git worktree add ../inodedb-05703a0 05703a0
(cd ../inodedb-05703a0 && cargo build --release -p inodedb-tests --examples --features rocksdb,fdb)
for backend in memory sqlite rocksdb fdb; do
  TMPDIR=/mnt/nvme/tmp ../inodedb-05703a0/target/release/examples/ops 5 $backend
done
```

The same benchmark on commit 05703a0 and on this tree, in the same sitting, using the FoundationDB cluster `scripts/bench/run` leaves running. 05703a0 ran one pass, so small differences mean little: FoundationDB's cold stats on it read 37,362 a second here and 30,679 on another machine of this type.

| Measure | memory | SQLite | RocksDB | FoundationDB |
| --- | ---: | ---: | ---: | ---: |
| stat, 64 clients, /s | 228,009 → 383,880 | 230,926 → 392,433 | 229,794 → 400,626 | 225,341 → 400,025 |
| list, 64 clients, /s | 50,534 → 168,008 | 45,589 → 173,103 | 44,874 → 168,283 | 55,471 → 169,647 |
| put, 64 clients, /s | 51,669 → 95,960 | 24,397 → 36,680 | 53,478 → 84,118 | 15,007 → 22,851 |
| rename, 64 clients, /s | 64,301 → 103,663 | 28,060 → 41,603 | 50,392 → 90,673 | 13,313 → 21,894 |
| cold stat, 16 clients, /s | 137,785 → 179,437 | 70,852 → 84,046 | 92,937 → 105,306 | 37,362 → 40,948 |
| stat, one client, p50 ms | 0.042 → 0.023 | 0.041 → 0.023 | 0.042 → 0.023 | 0.042 → 0.022 |
| list, one client, p50 ms | 0.091 → 0.058 | 0.089 → 0.058 | 0.092 → 0.058 | 0.089 → 0.058 |
| put, one client, p50 ms | 0.086 → 0.051 | 0.270 → 0.164 | 0.193 → 0.108 | 1.004 → 1.005 |
| put, 64 clients, p99 ms | 1.749 → 1.053 | 3.386 → 3.460 | 1.569 → 1.032 | 5.421 → 3.221 |

Where the time went, and what changed:

- **Warm reads.** Profiled on EC2 at 64 clients on 05703a0, the engine's task took 3.7% of all CPU samples, so it set no ceiling. HTTP/2 took over half of each side, in hyper's and tonic's layers and the tasks between them. `crates/transport` drives `h2` directly: a reply's headers and data go out in one write, and a request whose answer is ready needs no task. A read the cache holds answers on the request's task, under a shared lock on the role, with one look at the clock.
- **Listings.** A page the cache holds answers on the request's task too. That, with the transport, took listings from about 45,000 a second to about 170,000.
- **One client's commit.** On EC2 at default idle states, a commit on 05703a0 took 1.35 to 1.6 ms on RocksDB and SQLite, against a device sync of 17 µs: idle-state wake-ups at each hop, the file system's journal at each sync, and two trips to a blocking thread, one to open a transaction and one to commit it. Shallow idle states, logs written ahead of time, and `commit_group`, which commits a group from its write set in one trip, took that to 0.09 ms on RocksDB and 0.17 ms on SQLite there. The metal machine has no deep idle states to leave, so the table here shows the other changes alone.
- **Grouped commits.** The engine takes up to 256 commands at once, with one look at the clock and one trip through the writer's actions, so everything that queued during a commit joins the next group. On RocksDB, the backend drops concurrent-memtable machinery the one writer never needs; on FoundationDB, a group after the handle's own commit reads no head, since its read version covers it.
- **Memory.** The server and benchmarks allocate with mimalloc and keep the memory they free, the server copies a put's value out of its request's buffer, and the tail decodes an entry only when a stream asks for it ("Sustained puts").

## Group commit

```console
scripts/bench/run
cargo run --release -p inodedb-tests --example commits --features rocksdb,fdb -- 3
```

Each client commits one 100-byte file per batch into its own directory, waiting for each answer before sending the next. "One per group" caps every group at one batch; "grouped" uses the default cap of 9 MB. Batches a second, with p50 and p99 in ms:

| Backend | Commit | Clients | Batches/s | p50 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| SQLite | one per group | 1 | 6,333 | 0.16 | 0.21 |
| SQLite | grouped | 1 | 6,043 | 0.16 | 0.22 |
| SQLite | one per group | 16 | 9,187 | 1.71 | 2.36 |
| SQLite | grouped | 16 | 30,259 | 0.51 | 0.70 |
| SQLite | one per group | 128 | 9,016 | 14.09 | 16.03 |
| SQLite | grouped | 128 | 29,797 | 4.46 | 6.50 |
| RocksDB | one per group | 1 | 10,291 | 0.09 | 0.12 |
| RocksDB | grouped | 1 | 9,274 | 0.11 | 0.14 |
| RocksDB | one per group | 16 | 17,193 | 0.90 | 1.13 |
| RocksDB | grouped | 16 | 54,336 | 0.28 | 0.54 |
| RocksDB | one per group | 128 | 16,810 | 7.57 | 9.32 |
| RocksDB | grouped | 128 | 40,013 | 3.17 | 4.62 |
| lionturtle | one per group | 1 | 11,144 | 0.09 | 0.11 |
| lionturtle | grouped | 1 | 11,092 | 0.09 | 0.12 |
| lionturtle | one per group | 16 | 19,967 | 0.79 | 1.11 |
| lionturtle | grouped | 16 | 60,542 | 0.25 | 0.52 |
| lionturtle | one per group | 128 | 18,653 | 6.94 | 8.39 |
| lionturtle | grouped | 128 | 45,098 | 2.84 | 4.30 |
| FoundationDB | one per group | 1 | 995 | 1.00 | 1.08 |
| FoundationDB | grouped | 1 | 994 | 1.00 | 1.08 |
| FoundationDB | one per group | 16 | 996 | 16.07 | 16.44 |
| FoundationDB | grouped | 16 | 7,954 | 2.01 | 2.14 |
| FoundationDB | one per group | 128 | 995 | 128.65 | 129.82 |
| FoundationDB | grouped | 128 | 29,427 | 4.28 | 6.91 |

- Grouping changes little for one client, which never has a second batch waiting.
- With one batch per group, every batch waits for its own durable commit, so 128 clients get at most 1.7 times one client's throughput.
- At 128 clients, grouping multiplies FoundationDB's throughput by 30, SQLite's by 3.3, and RocksDB's and lionturtle's by 2.4, since a sync there takes 24 µs and the device no longer sets the pace.
- At 128 clients, grouped commits reach 45,000 batches a second on lionturtle, 40,000 on RocksDB, 30,000 on SQLite and 29,000 on FoundationDB.

## Expiring records

```console
scripts/bench/run
cargo run --release -p inodedb-tests --example expiry --features rocksdb,fdb -- 60 60 lionturtle
```

Every batch leaves a request record, which lasts `request_ttl`: 10 minutes by default. Once records reach that age, each group deletes as many as it writes, so a server under steady load runs in this state for as long as the load lasts, and the benchmarks above run too briefly to reach it. Here records last 60 seconds. 64 clients each commit one file per batch for 60 seconds, then 60 more as records expire:

| Backend | Before, /s | p50 ms | p99 ms | While expiring, /s | p50 ms | p99 ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| memory | 44,989 | 1.42 | 2.22 | 41,549 | 1.54 | 2.62 | −8% |
| SQLite | 26,824 | 2.37 | 4.81 | 21,007 | 2.68 | 27.36 | −22% |
| RocksDB | 37,156 | 1.71 | 2.44 | 32,753 | 1.95 | 2.80 | −12% |
| lionturtle | 40,297 | 1.58 | 2.26 | 36,872 | 1.72 | 2.53 | −8% |
| FoundationDB | 19,663 | 3.31 | 3.97 | 16,423 | 3.88 | 4.62 | −16% |

- RocksDB falls 12% and lionturtle 8%, to 32,800 and 36,900 batches a second with p99 under 3 ms. Each group names every key it deletes, so it reads nothing and leaves no range tombstones.
- Range deletes would cost far more here. With one per record, RocksDB fell 94%, with p99 at 130 ms, and a profile put over 40% of its CPU in the fragmented range tombstones each read rebuilds. lionturtle, which reads a range to clear it, fell 32%.
- SQLite falls 22%, with p99 at 27 ms, FoundationDB 16% and memory 8%.
- A 10-minute lifetime leaves more records to delete, and older ones, which these 60 seconds don't measure.

## Sustained puts

```console
/usr/bin/time -v target/release/examples/ops 120 lionturtle put
```

Two minutes of puts from one client, then two from 64, each backend in its own process, on commit eeca4a0 and on this tree. Peak memory is the whole process's: the server and the benchmark's clients. At 64 clients:

| Backend | Puts/s at eeca4a0 | Puts/s now | p99 ms at eeca4a0 | p99 ms now | Peak GiB at eeca4a0 | Peak GiB now |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory | 72,927 | 74,763 | 1.281 | 1.249 | 21.9 | 15.6 |
| SQLite | 29,756 | 30,154 | 2.866 | 2.809 | 8.9 | 2.2 |
| RocksDB | 67,945 | 68,976 | 1.237 | 1.245 | 10.3 | 3.7 |
| lionturtle | 65,947 | 75,581 | 1.509 | 1.430 | 10.3 | 5.6 |

Three changes separate the columns:

- **A cached value held its request's buffer.** The decoder hands a put's value over as a slice of the request's buffer, 8 KiB or more, and the cache kept the slice: 300 bytes charged and 8 KiB held, about 7 GiB at the default 256 MiB of values. The server now copies the value out. Peak memory fell by 5 to 7 GiB on every backend. Each request's buffer had also come from a fresh page, which the kernel zeroes: 3.4% of all CPU samples in a profile of puts on lionturtle, beside 2.6% in the allocator beneath the decoder.
- **The server keeps the memory it frees.** mimalloc returned it to the kernel a second after it came free, on whichever thread allocated next. lionturtle 0.5.3 kept its memtable in millions of allocations and freed them all at each checkpoint, every four seconds under this load. `perf` found commits after a checkpoint inside `madvise` for 4 to 40 ms, splitting huge pages, and the commits after them took 3 to 6 page faults each to map the memory again. With purging off and nothing else changed, on another machine of this type, lionturtle ran these two minutes at 74,109 puts a second against 67,001, and its slowest commit took 4.0 ms against 27.0. RocksDB's memtable sits in an arena of its own, outside the allocator, and moved from 68,324 to 68,905. A 10-second delay recovered a third of the gain, and turning huge pages off alone none.
- **lionturtle 0.6.0.** It keeps a memtable's memory to itself and reuses it for the next, writes each log segment whole before the segment takes writes, and copies what a write gives it, so InodeDB passes a group's keys and values where they lie. With the two changes above and lionturtle 0.5.3, these two minutes ran at 72,148 puts a second. Over 30 seconds of puts, the mean commit fell from 214 µs on 0.5.3 to 175 to 180, its p99 from 330 to 341 µs to 275 to 280, and its p99.9 from 484 to 514 µs to 357 to 363. A new store on 0.5.3 also zero-filled its log as it went: its first rotation held a commit 6 to 9 ms, and a log write that overlapped a fill took 219 µs at the median against 46.

What they leave:

- **lionturtle leads RocksDB by 10% over two minutes,** where it trailed by 3%, and matches the memory backend, whose own maps grow through the run.
- **The writer's task bounds these rates, on every backend.** At 64 clients a group of 32 puts carries 257 writes and 13 KB. lionturtle commits it in 178 to 182 µs at the median and RocksDB in 264 to 266, of which about 160 is one thread's inserts into its memtable. A commit is in flight 48% of the time on lionturtle while the writer's task is busy 79%. On 0.5.3, a prototype that ran lionturtle's inserts on other threads beside the log's write cut its commit from 229 to 148 µs, and the rate rose not at all. In a profile of that run, lionturtle's commits took 8.6% of the process's CPU samples and the writer's task 13.3%; HTTP/2, the codec and the runtime's scheduling took 62.6%, clients included.
- **A thread of the store's own for commits** shortens lionturtle's mean commit from 175 to 180 µs to 154 to 160 and one client's put by 2% to 4%, and leaves 64 clients' rate where it was. InodeDB runs commits on the runtime's blocking pool.

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
| memory | default | 6,000 | 6.01 | 6.13 | 9.28 |
| memory | 600 ms | 800 | 0.85 | 0.92 | 3.88 |
| FoundationDB | default | 6,000 | 6.12 | 6.29 | 9.56 |
| FoundationDB | 600 ms | 800 | 0.90 | 0.92 | 3.67 |

- Failover is the takeover wait, the lease plus the margin, and about 200 ms more: the standby reads the head every 100 ms, then reads the store and commits its claim. Its cache is warm, so the first batches after the takeover read little.
- A client's gap runs about 3 seconds past failover, since it backs off between tries, up to 2 seconds a try, while the standby still sends it to the dead writer.
- The lease trades failover against clock trust: the margin must cover how far two servers' clocks drift apart over a lease, and the writer renews every second however idle.
- A clean stop releases the store, so a standby takes over at once.

## Footprint

```console
cargo run --release -p inodedb-tests --example footprint
```

What state costs. Log and storage bytes come from the harness's writer over the memory backend; the tree's directories are three deep, and each batch carries a request id. Memory counts the bytes allocations ask for, for the writer alone, over 200,000 batches each adding one item, against storage that keeps nothing, beside what its cache charges for them. Each key is one of 50,000 that an admin key mints, measured while they live and again once they have expired and the writer has deleted them. Each cached record is 200,000 of its kind filled into an empty cache. The undo is what a subscriber holding a snapshot open keeps beyond one without, over 10,000 replaced values, beside what the `undo_bytes` budget charges for it.

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
| Live key with a TTL, in the writer's memory | 225 | none |
| Key minted by a key, once expired and deleted | 0 | none |
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
| Resolve the path to its inode | 197 ns | 149 ns |
| `Stat` through the query engine | 194 ns | 155 ns |
| `Stat` by handle under a key rooted five levels up | 75 ns | 66 ns |
| Make a handle's MAC and check it | 30 ns | 109 ns |

Resolution walks one map per level, so its cost grows with depth and the logarithm of each directory's size, and a rename costs the same whatever lies beneath. A read by handle skips resolution and follows the file's back-pointer and its directories' parents up to the key's root instead, well under the design's estimate of a microsecond. The server makes a MAC for each handle it sends and checks one for each it receives: one AES-256 block.

## Revocation

```console
cargo run --release -p inodedb-tests --example revoke
```

Keys minted under one admin key, through the harness's writer over the memory backend, revoked in one commit by revoking the admin key or by removing the directory every key is rooted at. The last column mints them each with the same TTL under an admin key that never expires, and times the writer's deletions once they expire: a thousand keys to a commit, each commit sent as the one before lands. Four runs stayed within 5% of one another; the table is the last.

| Keys | Revoke the minter | Remove their root | Delete them once expired |
| ---: | ---: | ---: | ---: |
| 4,000 | 1.9 ms | 1.8 ms | 1.5 ms |
| 16,000 | 8.6 ms | 8.1 ms | 6.4 ms |
| 64,000 | 44.8 ms | 49.6 ms | 33.3 ms |

Deleting an expired key costs the writer about half a microsecond. Finding that none is due reads the first entry of an index ordered by expiry.

Revocation follows each key's minted keys through an index, so its cost grows with the keys it revokes. A store holds at most 100,000 keys, so revoking all of them fits one commit.

## Detaching beside many keys

```console
cargo run --release -p inodedb-tests --example detach
```

Tenants, each a directory with two keys rooted at it, through the harness's writer over the memory backend, with the freeing that follows each commit: the median of 20 commits. One commit removes 50 empty directories under one tenant, none of them a key's root. The other removes one tenant, which is a key's root. The first row holds no keys.

| Tenants | Keys | Removing 50 directories | Removing a tenant |
| ---: | ---: | ---: | ---: |
| 10,000 | 0 | 0.19 ms | 0.01 ms |
| 1,000 | 2,000 | 0.19 ms | 0.10 ms |
| 10,000 | 20,000 | 0.21 ms | 1.10 ms |
| 50,000 | 100,000 | 0.21 ms | 5.99 ms |

The 50 directories lie outside the cover, the directories that hold or lie above a key's root, so their commit looks at no key and costs the same beside any number of keys. A tenant lies inside it, so its commit searches: it follows each directory a key is rooted at up to the tree's root once, about 110 ns a root, whatever the commit detaches. A writer's first detach searches too, whatever it removes, and leaves the cover.

These runs kept every record cached. From a cold cache the search asks for every record it lacks in one round per level (`keys::a_detach_reads_the_key_roots_a_cold_cache_lacks_in_rounds`: 9 rounds for 300 roots).

## Freeing a subtree

```console
cargo test --release -p inodedb-sim --test ops a_large_subtree_frees_in_bounded_commits
```

A restarted writer, its cache empty, frees a detached subtree of 2,803 inodes: 2,500 files in one directory and 300 empty directories in another. The count is of storage round trips, each carrying any number of reads.

| Reads per round | Commits | Round trips |
| --- | ---: | ---: |
| Each directory's entries, then everything freeing them needs | 3 | 14 |
| One fact, as the freeing first finds it missing | 3 | 5,611 |

The writer frees subtrees only when it has nothing else to do, but a batch that arrives meanwhile waits until the commit being prepared has read what it needs. At about a millisecond per FoundationDB round trip, gathering each directory's reads keeps that wait to a few milliseconds rather than seconds.

## Freeing and claiming across tenants

```console
cargo run --release -p inodedb-tests --example claims
```

Tenants through the harness's writer over the memory backend, each a directory `tenants/tN` holding one part, `store/aa/table/part`, of 20 files, so six directories lie between a file and the tree's root. The run removes every tenant's part, 50 to a commit, and times each commit with the freeing that follows. It then claims up to 60 entries under one tenant's root and acknowledges them, for 20 tenants spread across the queue, and reports the median. "Before" is commit be669f1.

| Tenants | Queue entries | | Queue bytes an entry | One commit removing 50 parts | Reads for a claim | A claim and its acknowledgement |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 100 | 2,100 | Before | 29 | 2.95 ms | 2 | 0.227 ms |
| | | Now | 184 | 4.98 ms | 2 | 0.068 ms |
| 1,000 | 21,000 | Before | 31 | 2.80 ms | 21 | 1.899 ms |
| | | Now | 186 | 4.71 ms | 2 | 0.088 ms |
| 10,000 | 210,000 | Before | 35 | 2.97 ms | 205 | 29.070 ms |
| | | Now | 190 | 5.38 ms | 2 | 0.126 ms |

A claim under a tenant's root reads that root's part of the queue and then the entries it names: two reads, whatever the queue holds for other tenants. Before, it read the whole queue, a page of 1,024 entries at a time. The queue's keys under directories cost the removal: each freed inode writes a key of 26 bytes under each of the six directories above it, so a queued entry takes 5.4 times the bytes, and a commit that removes 50 parts, 1,050 inodes, takes 1.8 times as long in the writer. The freeing itself went from 50 commits, one for each part, to two of at most 1,000 inodes.

## A housekeeping pass over tenants' tables

```console
cargo run --release -p inodedb-server -- --config inodedb.toml --init-key admin.key
cargo run --release -p inodedb-tests --example housekeep -- setup http://127.0.0.1:7070 admin.key keys.txt 10000 8
cargo run --release -p inodedb-tests --example housekeep -- pass http://127.0.0.1:7070 admin.key keys.txt 32
```

10,000 tenants on one SQLite store on the Linux desktop's drive, about 4 million inodes in the shape a ClickHouse disk gives them: a tenant has a key and two tables of ten parts, a part of one table holds 11 files, and a part of the other holds 13 files and a directory of 10. `setup` ran once and each pass started from a copy of its store with a server just started. A pass housekeeps every tenant once, a fixed number at a time: each housekeep holds a lease, removes eight parts of a table and their claim files in one commit, then lists the free queue, claims and acknowledges until a round claims nothing. The pass removes 160,000 parts. "Before" is commit 1191cde. Both servers carried a stack sampler and counters of what the writer's queue spends its time on.

| Housekeeps in flight | | Parts removed a second | Server cores | Runs of a freeing commit | A freeing commit holds the queue | Freeing level after the pass |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | Before | 573 | 1.25 to 1.29 | 40 | 37 ms | at once |
| | Now | 1,404 | 1.44 to 1.53 | 4 | 8 ms | 8 s |
| 32 | Before | 307 | 1.11 to 1.16 | 53 | 220 ms | 288 s |
| | Now | 2,639 | up to 2.08 | 2 to 3 | 12 ms | 46 s |

A freeing commit runs again after each round of reads it waits for, and batches wait behind it meanwhile. Before, a part that held a directory asked for that directory's listing only once every earlier part in the commit had its records, so such parts advanced one at a time, two rounds each. While freeing kept level a commit freed about 700 inodes in 40 runs. At 32 in flight housekeeping detached more than that pace freed, every commit became a full one of 53 runs and 206 ms of CPU, and freeing fell to 4,100 inodes a second with the batches queued behind it: twice the housekeeps removed half the parts. Now the directories inside every part of a commit ask in the same rounds, and a commit takes two to four runs.

A listing of the free queue reports how far freeing has reached under its root. Before, it scanned every detached subtree waiting, 0.22 ms a call with 50,000 and 0.45 ms with 100,000; with the first cost gone and 40,000 to 70,000 subtrees waiting, these listings took 15% to 42% of the writer's time. Now the writer counts the subtrees waiting under each directory and a listing takes one lookup.

## What FoundationDB charges small keys

```console
scripts/fdb cargo test --release -p inodedb-storage-conformance --features fdb --test fdb what_foundationdb_charges_small_keys -- --ignored --nocapture
```

FoundationDB 7.3.79 in the test container, a store whose prefix holds 32 bytes, with one more for the tree. The run finds the most sets, and the most clears, of keys of one length that a transaction commits, and divides FoundationDB's 10,000,000 bytes by it.

| Key bytes | A set | A clear |
| ---: | ---: | ---: |
| 26 | 243 bytes | 302 bytes |
| 34 | 268 bytes | 334 bytes |
| 100 | 465 bytes | 599 bytes |

FoundationDB counts a key three times over when set, once written and twice in the range that guards it from conflicts, and four times when cleared, each time with the store's prefix, and 66 bytes more for each write: 3 × (26 + 33) + 66 = 243. The backends' own limit counts the key once, so a transaction of 80,000 sets of 26-byte keys, 2 MB by that count, is refused. The writer sizes its groups, and refuses a batch, by `WriteSet::charged`, which counts as FoundationDB does with 72 bytes a write and room for a prefix of 16 bytes: `fdb::small_keys_fit_a_transaction_as_they_are_charged`.
