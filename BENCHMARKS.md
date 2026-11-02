# Benchmarks

Measurements that guide InodeDB's design. Each names its command, so it can be run again after a change.

Two machines:

- **EC2,** for "Devices", "Idle states", "Operations", "Before and after", "Group commit", "Failover" and "Path resolution": an i7ie.6xlarge (Intel Xeon Platinum 8559C, 24 vCPUs, 192 GiB) on Ubuntu 24.04 with Linux 7.0, with every store on one device at a time: one of its local NVMe drives, or a 500 GiB gp3 volume provisioned at gp3's maximum of 80,000 IOPS and 2,000 MiB/s. Each device holds an XFS file system. FoundationDB 7.3.79 runs one process per role on the `ssd-2` engine, with single replication and its data on the same device: coordinator, cluster controller, master, commit proxy, GRV proxy, resolver, log, storage, ratekeeper, data distributor and consistency scan. `scripts/ec2/setup` prepares the instance, and `scripts/ec2/bench` runs every benchmark on each device, checks that each role holds a process of its own, and records them in `fdb-roles.txt`. Cores stay out of idle states deeper than C1, except where "Idle states" says otherwise.
- **A Mac** for "Footprint", "Revocation" and "Freeing a subtree", which measure memory and CPU, and beside EC2 in "Path resolution": an Apple M3 Max (16 cores, 128 GB) on macOS 26.6.

Every run uses a release build.

## Devices

One 4 KiB write followed by a sync, one at a time, with `fio` for 15 seconds: the least each durable commit pays. What the sync must make durable sets its cost.

| 4 KiB write and sync | NVMe p50 | NVMe p99 | gp3 p50 | gp3 p99 |
| --- | ---: | ---: | ---: | ---: |
| Written blocks, `fdatasync` | 17 µs | 24 µs | 668 µs | 807 µs |
| Written blocks, `fsync` | 17 µs | 44 µs | 1,352 µs | 1,630 µs |
| Appending to a file, `fdatasync` | 46 µs | 130 µs | 1,352 µs | 1,565 µs |
| New blocks of an allocated file, `fdatasync` | 126 µs | 167 µs | 1,466 µs | 1,761 µs |

- Overwriting a written block with `fdatasync` costs the device alone. Growing a file or converting an allocated extent also commits XFS's journal, which costs 3 to 7 times as much on local NVMe and twice as much on gp3. So SQLite and RocksDB write their logs' files when a store opens and overwrite them from then on, and SQLite syncs with `fdatasync`.
- Provisioned IOPS set how many writes a gp3 volume takes at once, and each still crosses the network: its cheapest sync costs 39 times local NVMe's.

## Idle states

```console
scripts/ec2/bench
```

This instance's cores offer C1, C1E and C6, and C6 takes 190 µs to leave. A thread woken on a core in C6 pays that before it runs, so each hop between threads cost about 92 µs on average with the instance's default states, and a request makes several. The server asks for shallow idle states by holding `/dev/cpu_dma_latency` at a microsecond, which forbids C1E and C6 while it runs (`shallow_idle`, on by default; it needs root or a device rule, and says so in its log without them). The benchmark's server runs in-process, so `scripts/ec2/bench` sets the same limit through sysfs, then makes a last pass on local NVMe with the default states.

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

## Operations

```console
scripts/ec2/bench
cargo run --release -p inodedb-tests --example ops --features rocksdb,fdb -- 5 memory,sqlite,rocksdb,fdb
```

Each operation through the gRPC API. The server runs on 8 threads and the clients on 8 more, each client on its own connection, waiting for each answer before sending the next. The tree holds 10,000 files of 100 bytes, and a directory of 100 for listings. A server restarted over the store starts cold: 16 clients stat each file once, then read each value once. A second restart measures the same stats as a preloading server starts. A put creates a new file. Reads come from the run on local NVMe; on gp3, warm reads ran within 6% of these. "Nine stats for each put" has each client do both, on one connection.

Operations a second:

| Operation | Device | Clients | memory | SQLite | RocksDB | FoundationDB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| stat, cold cache | NVMe | 16 | 124,443 | 88,616 | 102,762 | 28,107 |
| read a value, cold cache | NVMe | 16 | 159,776 | 100,302 | 138,198 | 56,497 |
| stat, as a preloading server starts | NVMe | 16 | 290,383 | 215,057 | 259,261 | 193,996 |
| stat | NVMe | 64 | 440,140 | 438,301 | 436,641 | 436,267 |
| read a 100-byte value | NVMe | 64 | 433,266 | 442,188 | 440,804 | 433,089 |
| list 100 entries | NVMe | 64 | 171,899 | 176,663 | 175,845 | 182,617 |
| put a new 100-byte file | NVMe | 64 | 71,769 | 35,985 | 62,314 | 19,638 |
| put a new 100-byte file | gp3 | 64 | 70,188 | 11,972 | 24,504 | 10,292 |
| rename a file | NVMe | 64 | 81,029 | 40,099 | 64,141 | 18,048 |
| rename a file | gp3 | 64 | 79,201 | 12,543 | 27,345 | 10,757 |
| nine stats for each put | NVMe | 64 | 301,852 | 252,981 | 287,955 | 159,879 |
| nine stats for each put | gp3 | 64 | 305,683 | 79,909 | 178,007 | 114,473 |

Latency in ms, p50 for one client and p99 for 64:

| Operation | Device | Latency | memory | SQLite | RocksDB | FoundationDB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| stat | NVMe | one client, p50 | 0.023 | 0.023 | 0.023 | 0.023 |
| read a 100-byte value | NVMe | one client, p50 | 0.023 | 0.024 | 0.023 | 0.023 |
| list 100 entries | NVMe | one client, p50 | 0.062 | 0.062 | 0.061 | 0.062 |
| put a new 100-byte file | NVMe | one client, p50 | 0.046 | 0.172 | 0.090 | 1.004 |
| put a new 100-byte file | gp3 | one client, p50 | 0.046 | 1.124 | 0.758 | 2.220 |
| rename a file | NVMe | one client, p50 | 0.043 | 0.165 | 0.095 | 1.006 |
| rename a file | gp3 | one client, p50 | 0.043 | 1.029 | 0.762 | 2.202 |
| stat | NVMe | 64 clients, p99 | 0.242 | 0.244 | 0.247 | 0.248 |
| read a 100-byte value | NVMe | 64 clients, p99 | 0.246 | 0.238 | 0.243 | 0.247 |
| list 100 entries | NVMe | 64 clients, p99 | 0.730 | 0.704 | 0.709 | 0.662 |
| put a new 100-byte file | NVMe | 64 clients, p99 | 1.258 | 3.753 | 1.405 | 3.764 |
| put a new 100-byte file | gp3 | 64 clients, p99 | 1.287 | 9.816 | 4.035 | 7.159 |
| rename a file | NVMe | 64 clients, p99 | 1.036 | 2.268 | 1.209 | 4.185 |
| rename a file | gp3 | 64 clients, p99 | 1.054 | 11.360 | 2.675 | 6.731 |
| nine stats for each put | NVMe | 64 clients, p99 | 0.545 | 2.025 | 0.790 | 3.483 |
| nine stats for each put | gp3 | 64 clients, p99 | 0.540 | 7.639 | 2.553 | 5.135 |

- The writer answers warm reads from its cache, on the request's own task, so they cost the same on every backend and device: about 440,000 a second at 64 clients with p99 under 0.25 ms, and 23 µs for one client.
- A listing of 100 entries runs at 172,000 to 183,000 a second, and one client's takes 62 µs.
- A cold read costs a storage read: 89,000 to 124,000 a second on memory, SQLite and RocksDB, and 28,000 on FoundationDB, whose reads cross to its storage process. A server that preloads the tree as it starts serves the same stats at 194,000 to 290,000 a second, and FoundationDB gains most.
- A write costs a durable commit, and a put first reads storage to learn its new name is absent. At one client on local NVMe, a put takes 0.09 ms on RocksDB, 0.17 ms on SQLite and 1.0 ms on FoundationDB. gp3 multiplies those by 8.4, 6.5 and 2.2.
- At 64 clients, writes gather into groups, which hide most of a sync's cost: gp3's syncs take 39 times as long, and cost RocksDB 61% of its puts, SQLite 67% and FoundationDB 48%.
- The memory backend's 72,000 puts a second mark the writer's own limit: one thread applies every batch.
- FoundationDB's commit takes 1.00 ms at one client and at any size of group on local NVMe. That is its commit proxy's batching window: with `knob_commit_transaction_batch_interval_min` and `knob_commit_transaction_batch_interval_from_idle` at 0.0001, one client's put took 0.49 ms, and 64 clients' 22,700 a second against 20,400 in the same pair of runs. These runs leave FoundationDB's knobs at their defaults.

## Before and after

```console
git worktree add ../inodedb-05703a0 05703a0
cp scripts/ec2/bench ../inodedb-05703a0/scripts/ec2/bench && ../inodedb-05703a0/scripts/ec2/bench
```

The same benchmark on commit 05703a0 and on this tree, on the same machine, with the same idle states and FoundationDB layout, every store on local NVMe:

| Measure | memory | SQLite | RocksDB | FoundationDB |
| --- | ---: | ---: | ---: | ---: |
| stat, 64 clients, /s | 223,431 → 440,140 | 224,435 → 438,301 | 223,271 → 436,641 | 227,155 → 436,267 |
| list, 64 clients, /s | 34,025 → 171,899 | 32,825 → 176,663 | 38,766 → 175,845 | 35,792 → 182,617 |
| put, 64 clients, /s | 41,236 → 71,769 | 24,938 → 35,985 | 40,304 → 62,314 | 12,828 → 19,638 |
| rename, 64 clients, /s | 45,184 → 81,029 | 27,005 → 40,099 | 36,695 → 64,141 | 10,893 → 18,048 |
| cold stat, 16 clients, /s | 87,801 → 124,443 | 69,968 → 88,616 | 81,717 → 102,762 | 23,743 → 28,107 |
| stat, one client, p50 ms | 0.049 → 0.023 | 0.048 → 0.023 | 0.048 → 0.023 | 0.049 → 0.023 |
| list, one client, p50 ms | 0.118 → 0.062 | 0.114 → 0.062 | 0.123 → 0.061 | 0.135 → 0.062 |
| put, one client, p50 ms | 0.082 → 0.046 | 0.306 → 0.172 | 0.190 → 0.090 | 1.006 → 1.004 |
| put, 64 clients, p99 ms | 1.915 → 1.258 | 3.393 → 3.753 | 1.852 → 1.405 | 6.660 → 3.764 |

Where the time went, and what changed:

- **Warm reads.** At 64 clients on 05703a0, the engine's task took 3.7% of all CPU samples, so it set no ceiling. HTTP/2 took over half of each side, in hyper's and tonic's layers and the tasks between them, and 0.3 ms of one client's read was idle-state wake-ups. `crates/transport` drives `h2` directly: a reply's headers and data go out in one write, and a request whose answer is ready needs no task. A read the cache holds answers on the request's task, under a shared lock on the role, with one look at the clock.
- **Listings.** A page the cache holds answers on the request's task too. That, with the transport, took listings from about 35,000 a second to about 175,000.
- **One client's commit.** On 05703a0 at default idle states, a commit took 1.35 to 1.6 ms on RocksDB and SQLite, against a device sync of 17 µs: idle-state wake-ups at each hop, the file system's journal at each sync, and two trips to a blocking thread, one to open a transaction and one to commit it. Shallow idle states, logs written ahead of time, and `commit_group`, which commits a group from its write set in one trip, took that to 0.09 ms on RocksDB and 0.17 ms on SQLite.
- **Grouped commits.** The engine takes up to 256 commands at once, with one look at the clock and one trip through the writer's actions, so everything that queued during a commit joins the next group. On RocksDB, the backend drops concurrent-memtable machinery the one writer never needs; on FoundationDB, a group after the handle's own commit reads no head, since its read version covers it.
- **Memory.** The server and benchmarks allocate with mimalloc, and the tail decodes an entry only when a stream asks for it.

## Group commit

```console
scripts/ec2/bench
cargo run --release -p inodedb-tests --example commits --features rocksdb,fdb -- 3
```

Each client commits one 100-byte file per batch into its own directory, waiting for each answer before sending the next. "One per group" caps every group at one batch; "grouped" uses the default cap of 9 MB. Batches a second, with p50 and p99 in ms:

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

- Grouping changes little for one client, which never has a second batch waiting.
- With one batch per group, every batch waits for its own durable commit, so 128 clients get at most 1.5 times one client's throughput, and on gp3 under 1,500 batches a second.
- At 128 clients on gp3, where each sync costs most, grouping multiplies throughput by 17 to 18. On local NVMe it multiplies FoundationDB's by 25, and SQLite's and RocksDB's by 2.9 and 1.8, since a sync takes 17 µs there and the device no longer sets the pace.
- FoundationDB's groups reach about 25,000 batches a second on local NVMe and 17,000 on gp3.

## Failover

```console
cargo run --release -p inodedb-tests --example failover --features fdb -- 5
```

A writer and a standby share a store. Eight clients that know only the standby's address commit one file per batch, following `NotWriter` to the writer; the writer's server stops mid-load, five times for each lease, as a killed process does: its runtime ends at once, closing its connections without releasing its lease. Failover is the time from the kill to the first answer after it; the client gap is the longest any client waited between answers. A host or network failure leaves connections open, so clients there also wait out their attempt timeout before they look for the new writer.

| Backend | Lease | Takeover wait ms | Fastest s | Median s | Longest client gap s |
| --- | --- | ---: | ---: | ---: | ---: |
| memory | default | 6,000 | 6.00 | 6.10 | 9.04 |
| memory | 600 ms | 800 | 0.80 | 0.87 | 3.81 |
| FoundationDB | default | 6,000 | 6.11 | 6.22 | 9.69 |
| FoundationDB | 600 ms | 800 | 0.81 | 0.93 | 3.60 |

- Failover is the takeover wait, the lease plus the margin, and at most about 200 ms more: the standby reads the head every 100 ms, then reads the store and commits its claim. Its cache is warm, so the first batches after the takeover read little.
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

| Operation | Mac, at 05703a0 | EC2 |
| --- | ---: | ---: |
| Resolve the path to its inode | 197 ns | 182 ns |
| `Stat` through the query engine | 194 ns | 191 ns |
| `Stat` by handle under a key rooted five levels up | 75 ns | 64 ns |
| Make a handle's MAC and check it | 30 ns | 104 ns |

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
