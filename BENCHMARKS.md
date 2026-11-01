# Benchmarks

Measurements that guide InodeDB's design. Each names its command, so it can be run again after a change.

Two machines:

- **EC2,** for "Operations" and "Group commit": an i7ie.6xlarge (Intel Xeon Platinum 8559C, 24 vCPUs, 192 GiB) on Ubuntu 24.04 with Linux 7.0, with every store on one device at a time: one of its local NVMe drives, or a 500 GiB gp3 volume provisioned at gp3's maximum of 80,000 IOPS and 2,000 MiB/s. Each device holds an XFS file system. FoundationDB 7.3.79 runs one process on the `ssd-2` engine, with its data on the same device. `scripts/ec2/setup` prepares the instance, and `scripts/ec2/bench` runs both benchmarks on each device.
- **A Mac** for the rest, which measure CPU, memory and failover: an Apple M3 Max (16 cores, 128 GB) on macOS 26.6, with FoundationDB in a Linux container (`scripts/fdb`).

Every run uses a release build.

## Devices

A 4 KiB write followed by `fsync`, one at a time, with `fio` for 15 seconds: the least each durable commit pays.

| Device | Synced writes a second | `fsync` p50 | `fsync` p99 |
| --- | ---: | ---: | ---: |
| Local NVMe | 2,680 | 322 µs | 725 µs |
| gp3, 80,000 IOPS and 2,000 MiB/s | 456 | 2,179 µs | 2,540 µs |

Provisioned IOPS set how many writes a gp3 volume takes at once, and each still crosses the network: a sync costs about 7 times what it costs on local NVMe.

## Operations

```console
scripts/ec2/bench
cargo run --release -p inodedb-tests --example ops --features rocksdb,fdb -- 5 memory,sqlite,rocksdb,fdb
```

Each operation through the gRPC API. The server runs on 8 threads and the clients on 8 more, each client on its own connection, waiting for each answer before sending the next. The tree holds 10,000 files of 100 bytes, and a directory of 100 for listings. A server restarted over the store starts cold: 16 clients stat each file once, then read each value once. A put creates a new file. Reads come from the run on local NVMe; on gp3, warm reads ran within 11% of these.

Operations a second, with 16 clients on a cold cache and 64 on a warm one:

| Operation | Device | Clients | memory | SQLite | RocksDB | FoundationDB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| stat, cold cache | NVMe | 16 | 31,425 | 70,615 | 79,307 | 7,229 |
| read a value, cold cache | NVMe | 16 | 44,001 | 96,099 | 117,773 | 11,820 |
| stat | NVMe | 64 | 236,623 | 239,433 | 237,995 | 229,883 |
| read a 100-byte value | NVMe | 64 | 229,966 | 236,875 | 236,487 | 233,663 |
| list 100 entries | NVMe | 64 | 40,120 | 43,614 | 43,360 | 40,122 |
| put a new 100-byte file | NVMe | 64 | 34,881 | 13,612 | 18,476 | 7,310 |
| put a new 100-byte file | gp3 | 64 | 35,101 | 5,718 | 10,582 | 6,052 |
| rename a file | NVMe | 64 | 38,516 | 15,289 | 20,788 | 8,313 |
| rename a file | gp3 | 64 | 39,053 | 6,387 | 10,664 | 5,570 |

Latency in ms, p50 for one client and p99 for 64:

| Operation | Device | Latency | memory | SQLite | RocksDB | FoundationDB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| stat | NVMe | one client, p50 | 0.316 | 0.318 | 0.130 | 0.318 |
| read a 100-byte value | NVMe | one client, p50 | 0.338 | 0.312 | 0.316 | 0.333 |
| list 100 entries | NVMe | one client, p50 | 0.458 | 0.419 | 0.439 | 0.465 |
| put a new 100-byte file | NVMe | one client, p50 | 0.408 | 1.941 | 1.701 | 3.933 |
| put a new 100-byte file | gp3 | one client, p50 | 0.415 | 3.747 | 3.342 | 5.767 |
| rename a file | NVMe | one client, p50 | 0.361 | 1.581 | 1.499 | 3.186 |
| rename a file | gp3 | one client, p50 | 0.372 | 3.356 | 3.073 | 4.749 |
| stat | NVMe | 64 clients, p99 | 0.498 | 0.500 | 0.559 | 0.684 |
| read a 100-byte value | NVMe | 64 clients, p99 | 0.604 | 0.475 | 0.469 | 0.497 |
| list 100 entries | NVMe | 64 clients, p99 | 2.254 | 1.977 | 2.061 | 2.238 |
| put a new 100-byte file | NVMe | 64 clients, p99 | 2.835 | 8.299 | 5.061 | 12.174 |
| put a new 100-byte file | gp3 | 64 clients, p99 | 2.854 | 22.224 | 7.060 | 12.816 |
| rename a file | NVMe | 64 clients, p99 | 2.465 | 7.418 | 4.626 | 11.848 |
| rename a file | gp3 | 64 clients, p99 | 2.448 | 21.045 | 6.990 | 15.584 |

- The writer answers warm reads from its cache, so they cost the same on every backend and device: 230,000 to 240,000 a second at 64 clients, with p99 under 0.7 ms. One client's read takes about 0.3 ms, nearly all of it the gRPC round trip. Two runs measured 0.11 to 0.13 ms, and the Mac's Linux VM 0.08 ms, so the round trip on this instance varies from run to run.
- A cold read costs a storage read. SQLite and RocksDB answer from the page cache, 70,000 to 118,000 a second; FoundationDB's reads cross to its server process, 6,700 to 11,800. The memory backend, built for tests, varied most between runs: its cold reads ran 44,001 a second in one and 108,367 in the other.
- A write costs a durable commit, and a put first reads storage to learn its new name is absent. At one client, a put takes 1.7 to 1.9 ms on SQLite and RocksDB and 3.9 ms on FoundationDB on local NVMe, and about 1.5 to 2 times that on gp3. At 64 clients, writes gather into groups, so gp3's slower syncs cost RocksDB 43% of its rate, SQLite 58%, and FoundationDB 17%.
- Listings run within 10% of each other on every backend, whether memory runs first or last.

## Group commit

```console
scripts/ec2/bench
cargo run --release -p inodedb-tests --example commits --features rocksdb,fdb -- 3
```

Each client commits one 100-byte file per batch into its own directory, waiting for each answer before sending the next. "One per group" caps every group at one batch; "grouped" uses the default cap of 9 MB. Batches a second, with p50 and p99 in ms:

| Backend | Commit | Clients | NVMe | p50 | p99 | gp3 | p50 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite | one per group | 1 | 641 | 1.60 | 2.19 | 291 | 3.40 | 5.88 |
| SQLite | grouped | 1 | 649 | 1.57 | 2.14 | 291 | 3.40 | 5.85 |
| SQLite | one per group | 16 | 1,050 | 15.60 | 19.83 | 335 | 47.61 | 54.22 |
| SQLite | grouped | 16 | 6,309 | 2.56 | 4.35 | 2,126 | 7.28 | 14.40 |
| SQLite | one per group | 128 | 967 | 131.03 | 158.43 | 343 | 382.42 | 400.78 |
| SQLite | grouped | 128 | 19,309 | 6.29 | 11.55 | 9,335 | 11.74 | 24.93 |
| RocksDB | one per group | 1 | 749 | 1.39 | 1.96 | 333 | 3.01 | 3.59 |
| RocksDB | grouped | 1 | 776 | 1.35 | 1.86 | 330 | 3.05 | 3.58 |
| RocksDB | one per group | 16 | 1,207 | 14.00 | 17.69 | 397 | 41.34 | 43.69 |
| RocksDB | grouped | 16 | 7,346 | 2.24 | 3.52 | 2,993 | 5.35 | 6.16 |
| RocksDB | one per group | 128 | 1,887 | 47.74 | 140.78 | 384 | 334.28 | 341.64 |
| RocksDB | grouped | 128 | 25,287 | 4.96 | 8.21 | 17,773 | 7.17 | 9.73 |
| FoundationDB | one per group | 1 | 316 | 3.18 | 3.91 | 205 | 4.88 | 5.60 |
| FoundationDB | grouped | 1 | 336 | 3.01 | 3.82 | 207 | 4.84 | 5.63 |
| FoundationDB | one per group | 16 | 479 | 33.45 | 38.12 | 224 | 71.57 | 75.71 |
| FoundationDB | grouped | 16 | 3,361 | 4.77 | 5.88 | 1,751 | 9.14 | 10.57 |
| FoundationDB | one per group | 128 | 457 | 280.73 | 295.77 | 285 | 399.53 | 576.19 |
| FoundationDB | grouped | 128 | 13,636 | 9.40 | 11.30 | 14,085 | 8.64 | 11.89 |

- Grouping changes nothing for one client, which never has a second batch waiting.
- With one batch per group, every batch waits for its own durable commit, so 128 clients get at most 2.5 times one client's throughput, and on gp3 under 400 batches a second.
- At 128 clients, grouping multiplies throughput by 13 to 30 on local NVMe and by 27 to 49 on gp3, where each sync costs more.
- FoundationDB groups reach about 14,000 batches a second on both devices, which suggests its one process, not the device, sets that limit.

## Failover

```console
cargo run --release -p inodedb-tests --example failover -- 5
scripts/fdb cargo run --release -p inodedb-tests --example failover --features fdb -- 5
```

A writer and a standby share a store. Eight clients that know only the standby's address commit one file per batch, following `NotWriter` to the writer; the writer's server stops mid-load, five times for each lease, as a killed process does: its runtime ends at once, closing its connections without releasing its lease. Failover is the time from the kill to the first answer after it; the client gap is the longest any client waited between answers. A host or network failure leaves connections open, so clients there also wait out their attempt timeout before they look for the new writer.

| Backend | Lease | Takeover wait ms | Fastest s | Median s | Longest client gap s |
| --- | --- | ---: | ---: | ---: | ---: |
| memory | default | 6,000 | 6.12 | 6.28 | 7.59 |
| memory | 600 ms | 800 | 0.87 | 0.92 | 1.78 |
| FoundationDB | default | 6,000 | 6.07 | 6.11 | 7.89 |
| FoundationDB | 600 ms | 800 | 0.83 | 0.92 | 2.11 |

- Failover is the takeover wait, the lease plus the margin, and about 100 ms more: the standby reads the head every 100 ms, then reads the store and commits its claim. Its cache is warm, so the first batches after the takeover read little.
- A client's gap runs past failover by up to its retry backoff, at most two seconds, since it waits between tries while the standby still sends it to the dead writer.
- The lease trades failover against clock trust: the margin must cover how far two servers' clocks drift apart over a lease, and the writer renews every second however idle.

## Footprint

```console
cargo run --release -p inodedb-tests --example footprint
```

What state costs. Log and storage bytes come from the harness's writer over the memory backend; the tree's directories are three deep, and each batch carries a request id. Memory counts the bytes allocations ask for, for the writer alone, over 200,000 batches each adding one item, against storage that keeps nothing. The undo is what a subscriber holding a snapshot open keeps beyond one without, over 10,000 replaced values, beside what the `undo_bytes` budget charges for it.

| Batch | Log entry bytes | Tree and record bytes |
| --- | ---: | ---: |
| put a new 100-byte file | 57 | 223 |
| replace its value | 43 | 202 |
| mkdir | 63 | 84 |
| rename a file across directories | 74 | 53 |
| unlink a file | 67 | 42 |

| State | Bytes each |
| --- | ---: |
| Live request record, in the writer's memory | 98 |
| Cached empty file: inode, entry and back-pointer | 830 |
| Snapshot undo per replaced value, held | 162 |
| Snapshot undo per replaced value, charged to `undo_bytes` | 160 |

- The log stays small: varint-coded effects and ancestor chains, no values. Even an hour of it at 1,000 batches a second is about 216 MB.
- The writer keeps only a live record's id, and a retry reads the record from storage, so a large batch's result costs no memory while its record lives.
- A cached file's 830 bytes, mostly map nodes and eviction bookkeeping, set the default inode budget: 1 Mi inodes is about 0.8 GB.
- The undo budget charges what a record holds, so 64 MiB of `undo_bytes` is 64 MiB of memory.

## Path resolution

```console
cargo run --release -p inodedb-tests --example resolve
```

A six-level path, every level a directory of 1,001 cached entries, resolved through the cache 2,000,000 times; then the same file read by handle, and a handle's MAC made and checked, 2,000,000 times each.

| Operation | Time |
| --- | ---: |
| Resolve the path to its inode | 197 ns |
| `Stat` through the query engine | 194 ns |
| `Stat` by handle under a key rooted five levels up | 75 ns |
| Make a handle's MAC and check it | 146 ns |

Resolution walks one map per level, so its cost grows with depth and the logarithm of each directory's size, and a rename costs the same whatever lies beneath. A read by handle skips resolution and follows the file's back-pointer and its directories' parents up to the key's root instead, well under the design's estimate of a microsecond. The server makes a MAC for each handle it sends and checks one for each it receives: HMAC-SHA256, keyed once per request.

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
