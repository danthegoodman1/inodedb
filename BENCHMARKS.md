# Benchmarks

Measurements that guide InodeDB's design. Each names its command, so it can be run again after a change.

Machine: Apple M3 Max (16 cores, 128 GB), macOS 26.6, internal SSD, release build.

## Group commit

```console
cargo run --release -p inodedb-tests --example commits --features rocksdb -- 3
scripts/fdb cargo run --release -p inodedb-tests --example commits --features fdb -- 3
```

Each client commits one 100-byte file per batch into its own directory, waiting for each answer before sending the next. "One per group" caps every group at one batch; "grouped" uses the default cap of 9 MB.

| Backend | Commit | Clients | Batches/s | p50 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| SQLite | one per group | 1 | 6,851 | 0.13 | 0.39 |
| SQLite | grouped | 1 | 5,539 | 0.16 | 0.46 |
| SQLite | one per group | 16 | 7,684 | 1.81 | 4.07 |
| SQLite | grouped | 16 | 16,285 | 0.75 | 5.83 |
| SQLite | one per group | 128 | 8,048 | 15.98 | 21.43 |
| SQLite | grouped | 128 | 20,149 | 4.85 | 15.24 |
| RocksDB | one per group | 1 | 8,742 | 0.12 | 0.15 |
| RocksDB | grouped | 1 | 8,393 | 0.12 | 0.15 |
| RocksDB | one per group | 16 | 14,786 | 1.07 | 1.34 |
| RocksDB | grouped | 16 | 31,514 | 0.49 | 0.84 |
| RocksDB | one per group | 128 | 14,966 | 7.75 | 26.81 |
| RocksDB | grouped | 128 | 38,765 | 3.31 | 5.10 |
| FoundationDB | one per group | 1 | 202 | 4.84 | 8.68 |
| FoundationDB | grouped | 1 | 216 | 4.75 | 7.01 |
| FoundationDB | one per group | 16 | 221 | 72.68 | 91.54 |
| FoundationDB | grouped | 16 | 1,470 | 9.77 | 72.26 |
| FoundationDB | one per group | 128 | 212 | 493.69 | 1,121.39 |
| FoundationDB | grouped | 128 | 9,737 | 11.17 | 44.42 |

- Grouping changes nothing for one client, which never has a second batch waiting, and multiplies throughput by 2.1 to 2.6 on the local backends once batches queue behind a commit.
- With one batch per group, throughput stops rising past 16 clients: every batch waits for its own durable commit.
- macOS `fsync` returns before the drive's cache reaches flash, so each commit here costs far less than on a server whose drive honors flushes.
- FoundationDB here is one process on its memory engine in a Linux container on the same machine (`scripts/fdb`). A commit takes about 5 ms, so one batch per group caps throughput near 200 batches a second whatever the load, and grouping multiplies it by 46 at 128 clients. A replicated cluster's commits cost more, and grouping gains more.

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
