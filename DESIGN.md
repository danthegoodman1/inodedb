# InodeDB

InodeDB is a transactional metadata store whose keyspace is a tree of inodes. Directories map names to inodes, and files hold up to 64 KiB of opaque bytes. Renaming a directory rewrites one entry, however much lies beneath it. Clients speak gRPC to a Rust server. One server writes at a time, keeping its records in SQLite, RocksDB, lionturtle or FoundationDB through one storage trait; FoundationDB adds replicated durability.

**Best fit:** metadata for systems that keep their data in object storage and commit by renaming directories. The first user is a ClickHouse object-storage disk, with InodeDB holding the file tree and S3 the blobs ([ClickHouse integration](#clickhouse-integration)).

**What it provides:**

- **O(1) rename** of a directory of any size, and atomic batches of POSIX-style operations with per-op conditions.
- **Coherent caches** in the server and in clients, kept current by a changefeed, so most reads cost no round trip.
- **Subtree keys:** each key acts as a chroot onto one directory.
- **A changefeed** of a subtree from any retained version.
- **Safe freeing:** a file's bytes, and whatever they reference outside InodeDB, outlive every reader that might still use them.

## Alternatives

| Option | Choose it when |
|---|---|
| **ZooKeeper or ClickHouse Keeper** | Paths are few or never renamed. Both commit multi-op transactions atomically, but neither has a rename: moving a subtree rewrites every node in it. |
| **etcd** | Keys are flat. Renaming a prefix rewrites every key under it. |
| **FoundationDB's directory layer** | You need movable key prefixes inside your own FoundationDB application, and no file semantics, caching or scoped keys. |
| **JuiceFS** | You want a POSIX filesystem through FUSE. Its metadata engine uses the same inode model, behind the filesystem. |
| **ClickHouse `plain_rewritable`** | One writer, no shared hardlinks, and no extra service. It keeps its directory map in the bucket and copies objects to make hardlinks. |

## Data model

### Inodes

Every directory and file is an inode with a 64-bit id. Ids are never reused. An inode record holds:

- **kind:** directory or file.
- **version:** the commit version of the inode's last change ([Versions and consistency](#versions-and-consistency)).
- **mtime:** milliseconds since the Unix epoch.
- **mode:** 32 bits the caller stores for its own use, such as `chmod` bits. Keys control access ([Auth](#auth)), and InodeDB ignores the mode.
- **Files only:** a link count, a size, and a value of up to 64 KiB. Each value is stored under the version that wrote it, so replacing a value leaves the old one in storage until it is freed ([Leases and freeing](#leases-and-freeing)).
- **Directories only:** the parent directory's id. The root is its own parent.

### Entries

A directory entry maps `(parent id, name)` to a child id, the child's kind, and the version that created the entry. A name is 1 to 255 bytes, contains neither `/` nor NUL, and is neither `.` nor `..`. Listings are in byte order of names.

A directory has exactly one entry naming it. A file can have many, one per hardlink, and each link also writes a back-pointer from the file to its parent directory and name, so the server can find every directory that holds a file.

### Paths

A path is names joined by `/`, resolved from the root of the caller's key. A leading `/` names that root. Paths contain no `.` or `..`, so a key can name only what lies under its root.

### Handles

Responses name inodes by handle: the inode id and a 16-byte MAC that binds it to the caller's key. The MAC is AES-256, under a secret the writer stores when it creates the tree, of the one block holding the key's id and the inode id; a block cipher over a single fixed-length block is itself a MAC. Each handle goes out once: an inode inside a listed entry or a change leaves its handle out, and the client takes the entry's or the change's. A handle works only with the key it was issued to, and only while the inode lies under that key's root. Every read by handle follows the inode's parents up to the tree's root, through a file's back-pointers or a directory's parent id, and succeeds only when the key's root is among them; a freed inode is checked against the directories its free-queue entry recorded. With directories and back-pointers cached, the check costs under 100 ns, and making and checking a MAC about 30 ns (`BENCHMARKS.md`).

Reads by handle reach files whose last link is gone, and old values that a `Put` replaced ([Leases and freeing](#leases-and-freeing)).

## Operations

| Op | Does | Fails with |
|---|---|---|
| `Mkdir(path)` | Creates a directory. `Parents` creates missing ancestors; `IfAbsent` makes the op succeed without change when a directory already has the name. | `Exists`, `NotFound`, `NotDir` |
| `Put(path, value)` | Creates a file, or replaces an existing file's value in place, so every link sees the new bytes and the old value is freed. The mode is `Create`, `Replace` or `Upsert` (the default). Sets the file's mtime to the commit's time unless the op gives one. | `Exists` (`Create`, whatever the name holds), `NotFound` (`Replace`), `IsDir`, `TooLarge` |
| `Link(from, to)` | Gives a file another name and raises its link count. A directory source fails only after `to` resolves, as in Linux. | `Exists`, `NotFound`, `IsDir` |
| `Unlink(path)` | Removes a file's name and lowers its link count. `IfExists` makes the op succeed without change when the name is absent. | `NotFound`, `IsDir` |
| `Rmdir(path)` | Removes an empty directory. Takes `IfExists`. | `NotFound`, `NotDir`, `NotEmpty` |
| `RemoveAll(path)` | Removes a file, or detaches a directory and everything under it in one step ([Removing subtrees](#removing-subtrees)). Takes `IfExists`. | `NotFound` |
| `Rename(from, to)` | Moves an entry. The flag is `Replace` (the default), `NoReplace` or `Exchange`. | See below |
| `SetAttr(path)` | Sets mtime, mode or both. | `NotFound` |

Any op can carry a condition ([Batches](#batches)). An op that fails, or whose condition fails, is skipped, and the rest of its batch goes on. An op that would remove, move or replace the key's own root fails with `Invalid`.

**Rename** follows `rename(2)`:

- With `Replace`, a file replaces a file, whose inode loses a link, and a directory replaces an empty directory. A directory onto a file fails with `NotDir`, a file onto a directory with `IsDir`, and a directory onto a non-empty one with `NotEmpty`.
- `NoReplace` fails with `Exists` when `to` exists.
- `Exchange` swaps two existing entries of any kinds.
- Moving a directory into its own subtree fails with `Invalid`, and so does exchanging a directory with one of its descendants. The writer checks by following parent ids from each destination's parent up to the root.
- Renaming an entry onto itself, or onto another link to the same file, does nothing. Under `NoReplace` it fails with `Exists`, since the target exists.
- Errors come in Linux's order: both parent paths resolve before the source's entry is looked up.
- Rename leaves the moved inode's mtime unchanged.

**Directory mtime follows POSIX.** A commit sets the mtime of every directory whose entries it changes to the commit's time.

**Freeing.** A file is freed when its last link goes, through `Unlink`, `RemoveAll`, or a rename that replaces it. A directory is freed when `Rmdir`, `RemoveAll` or a replacing rename removes it. A file's old value is freed when `Put` replaces it. Freed inodes and values enter the free queue.

## Batches

A `Commit` carries an ordered list of ops. The writer applies them in order, each against the state the ops before it left, and commits what applied as one transaction.

- **Per-op outcomes.** An op applies when its condition holds and the op succeeds. Otherwise it is skipped: it writes nothing, its result names the predicate or error that stopped it, and the rest of the batch goes on.
- **Cascades.** Each op sees only what the ops before it applied, so a skip can cascade. When a `Mkdir` is skipped, a later `Put` into that directory fails with `NotFound`.
- **Conditions.** An op's condition is a list of predicates that must all hold: `Exists`, `Absent`, `IsFile`, `IsDir`, `Empty` or `VersionIs(v)` on any path under the key's root, and `Applied(i)` on an earlier op. `Applied` ties an op to another whatever the state shows.
- **Batch conditions.** A batch's `when` predicates join every op's condition. A writer fences out stale writers with a `when` on an owner file's version, so a stale writer's batch applies nothing.
- **All or nothing, on request.** With `all_or_nothing`, any skipped op makes the whole batch apply nothing. A batch whose ops must land together, such as a ClickHouse part, sets it.
- **Atomic visibility.** Whatever a batch applies becomes visible at once, at one version. A batch that applies nothing writes nothing and takes no version.
- **Results.** The commit version, and for each op either its handle or why it was skipped.
- **Batch errors.** Only a problem with the batch as a whole rejects it: a malformed op, a key without `write`, a batch over the limits, or `NotWriter`. The status names its reason in the `inodedb-error` metadata key: `malformed`, `too-many-ops`, `too-large` or `not-writer`, which carries the writer's address in `inodedb-writer`.
- **Execution.** The writer runs batches one at a time, in arrival order, so batches never conflict with each other ([The writer](#the-writer)).
- **Request ids.** The client gives every batch a random 128-bit request id, so a retry gets the first answer; a batch sent without one (zero) applies each time it arrives. A network can lose a response, and a blind retry of a rename that already succeeded would be skipped with `NotFound`.
  - When a batch applies anything, the writer records its version and result under the id in the same transaction. A large result spans several keys, and the record's expiry key says how many.
  - A record lasts `request_ttl` (10 minutes by default, 5 at least). Each group deletes records past their lifetime, ahead of its own writes, naming each key, so the deletions read nothing and leave no range tombstones; a restarted writer loads the ids of every live record, so a new id needs no read. A starting writer deletes none for a lifetime on its own clock, so every record it finds lives a whole lifetime with no record's age compared across servers' clocks.
  - A batch that applies nothing writes nothing, so the writer keeps its answer in memory for the same lifetime. A writer that restarts runs such a retry again, against the state it finds.
  - The client keeps retrying for less than the lifetime, and takes an answer only to the try it is waiting on. A retry it sends to a restarted writer replaces the try that writer never answered, so no client hears from two runs of one batch.
- **Limits.** A batch holds at most 10,000 ops and writes at most 8 MiB, counting everything it writes: values, inode records, entries, back-pointers, its log entry and its recorded result. A value is at most 64 KiB. The same limits hold on every backend and leave room under FoundationDB's 10 MB transaction limit, so a batch that works on SQLite works on FoundationDB.

A batch that publishes a part, with a skip that cascades:

```text
when VersionIs("t/.owner", 7)                          joins every op's condition
0  Mkdir("t/all_5_5_0")                                applied
1  Put("t/all_5_5_0/data.bin", …)  if Applied(0)       applied
2  Put("t/all_5_5_0/count.txt", …) if Applied(0)       applied
3  Rename("t/a", "t/b") NoReplace                      skipped: Exists
4  Unlink("t/b/x")                 if Applied(3)       skipped: Applied(3)
```

## Versions and consistency

Each commit gets the next version from a 64-bit counter the writer keeps. A new writer continues from the last committed version.

- **Commits are strictly serializable,** since one writer applies them in order.
- **Every response carries the version it reflects.**
- **Reads choose their consistency.** `Latest` reads the newest committed state from the writer's cache. `AtLeast(v)` accepts any state at `v` or later from any server, so a client reads its own writes through a standby. `Cached`, in the client library, answers from the local cache at whatever version it holds.
- **A version past the writer's fails at once.** Clients see only committed versions, so the writer has always reached a `v` a client names. A later one means the client saw another store, and the read fails with `OutOfRange` rather than waiting. A standby waits for its feed to reach `v`.
- **Snapshots** belong to the client library: every read through one sees a single version ([Snapshots](#snapshots)).

## Changefeed

Each commit writes its state changes and one log entry in the same transaction, so the state and the log always agree. The entry is keyed by the commit's version, and an entry too large for one value spans several keys under that version.

**An entry records effects after path resolution:** entries added and removed, with parent, name, child and kind; inodes changed, with their new attributes; inodes and values freed; subtrees detached; keys minted and revoked; free-queue entries acknowledged. Each effect carries the state it replaced as well as the new state, so a client can rebuild earlier versions for its snapshots. A subscriber applies effects without walking paths, and a rename is two entry effects, O(1) like the rename itself. Values stay out of the log; a subscriber fetches the values it wants.

**`Watch(path, from)`** streams the effects inside a subtree from a version on, in order. The stream follows the directory by inode through renames, and `from` of zero starts after the current version.

- **Routing needs no state.** Each entry records, for every directory its effects name, the directory's ancestor chain before the commit and after it. An effect belongs to the subtree when its directory was in it before the commit or is after. An effect on a file lists every directory that holds it, from its back-pointers, and belongs when any of them does.
- A directory renamed into the subtree arrives as `Entered`, which tells the subscriber it has seen nothing beneath it. A directory renamed out arrives as a removal.
- **Progress.** Once the stream catches up, it sends a change with no effects at the newest version, so a subscriber knows nothing in its subtree changed up to there.
- A subscriber sees handles for its own key and nothing outside its root: an effect on a file lists only the holders inside the subtree.
- **The tail.** Each server holds its newest 4,096 log entries, up to 64 MiB of them encoded, from its writer's commits or its standby's reads of the log. The first stream to read an entry decodes it for the rest, so a server with no streams decodes nothing. A stream reads from the tail while it holds where the stream is, and storage otherwise, so streams that keep up cost no storage reads however many there are. Once caught up, a stream waits for the server's reads to reach the next version. The tail serves an entry only while it is younger than half the minimum retention, so a stream gets `Compacted` exactly when storage no longer holds where it starts.
- **Ends.** The stream announces only versions the writer has committed, so a read asking for any version it sent finds it. It ends with `Unauthenticated` when its key is revoked or expires, with `NotFound` when the watched directory leaves the key's tree, and with `Unavailable` when the server shuts down or its writer stops.

**Truncation.** The server keeps every log entry for `log.min_retention_ms` (five minutes by default), longer while a live lease's position still needs it, and at most `log.max_retention_ms` (an hour by default). A lease's position is the version its reader has applied, so it needs every entry after it. Truncation starts once the writer has loaded the stored leases. A watch from a position the log no longer holds gets `Compacted`, and the stream ends. The subscriber starts over with an empty cache at the current version and fills it as reads miss.

## Caching

The server and the client library share one cache implementation, in `core`. It holds three kinds of record:

- **Directories,** each a map of its cached names. Resolving a path walks one map per component, about 190 ns for six levels (`BENCHMARKS.md`). Renaming a directory moves one name from one map to another, which carries every cached path beneath it. A directory whose whole listing is cached is marked complete, so it answers lookups and listings by itself and keeps no record of names it loses. Until then, negative entries record names known to be absent.
- **Inode records,** with each file's back-pointers.
- **Values,** by inode and value version, so a cached value never goes stale.

**Budgets.** The cache charges each record at least what the allocator holds for it, eviction's bookkeeping included: a directory 570 bytes and each name 80 beside its own bytes, an inode record 250, a file's links 360 and each link as a name, and a value 200 beside its bytes (`BENCHMARKS.md`). Directories, inode records and links share `tree_bytes`, 1 GiB by default; a cached file is charged about 980 bytes, so the default holds about a million. Values share `value_bytes`, 256 MiB by default. A cache within its budgets is within that much memory. The writer keeps only the ids of live request records, about 100 bytes each, and a retry reads its record back from storage.

**Eviction.** Past a budget, the cache evicts by SIEVE. A hand sweeps from the record written longest ago toward the newest, then starts again from the oldest. It drops each record it reaches, except one a read used since the hand last passed it, which it passes over once. A write moves a record to the newest place and keeps whether a read used it. Records that reads keep using stay cached, and records written once and never read go first. A dropped record costs one storage read when next needed, and dropping a directory drops its completeness with it, so the cache never answers from a partial listing. The writer evicts after each input it handles, sparing what it wrote since the batch at the queue's front, or any query, first needed a read, so a batch that needs more than the budget holds still finishes.

**The cache keys on directory entries.** A flat map from path to inode would answer a lookup in one probe, but MergeTree renames a directory on every insert. The map would have to drop every path under the renamed directory, or check each ancestor's version on every hit, which costs as much as the walk.

**Coherence.** A cache holds one current version and advances by applying the changefeed in order. The writer applies each commit to its own cache as the commit returns, and standbys and clients tail the log.

**A subscriber's cache** holds only its subtree:

- It forgets what leaves: a child a name stops reaching, unless a name in the subtree gains it in the same commit and no directory leaves in that commit, since the directory that gained it may have gone too. What lies outside the subtree changes without the feed showing it. When a directory leaves alive, the cache keeps only what its root still reaches through cached directories, so nothing stays cached beneath a directory outside, even behind a directory the cache had dropped; a directory that enters therefore brings nothing cached with it. A directory freed or detached never comes back, but a file beneath it may, through a link elsewhere, so removing one forgets it and everything cached beneath it, which the undo keeps while a snapshot is open ([Snapshots](#snapshots)).
- A read fills the cache at the version it reflects. One at the cache's version fills at once; an older one moves forward through recent changes, unless the cache forgot something since; a newer one waits until the feed reaches its version.

**Reads the cache holds cost no trip to the writer's task.** The request's own task answers one under a shared lock, which the writer's task takes exclusively for the microseconds it changes state, so reads never wake another thread. The checks are the writer's own: it must be ready and within its lease, by the clock at that moment; the floor must be at or below its committed version; and the key must hold `read`. Anything else, including a cache miss, goes to the writer's task as a query. A standby answers the same way for reads at a version it has reached.

**The writer's cache holds every committed batch,** so the core executes batches against it and reads storage only for keys the cache lacks. A batch that lacks keys waits for them, and the batches queued behind it, up to 1,024 ops of them, read what they lack in the same round, so a queue of batches that each miss costs a round of reads rather than one per batch, and cached work queued behind costs each round a bounded stall. Eviction spares what reads in progress fetched, so work that needs many reads, such as freeing a detached subtree, always finishes.

**Preload.** A writer or standby that starts reads the tree into its cache while it serves: every directory's entries, then every inode record, then every file's back-pointers, in pages of 16,384 keys, then every value, in pages of 1,024. Entries come first because they resolve paths, so in a tree whose inode records outgrow the cache, a path resolves from the cache and reads only the record at its end. Each page is a read like any other, moved to the cache's version through the commits since. A directory whose entries fall in one page arrives complete, and so does an empty directory the entries pass over once an entry has named it, so the cache answers that a name is absent without a read; a larger directory arrives as known names. The preload stops at the first record its budget has no room for, so it never evicts what it read before. Stats as a server starts run at 1.5 to 3.5 times the rate of cold ones (`BENCHMARKS.md`).

**Settings,** separate for the writer and each client: tree bytes, value bytes, and undo bytes for snapshots. A server takes the first two, and whether to preload, from its configuration's `[cache]` table, and a client passes a `Budget` to `cached_within`.

### Snapshots

A client snapshot pins version V while the cache moves on.

- **Undo records.** While a snapshot older than an effect is open, the cache keeps the effect's prior state as an undo record. It keeps one for every effect in its feed, cached key or not.
- **Reads.** A read through the snapshot takes the key's earliest undo record newer than V, which holds the state at V. Without one, the key hasn't changed since V, and the current state answers.
- **Misses.** A cache miss goes to a server by handle, so a rename since V doesn't send it elsewhere. When the answer reflects a version newer than V, the client waits until its feed reaches that version, so any undo record for the key exists, and then applies the rule above to the answer.
- **What leaves the feed's sight.** A directory that moves out of the subtree, or is removed, can change in storage without the feed showing it. As the cache forgets it, the cache keeps what it held of it and of everything cached beneath it, as of the commit it left in. A read through a snapshot older than that commit resolves beneath it from undo records up to that commit and the kept states alone; what they lack fails with `SnapshotTooOld`.
- **Old values** come from the free queue by handle and value version. The snapshot's lease holds them ([Leases and freeing](#leases-and-freeing)).
- **Budget.** Undo records and kept states last until the last snapshot older than them closes. When they pass `undo_bytes` (64 MiB by default), the oldest snapshots end, and reads through them fail with `SnapshotTooOld`. So does every snapshot when the cache starts over. The lease holds an ended snapshot's version until the snapshot drops, since its holder may still use what it read.

## Auth

A key is an id and a secret. The server stores a hash of the secret with the key's root, grants, expiry, and the key that minted it. Clients send the key in gRPC metadata as `authorization: Bearer <id>.<secret>`.

- **Root.** A key's root is a directory inode, and every path resolves from it. The root follows its directory through renames. A commit that frees or detaches a key's root revokes the key and every key minted from it. Keys are few, so the writer finds them by checking each key's root against the removed directory, a cost that falls on that commit alone.
- **Grants.** `read` covers `Stat`, `Read`, `List`, `Watch` and leases. `write` adds `Commit` and the free queue. `admin` adds minting and revoking keys.
- **Minting.** A key can mint keys rooted at or under its own root, with grants and expiry no wider than its own. A minted key expires with its minter at the latest. A key may revoke itself and the keys minted from it, and revoking a key revokes every key minted from it. A writer mints its replicas' read keys.
- **Bootstrap.** `inodedb --init-key <file>` gives a store with no keys its first admin key, rooted at the tree's root: the key in the file, or a new one written there, readable by its owner alone.
- **Form.** A key is `<id>.<secret>`: the id is 16 hex digits, and the server mints 128-bit secrets. The writer holds every key's record from startup on and checks each request's key when the request runs, so a request that runs after a revocation fails.
- **Boundaries.** `Link` and `Rename` resolve both paths from the caller's root, so a key keeps everything it moves inside its own tree. Reads by handle check the same boundary ([Handles](#handles)).

## Leases and freeing

Freeing waits for readers. A reader such as a ClickHouse replica holds file handles and blob keys from versions it has read, and it reads S3 directly, where InodeDB can't see.

**Free queue.** A freed inode, or a value that `Put` replaced, gets an entry in the free queue under the version that freed it. The entry is small: the inode id, the value's version when a value was freed, and the directories above the inode at that moment, so a consumer claims only entries from its own subtree. The inode record and value stay in storage until the entry is acknowledged, so downstream code can read what the value references, and readers can still read them by handle.

**Leases.** A reader holds a lease: its key, the oldest version it still uses, its changefeed position, and an expiry. The client library sends a heartbeat every 10 seconds, and a lease lasts `lease_ttl` (30 seconds by default) past its last heartbeat.

- The oldest version in use is the lowest version among the reader's open snapshots, or its cache's version when it has none. A new snapshot starts at the cache's current version, which is at least what the lease last reported, so opening one needs no call.
- The oldest version only moves forward: the server refuses a heartbeat that would lower it.
- A dropped connection leaves the lease to run out its TTL, so a network blip costs nothing. An expired lease can't be renewed; its client gets `LeaseExpired`, drops its handles, and reloads.
- A new lease starts at or after the release horizon, or fails with `TooOld`. A reader can instead open one at the newest committed version, which the horizon never passes; the cache does, and starts its feed there.
- A client counts a lease's lifetime from when it sent the heartbeat or opening, which the writer runs no sooner. Dropping a lease releases it.
- Only a lease guarantees that freed inodes and old values stay readable. A reader without one can find a queue entry acknowledged, and its handle read failing with `NotFound`, at any time.

**Release.** A queue entry freed at version F becomes claimable once every live lease's oldest version has reached F. The release horizon is the highest F released so far. The horizon is global: one long reader delays freeing for every tenant ([Open questions](#open-questions)).

**Claiming.** A consumer with `write` claims released entries under its root, deletes what they reference, and acknowledges them, which deletes each entry with the records it kept. A claim expires after `claim_ttl` (60 seconds by default) unless acknowledged or renewed, and the entry returns to the queue, so a consumer that dies loses nothing. A claim can expire partway through a delete and be claimed again, so deleting what an entry references must be idempotent.

- A claim reads what each entry kept through the entry itself, within the directories it recorded, so a replaced value stays readable after its file's own entry is acknowledged or its file moves away.
- A claim lasts its time once every entry is acknowledged, so a retried acknowledgement succeeds. Each writer draws claim ids from its own id, so an acknowledgement meant for an earlier writer's claim finds nothing.
- A consumer's next claim scans on from where its root's last one stopped, and back from the queue's start once it reaches the horizon, so entries other roots leave in the queue cost one pass rather than one per claim.

**Where leases live.** The writer keeps leases in memory, runs release, and writes each heartbeat to storage with its next commit, so a new writer finds them; when no batch is coming, a commit of heartbeats alone goes out at most once a second. A new writer gives every stored lease a fresh TTL before it releases anything, so no lease deadline is ever compared with another server's clock, and answers lease calls only once it has loaded them. A writer that starts over after a failed first commit loads them again, and what storage holds replaces all it held and queued before.

- **The release record** holds the horizon claims may use and the next lease id. A lease's opening commits the record past its id, and a claim releases nothing above the horizon the record holds, committing a newer one first. A starting writer begins from the record, so it never hands an id out again, and never releases less than an earlier writer let a consumer claim.

### Removing subtrees

`RemoveAll` on a directory removes its entry and records the subtree as detached, in one commit, which also revokes the keys rooted inside the subtree. The detach record holds the directories above the subtree as it detached. When the writer has nothing else to do, and every 100 milliseconds however busy it is, it frees part of the subtree, deepest first, in transactions of at most 1,000 inodes, and frees the top last with its detach record. Each step reads only as many of a directory's entries as it can free. A file with links outside the subtree loses only the links inside it. Each freed record names the directories above it inside the subtree, then those the detach record holds, since the directories that held the subtree may since have moved or gone. The writer reads each directory's entries and what freeing them needs in one round, so a subtree costs a round of reads per directory. The changefeed routes the freeing by those same directories, so a subscriber that saw the subtree before it detached sees it freed, and no other does. Contents freed after the detach carry later free versions than the detach itself, so leases hold them at least as long.

## Storage

### The trait

InodeDB's semantics live once, in `core`, above an ordered, transactional key-value trait. Each backend implements only the trait. Sketch:

```rust
pub trait Storage: Send + Sync + 'static {
    type Txn: Txn;

    /// Runs every read against one committed state.
    async fn snapshot_read(&self, reads: Vec<KvRead>) -> Result<Vec<KvResult>, StorageError>;
    /// Begins a transaction at the latest committed state.
    async fn begin(&self) -> Result<Self::Txn, StorageError>;
    /// A handle for one writer's term, whose reads may start from its latest commit.
    fn for_writer(&self) -> Self;
    /// Log entries from version `from` on, in version order.
    async fn read_log(&self, from: Version, limit: usize) -> Result<Vec<LogEntry>, StorageError>;
    /// Resolves with the latest version once a commit later than `v` exists.
    async fn wait_past(&self, v: Version) -> Result<Version, StorageError>;
    /// Deletes log entries older than `below`.
    async fn truncate_log(&self, below: Version) -> Result<(), StorageError>;
    /// Commits a group if the head record holds `expect`. The default runs a
    /// transaction; a backend may do it in one call.
    async fn commit_group(&self, expect: Option<Vec<u8>>, writes: Arc<WriteSet>) -> CommitOutcome;
}

pub trait Txn: Send {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, StorageError>;
    async fn scan(&mut self, range: KeyRange, limit: usize) -> Result<Vec<(Bytes, Bytes)>, StorageError>;
    fn set(&mut self, key: &[u8], value: &[u8]);
    fn clear(&mut self, range: KeyRange);
    fn append_log(&mut self, version: Version, entry: &[u8]);
    /// Fails with Conflict, Unknown, TooLarge or Unavailable.
    async fn commit(self) -> Result<(), CommitError>;
}
```

The log is keyed by version, so `truncate_log` is one range delete, and the engine reclaims the space in its own compaction.

**The contract:**

- **Order.** Keys and scans order bytes lexicographically.
- **Isolation.** Committed transactions are strictly serializable. A commit fails with `Conflict` when another commit, after this transaction began, wrote a key it read or scanned. With one writer, only the fence produces conflicts.
- **The writer's handle.** The engine makes one with `for_writer` at each takeover. Its reads and transactions start from a state at least as new as its latest successful commit, and from the latest after a failed one. FoundationDB's reuses the read version of its latest commit, or of its latest request for one, for up to a second, so a round of reads or a commit skips a round trip to the cluster; the other backends always read the latest. A group that follows the handle's own commit skips reading the head too: it runs at that commit's version and declares a read of the head, so any commit to the head since then conflicts. Fencing covers what such reads miss: a transaction that read state another writer has since changed conflicts, and the writer answers reads only within its lease ([Fencing](#fencing)). Standbys read the latest through the engine's other handle.
- **The log.** Entries read back in version order.
- **Unknown outcomes.** `Unknown` means the commit may or may not have applied.
- **Durability.** A commit is durable before it returns.
- **Group commits.** `commit_group` applies a group's writes in one transaction when the head record holds what it expects, and answers `Fenced` otherwise. A transaction that read what it writes conflicts. SQLite, RocksDB and lionturtle read the head and write under their commit locks in one blocking call, so a commit costs one trip to a blocking thread and no copy of its writes.
- **Limits,** matching FoundationDB on every backend: keys up to 9,000 bytes, which leaves room for a FoundationDB store's prefix within its 10,000-byte keys, values up to 100,000 bytes, and transactions up to 10,000,000 bytes. FoundationDB also ends a transaction after 5 seconds; the writer's take milliseconds, and only a whole-store check (`idb fsck`) reads for longer.

### Backends

| Backend | Use | How it keeps the contract |
|---|---|---|
| **memory** | Simulator, unit tests | An ordered map under one lock, with optimistic conflict checks. Hooks inject unknown outcomes, applied or not, and outages; the simulator adds latency and writer crashes around it. |
| **SQLite** | Local development, tests, small single-server deployments | rusqlite with SQLite bundled, WAL mode, `synchronous = FULL`, syncing with `fdatasync`. The WAL file is written to 128 MiB when the store opens. A thread of the store's own copies the WAL into the database file and, once it passes half its size, briefly holds commits to send them back to its start, so commits overwrite written blocks. The last handle's drop ends that thread, so the store's lock is free once the drop returns. Its file lock admits one writing process. |
| **RocksDB** | Single-server deployments | The log in its own column family, the WAL synced on each commit, and its lock file admitting one process. Five WAL files are written when the store opens and overwritten from then on, and a flush comes due once the live WAL reaches three memtables' worth, so commits stay within those files. A crash while they are written leaves a store that still opens. Point reads check a Bloom filter in each table and memtable. A range clear leaves a range tombstone that every later read of the memtable sorts through, so the writer avoids them. Behind the `rocksdb` feature. |
| **lionturtle** | Single-server deployments | One keyspace: the tree beneath `00` and the log beneath `01`, keyed by version. Every commit is a durable write that returns once it is readable. Each log segment is zero-filled ahead of its writes, so commits overwrite written blocks. lionturtle deletes key by key, so a range clear reads what it clears, and truncation deletes the log oldest first, a chunk at a time, so a crash partway leaves its newest entries whole. A write that fails stops the store, which then answers `Unavailable` until the server restarts and recovers from the log, because the failed batch stays readable in memory though it may never reach the disk. A lock file beside the store's directory admits one process. |
| **FoundationDB** | Replicated deployments, with standbys | A store under one key prefix: the tree beneath `<prefix> 00`, the log beneath `<prefix> 01` in chunks of up to 100,000 bytes keyed by version and chunk number, and the newest logged version at `<prefix> 02`, which `wait_past` watches. Stores share a cluster without touching. Behind the `fdb` feature. |

- **Every backend has one writer.** A batch takes microseconds of CPU, so the durability wait bounds the commit rate: a sync locally, a commit of a millisecond or more on FoundationDB. Group commit spreads that wait over many batches ([The writer](#the-writer)).
- **A sync of written blocks costs the device alone.** One that grows a file or converts an allocated extent also commits the file system's journal, which costs 2.5 to 4 times as much on local NVMe and twice on gp3 (`BENCHMARKS.md`, "Devices"). So SQLite, RocksDB and lionturtle keep their logs in files already written.
- **RocksDB and FoundationDB link C++ libraries,** so they sit behind cargo features. lionturtle is Rust, and every build has it. `core` and the simulator build without any of them.
- **InodeDB uses existing engines.** An engine of our own, a data file and a WAL, would write each change once, since its WAL could serve as the log, and would let the simulator inject torn writes and lost syncs. It would also need its own page format, recovery, checksums and replication. Replication decides it: blob keys are random, so losing the metadata loses the data, and FoundationDB already replicates. `append_log` and `read_log` stand apart in the trait so that such an engine can arrive later as another backend.

### Key layout

Every key starts with a format byte.

```text
i <ino>                         inode record
v <ino> <version>               file value, under the version that wrote it
e <parent> <name>               entry: child, kind, version
p <ino> <parent> <name>         a file's back-pointer, one per link
l <version> <part>              log entry, in backends that keep the log as keys
f <version> <ino> [<value>]     free-queue entry, naming a value's version when a value was freed
d <version> <ino>               detached subtree awaiting freeing → the directories above it
s ...                           keys, leases, request ids, head (also the writer record), release record
```

Numbers are big-endian, so ranges scan in numeric order. Values sit apart from inode records, so `Stat` and plain listings never read them. Inode ids come from a counter the writer updates in each commit that creates an inode.

### Conformance suite

`storage-conformance` states the contract as tests generic over `Storage`:

- byte ordering, ranges, and each limit at and past its bound;
- read-your-writes within a transaction, and snapshot reads across transactions;
- conflicts: write skew fails, a scan conflicts with an insert into its range, and a transaction that read the head record fails once another commit changes it;
- a writer's handle: it reads its own commits, and once another handle commits, it conflicts on what changed and then reads the latest;
- `commit_group`: it checks the head, applies its writes in order, applies nothing when fenced or past the limits, and conflicts with transactions that read what it writes, on a writer's handle too;
- log order, truncation and `wait_past`;
- `Unknown` from backends that report it, and durability across close and reopen.

Every CI run tests memory, SQLite, RocksDB and lionturtle, memory's injected unknown outcomes and outages, and FoundationDB in a container (`scripts/fdb`). The suite carries the simulator's results to real deployments: the simulator shows InodeDB correct over the contract, and the suite shows each backend keeps the contract.

## The writer

One server writes. On FoundationDB, other servers run as standbys.

- **Process.** tonic's generated services over `crates/transport`, on tokio, configured by a TOML file that names the backend.
- **Batches.** The core runs a batch against the cache and returns either a write set and response, or the keys it lacks. The writer reads missing keys from storage and runs the batch again; a warm cache rarely misses, and a run costs microseconds.
- **Group commit.** One storage transaction is in flight at a time, because FoundationDB orders no two independent transactions, and a batch must never commit without the batches it built on. While a transaction commits, the writer runs the batches that arrive, in order, against the cache plus every change not yet committed, and gathers them into groups. Each batch keeps its own version and log entry, and every group reads and sets the head record. The writer's task takes the requests that have queued, up to 256, in one turn, so what arrived together commits together.
- **Group size.** A group closes at `group_max_bytes` (9 MB, also its most, counted as a batch's limit counts), which keeps it under FoundationDB's transaction limit with the head and the group's expired-record deletions. The batches after it start the next group, and groups commit one at a time, in order.
- **Answers.** A batch hears back only after its group commits. A batch that applies nothing joins no group, but its answer still waits for every group it was evaluated against, since its skips depend on their changes. `Latest` reads see only committed groups.
- **The head record** holds the last committed version and the next inode id, and doubles as the writer record: the writer's random id, its epoch, its address, and a count of its commits. Every transaction the writer commits expects the head it last wrote and writes the next one, so each commit changes the record.
- **Unknown outcomes.** When a group's commit returns `Unknown`, the writer commits a transaction that reads and rewrites the head record. The head this group would write means the group committed; the head it expected means the group never will, because this transaction conflicts with it. Any other head means another writer committed, and the writer stops.
- **Failed groups.** When a group fails, the writer drops its changes and those of every batch run after it, including batches that applied nothing and batches rejected as too large, and runs them all again. The server waits before reporting a failure, longer after each in a row.
- **Reads straddle commits.** A read issued at the committed version may return after more groups commit, or see a group still in flight. The writer moves its result to the committed version through the changes in between, and stops if any are missing, since only another writer's commit leaves a gap.
- **Background work:** log truncation, request-id expiry, freeing detached subtrees, lease expiry and release.
- **Time.** mtime and lease deadlines come from the writer's clock, which it passes into the core. A standby times its wait on a monotonic timer started from the wall time, which never runs fast; the writer takes the later of that timer and the wall clock, which never runs slow. So a step of the wall clock, or a host that sleeps, can only end a lease early or lengthen a wait.

### Fencing

On SQLite, RocksDB and lionturtle, the database's lock admits one process, and a starting server writes at once. On FoundationDB, every server starts as a standby, and the head record does the fencing.

- **Renewal.** Every commit renews the writer's lease, `writer.lease_ms` (5 seconds by default) from when the commit went out. An idle writer commits the head alone every `writer.renew_every_ms` (a second by default).
- **Takeover.** A standby reads the head record every 100 ms. Once it has seen the record unchanged for the lease plus `writer.takeover_margin_ms` (a second by default) on its own clock, it starts a writer that reads the store and commits the next epoch, expecting the head it watched. A head that changed meanwhile means the writer renewed, and the standby goes back to watching. An empty store has no writer to wait for.
- **Fenced commits.** Every commit expects the head its writer last wrote, so once a successor commits, every commit of the old writer conflicts. It hears `Fenced`, answers what it holds with `Unavailable`, and goes back to being a standby.
- **Fenced reads.** The old writer answers reads, and batches that applied nothing, only within its lease. A successor takes over only after the head has gone unchanged for longer than that, so every read the old writer answers comes before its successor's first commit, and `Latest` reads from its cache stay current. This assumes clock rates drift apart by less than the margin over a lease. Reads that arrive once the lease lapses wait for the next renewal.
- **Release.** A server that shuts down cleanly first finishes the requests in progress, then has its writer release the store: the writer answers everything else with `Unavailable`, lets its commit in flight finish, and commits its head marked released. A standby that reads a released head takes over at once, so a planned restart costs a poll of the head instead of the lease and the margin. The releasing writer answers nothing from the moment it starts, so every answer it gave comes before the released head, and so before its successor's first commit. A head its writer holds encodes as it did before releases existed.
- **Kubernetes** keeps a writer running, for example as a StatefulSet of a writer and standbys behind one service address. The fence keeps a second writer from committing whenever Kubernetes runs two.

### Standbys

A standby follows the log into a cache of the whole tree, and serves `AtLeast(v)` reads, waiting for its cache to reach `v`, and `Watch`. It answers writes, `Latest` reads, leases and claims with `NotWriter` and the address the head record names. It takes over with its cache, so failover costs the lease and margin plus a read of the store and the epoch commit.

## API

```proto
service InodeDB {
  rpc Commit(CommitRequest)       returns (CommitResponse);   // one atomic batch
  rpc Stat(StatRequest)           returns (StatResponse);     // many paths or handles
  rpc Read(ReadRequest)           returns (ReadResponse);     // many values
  rpc List(ListRequest)           returns (stream ListPage);  // one directory, paged
  rpc Watch(WatchRequest)         returns (stream Change);    // a subtree, from a version
  rpc Lease(stream LeaseBeat)     returns (stream LeaseState);
  rpc ClaimFreed(ClaimRequest)    returns (ClaimResponse);
  rpc AckFreed(AckRequest)        returns (AckResponse);
  rpc MintKey(MintKeyRequest)     returns (MintKeyResponse);
  rpc RevokeKey(RevokeKeyRequest) returns (RevokeKeyResponse);
  rpc Check(CheckRequest)         returns (CheckResponse);    // fsck, root admin keys only
}
```

- **The transport.** Client and server speak plain gRPC over HTTP/2, directly on the `h2` crate. The client sends a request's headers and message in one write from the caller's task. The server polls each request once on the connection's task: a request that needs nothing else answers there, in one write, with no task of its own, and only one that has to wait gets a task. So a read the cache holds wakes one server thread.
- Every op in a `CommitRequest` addresses a path. Reads address a path or a handle, and a read by handle can name a value's version, for snapshots.
- `List` takes a name to start after and a name prefix. It can return each entry's attributes and value, so a client loads a directory of small files in one call. Each page holds at most 1,024 entries and about 3 MiB, counting each entry's name, its value when asked for, and 128 bytes for the rest; a page holds one entry whatever its size. Pages after the first are at least as new as the one before, so a listing reflects at least its first page's version, as `readdir` does. Every page names the directory it lists, and pages after the first list that directory wherever renames take it, as `readdir` follows an open directory; a listing resumed by that handle does the same.
- A `CommitResponse` carries each op's outcome. Errors that reject a whole request are gRPC status codes carrying an InodeDB error code, and `NotWriter` carries the writer's address. A batch holding an op or predicate the server can't read is malformed. A missing, unknown, revoked or expired key gets `Unauthenticated`; a key without the grant, `PermissionDenied`; a handle issued to another key, `InvalidArgument`.
- Responses stay under gRPC's default 4 MiB message limit: listings page, a `Read` names at most 60 targets and a `Stat` at most 1,000, and the client splits larger requests and sends the parts at once.

## Client library

The Rust client handles connections, request ids, retries, the cache and the lease.

```rust
use inodedb::{Client, Cond, Op};

let db = Client::connect("http://meta:7070", writer_key).await?; // root: clickhouse/tenant-42

// Publish a part in one round trip.
let mut batch = db
    .batch()
    .when(Cond::version_is("tables/t1/.owner", owner_version)) // fence
    .all_or_nothing();
let part = batch.push(Op::mkdir("tables/t1/all_5_5_0"));
batch.push(Op::put("tables/t1/all_5_5_0/data.bin", blob_list).if_applied(part));
batch.push(Op::put("tables/t1/all_5_5_0/checksums.txt", checksums).if_applied(part)); // inline bytes
batch.push(Op::set_mtime("tables/t1/all_5_5_0", now).if_applied(part));
let commit = batch.commit().await?;
// commit.outcomes[i] says whether op i applied; once another writer takes over, none do.

// A replica with a cache kept current by Watch.
let replica = Client::connect("http://meta:7070", replica_key).await?.cached();
replica.wait_for(commit.version).await?; // read-your-writes across processes
let snap = replica.snapshot().await?; // holds its version until dropped
for part in snap.list("tables/t1").await?.entries {
    let path = [b"tables/t1/", &part.name[..]].concat();
    let files = snap.list_with_values(&path).await?; // answered from the cache
}
drop(snap);
```

- `Unavailable`, timeouts and lost responses retry with the same request id, for up to two minutes. `NotWriter` sends the request, and every later one, on to the address it names; after a failure it retries, the client goes back to the address it was built with, so a service address finds the writer again after a takeover. A listing cut off midway resumes after its last entry, in the directory it started on.
- `mint_key(root, grant, ttl)` returns a new key; `revoke_key(key)` revokes one and what it minted.
- The client passes the newest version it has seen as each read's `AtLeast`, so its reads never go back in time. `wait_for(v)` raises that floor once a server confirms it has reached `v`.
- Turning on the cache opens a lease and a `Watch`; the lease's heartbeats run on their own, whatever the stream does. The cache keeps each inode's MAC from the handles the server sends, so the handles it returns work with the server. A listing that fills it must be of the directory the cache expects at that path, so a rename the feed has yet to show fills nothing.
- `snapshot()` pins the cache's current version in the lease until the snapshot drops, and every read through it sees that version or fails with `SnapshotTooOld`.
- Op modifiers read as builders: `Op::mkdir(p).parents()`, `Op::put(p, v).create()`, `Op::unlink(p).if_exists()`, `Op::rename(a, b).exchange()`, `Op::put(p, v).if_(Cond::absent(q))`, `op.if_applied(earlier)`.

## CLI

```text
idb ls -l tables/t1
idb stat tables/t1/all_5_5_0/checksums.txt
idb mv tables/t1/tmp_insert_all_5_5_0 tables/t1/all_5_5_0
idb rm -r tables/t1/delete_tmp_all_1_1_0
idb watch tables/t1 --from 81234
idb key mint --root clickhouse/tenant-42 --grant read --ttl 30d
idb key revoke 9f3a6c21d0e4b785
idb fsck
idb fsck --sqlite data/db.sqlite
idb mount ./mnt
```

`idb` reads its endpoint and key from `INODEDB_ENDPOINT` and `INODEDB_KEY`.

- `idb watch` prints each change with its version and path: `+` for a name added, `-` for one removed, `~` for one replaced or a file whose value or attributes changed.
- `idb fsck` checks the invariants the simulator checks, over one snapshot of the whole store: through the server's `Check` call, under an admin key rooted at the tree's root, or in a SQLite store directly while no server runs on it. A FoundationDB store must read within one transaction's five seconds.
- `idb mount` is a read-only FUSE view of the tree under the key's root, for debugging: each call asks the server, and the kernel caches answers for a second. It needs the `mount` feature, and FUSE: `fusermount3` on Linux, macFUSE on macOS.

## Testing

Testing follows s3-accelerator's design: a deterministic core, a simulator that checks it against an independent model, and conformance suites that tie the simulator's assumptions to real systems.

### Core rules

- `core` does no I/O, reads no clocks and starts no threads. It takes requests, storage results and the time as inputs and returns actions. The server carries them out over gRPC and a backend; the simulator carries them out over its models.
- `clippy.toml` in `core` and `sim` forbids unordered collections, clocks and threads.
- Randomness reaches the core as values passed in: a writer's id and handle secret, and request ids. The server draws them from the system; the simulator, from the seed.

### Simulator

One thread runs clients with caches and snapshots, a writer, standbys that follow the log and answer reads at a version, the memory backend, and a network. A standby that takes over hands its cache and waiting reads to its writer. Clients read by path and, for stats, by inode, inside and outside their keys' roots. Half the reads a server gets take the path a request's task takes, answering from the cache between the writer's other steps. Each seed draws whether servers preload, the number of clients and standbys, the share of clients that only read, the op mix, and the fault rates. Faults:

- **Storage:** unknown outcomes, latency, and stalls long enough for a standby to take over while the old writer still runs. A writer reads storage as of its latest commit until a commit of its fails, so an old writer reads from before its successor's commits.
- **Servers:** crashes and restarts mid-group, mid-heartbeat and mid-release, and clean shutdowns that release the store.
- **Network:** delay, loss, duplication, reordering, partitions, cut watch streams.
- **Clocks:** rate drift within the writer lease's margin.

**The oracle** is a deliberately naive model: a map from full path to node and from node to content, whose renames rewrite every path under the source. It applies committed batches in version order, evaluating each op's condition and errors itself. It takes `core`'s op and query types, and shares none of its evaluation.

**Properties:**

1. Every response matches the model at the version the response reports, every op's outcome included, and a batch acknowledged before another began has the lower version.
2. Every acknowledged batch applies exactly once, through retries, unknown outcomes and takeovers.
3. A batch whose `when` fails, such as one guarded by a stale owner version, applies nothing, and so does an `all_or_nothing` batch with a skipped op.
4. A fenced writer commits nothing once its successor's epoch commits, and after its successor's first commit answers nothing from its state: no read, no batch that changed nothing, no refusal, heartbeat or claim.
5. `fsck` passes after every commit and every restart. Each entry's child exists with the kind the entry records. Each directory but the root has one entry naming it, in the directory its parent id names, and parent ids lead to the root without repeating. A file's link count equals its entries and its back-pointers. Every inode is reachable, detached or queued, and every stored value is its file's current value or queued. Log entries decode, run without gaps, and end at the head's version.
6. A cache at version V answers as the model does at V, and so does every read through a snapshot at V, for as long as the snapshot lasts.
7. No queue entry is claimed while a live lease's oldest version is below its free version, and every handle a live lease could hold still reads.
8. No key reads or writes outside its root, by path or by handle, and a revoked key does nothing.
9. The position the writer gives log truncation is at most every live lease's position, as the simulator's own record of the leases has it.

### Seeds, regressions and mutants

- A seed replays its run exactly. The simulator's PRNG is its own module, `sim/src/prng.rs`, and each source of randomness draws from its own stream, so a new source leaves existing draws unchanged.
- `--seeds N` sweeps N seeds on every core.
- A bug the simulator finds becomes a regression test in `sim/tests` that runs its seed. The commit message records the seed and the commit that failed.
- `scripts/mutants` plants known bugs one at a time and requires some layer to catch each: skipping the cycle check or its reverse for `Exchange`, keeping a file's link count on unlink, releasing below a lease, dropping the request-id lookup, skipping the epoch check, skipping the scope check on reads by handle, an op that applies although an op it names in `Applied` was skipped, a batch answered before the groups it was evaluated against commit, a cache that keeps a name a commit removed, a feed that leaves cached links as they were, a snapshot that ignores its undo records, a writer that answers after its lease, a read from the cache past the lease or outside its key's root, a standby that waits out a released head. `scripts/mutants --rocksdb` and `--lionturtle` plant bugs in those backends, and `scripts/fdb python3 scripts/mutants --fdb` in the FoundationDB backend, against a cluster. `EVIDENCE.md` maps every behavior here to the tests, properties and planted bugs that check it.

### Conformance and differential suites

- **Storage conformance,** above.
- **API conformance:** `tests/` runs the gRPC API against a server on each backend.
- **POSIX differential:** the POSIX subset (mkdir, rename and its flags, link, unlink, rmdir, and their errors) runs against both a scratch directory of the host's filesystem and InodeDB, and the results must match.

## Crates

| Crate | Role |
|---|---|
| `core` | Inode semantics, batch execution, the cache and snapshots, changefeed effects, lease and release decisions, `fsck`. No I/O, clocks or threads. |
| `storage` | The trait, key encoding, and the memory backend. |
| `storage-sqlite`, `storage-rocksdb`, `storage-lionturtle`, `storage-fdb` | Backends. |
| `storage-conformance` | The contract as a test suite generic over `Storage`. |
| `proto` | The `.proto` files and tonic code generation. |
| `transport` | gRPC over HTTP/2 for tonic's generated code: the client's channel and the server's connection loop. |
| `server` | The gRPC service, auth, the writer with group commit and fencing, standbys, background work. |
| `client` | The Rust client, with the cache, leases and snapshots. |
| `cli` | `idb`. |
| `sim` | The simulator. |
| `tests` | API conformance against a running server. |

The workspace uses Rust edition 2024.

## ClickHouse integration

The ClickHouse side is a new `metadata_type` for object-storage disks: an `IMetadataStorage` and `IMetadataTransaction` backed by InodeDB, as *Custom Metadata for Object-Storage Disks* describes. InodeDB knows nothing of ClickHouse; the disk decides how its files map onto values.

- A file's value holds its blob list, or its bytes when the disk keeps them inline.
- `tmp_*` directories stage locally and reach InodeDB as one batch at their rename.
- The ClickHouse writer fences itself with a `when` on an owner file's version, and sends each transaction as an `all_or_nothing` batch.
- Replicas read through snapshots. `refresh` waits for the latest version, and the changefeed reports removed parts as well as new ones.

| ClickHouse | InodeDB |
|---|---|
| `commit`, `tryCommit` | One `all_or_nothing` `Commit`; a failed `when` is the conflict outcome |
| `moveDirectory` | `Rename` with `NoReplace` |
| `replaceFile` | `Rename` with `Replace` |
| `createHardLink` | `Link` |
| `removeRecursive` | `RemoveAll` |
| `setLastModified` | `SetAttr` |
| `iterateDirectory`, `listDirectory` | `List`, from the cache |
| `refresh` | `wait_for` the latest version |
| `getBlobsToRemove`, `recordAsRemoved` | `ClaimFreed`, `AckFreed` |

ClickHouse already builds Rust crates, so the disk can link the Rust client and its cache through a C++ bridge.

## Open questions

- **Per-tenant release horizons.** Scoping the horizon to key roots would keep one tenant's long reader from delaying another's freeing. But a directory moved between roots before its files are freed would escape the hold of a reader under its old root.
- **Offline attenuation.** Minting goes through the server. Macaroon-style keys would let a holder narrow a key without a call, and revocation would then work by key family.
- **The ClickHouse client.** Linking the Rust client puts a tokio runtime in the ClickHouse process; a C++ client would duplicate the cache.
- **Scale beyond one writer.** One writer commits everything. Sharding by subtree would add writers, but renames across shards would need a protocol of their own.

## Defaults

Each default, and the measurement it rests on (`BENCHMARKS.md`; rates are batches a second).

| Setting | Default | Basis |
| --- | --- | --- |
| `writer.lease_ms`, `renew_every_ms`, `takeover_margin_ms` | 5 s, 1 s, 1 s | Failover takes 6.2 s on FoundationDB. A grouped FoundationDB commit's p99 at 128 clients is under 10 ms, on local NVMe or gp3, well inside the lease, and an idle writer commits once a second. The margin covers clocks drifting apart by up to a fifth of the lease. |
| `writer.lease_ttl_ms` | 30 s | Heartbeats go every 10 seconds, so a reader rides out two lost ones; a dead reader holds the horizon back 30 seconds. |
| `writer.claim_ttl_ms` | 60 s | A consumer deletes a claim's blobs well within it; one that dies returns its entries a minute later. |
| `log.min_retention_ms`, `max_retention_ms` | 5 min, 1 h | A log entry takes 43 to 74 bytes: 18 MB for five minutes at 1,000, and 216 MB for an hour. |
| `request_ttl` | 10 min, 5 at least | Five times the client's two-minute retry deadline. A live record takes about 100 bytes of the writer's memory and a little more in storage: 60 MB of memory at 1,000. |
| `group_max_bytes` | 9 MB | FoundationDB's 10,000,000-byte transactions, less the head, expiry deletions and up to 512 KiB of lease heartbeats. At 128 clients, grouping multiplies FoundationDB throughput 28 times on local NVMe and 18 times on gp3. |
| `undo_bytes` | 64 MiB | A replaced value's undo takes 160 bytes, so a snapshot outlasts about 400,000 changes in its subtree. |
| `cache.tree_bytes`, `value_bytes` | 1 GiB of tree, 256 MiB of values | A cached file holds 910 bytes and is charged 983: about a million files. |
| `cache.preload` | on | The server serves while it reads, and the cache reaches its budget at most, so the cost is memory the budget already allows. |
| `shallow_idle` | on | A core in C6 takes 190 µs to wake, on each hop of a request. Holding `/dev/cpu_dma_latency` at a microsecond cuts one client's read from 0.3 ms to 0.05 ms before any other change. It needs root or a device rule, and a server without either runs as it would have. |
