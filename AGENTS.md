# InodeDB

`DESIGN.md` is the design contract. Read it before changing behavior, and when code and design disagree, fix one of them in the same change. `PLAN.md` is the build plan: work in its order and update its ledgers with evidence.

## Core rules

- `crates/core` does no I/O, reads no clocks and starts no threads. It takes requests, storage results and the current time as inputs and returns actions. The server carries them out over gRPC and a storage backend; the simulator carries them out over its models.
- InodeDB's semantics live once, in `crates/core`. Storage backends implement only the `Storage` trait and hold no inode logic.
- One writer applies every batch. The core decides grouping, answers and fencing; the server carries them out.
- Randomness reaches the core as values passed in, such as a writer's id and handle secret; the server draws them from the system, and the simulator from the seed.
- `clippy.toml` in `crates/core` and `crates/sim` forbids unordered collections, clocks and threads.

## Testing

- Every behavior `DESIGN.md` names needs a test and a planted bug in `scripts/mutants` that the test catches. A behavior the core decides needs a simulator property or a scripted simulator scenario. A behavior only the server carries out, such as gRPC framing, config or key plumbing, needs a server test or a unit test in `crates/server`.
- The simulator checks responses against its naive path model, never against the core's own state.
- A seed replays its run exactly. The simulator draws from its own PRNG; give each new source of randomness its own `Prng::stream`, so it leaves existing draws unchanged.
- A bug the simulator finds becomes a regression test in `crates/sim/tests` that runs its seed. The commit message records the seed and the commit that failed.
- Every backend passes `storage-conformance`. A change to the storage contract lands with its conformance test, and CI runs the suite on memory, SQLite, RocksDB, lionturtle and FoundationDB.
- Every API behavior needs a test in `tests/` that runs against a server on each backend.

## Commands

The tests need `protoc` (`brew install protobuf` or `apt-get install protobuf-compiler`): `crates/proto` holds the code generated from `proto/inodedb.proto`, and its test checks that code and rewrites it when the proto file changes.

```console
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --features rocksdb
cargo run -p inodedb-server -- --config inodedb.toml --init-key admin.key
scripts/fdb
cargo run --release -p inodedb-sim [-- SEED] [--seeds N]
scripts/posix
scripts/mutants
```
