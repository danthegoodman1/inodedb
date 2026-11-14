# Releasing

Releases are automated. Four crates publish to crates.io, sharing the
workspace's version in `Cargo.toml`'s `[workspace.package]`:

- `inodedb-client`, the Rust client, imported as `inodedb`
- `inodedb-core`, `inodedb-proto` and `inodedb-transport`, which it builds on

The server, CLI, storage backends, simulator and tests stay unpublished. On
every push to `main`, `.github/workflows/bump.yml` ("Auto Release") decides a
version, tags it, creates a GitHub release, and calls
`.github/workflows/release.yml`, which publishes the crates over **Trusted
Publishing** (OIDC, with no long-lived token secret).

## Day-to-day

Merge to `main`. The version bumps by a **patch** by default and escalates
(patch → minor → major) until `cargo-semver-checks` agrees the level covers
every published crate's public-API diff since the last release, so a breaking
change cannot go out as a patch. Under 0.x, a breaking change bumps the minor
version.

Control it through the head commit message; a merge commit carries the pull
request's title:

- `#minor` / `#major`: force that bump level (the run fails if the level can't
  cover the API diff).
- `[skip release]`: release nothing for this push.

**Actions → Auto Release → Run workflow** also releases, at a level you pick.

`workflow_dispatch` on **Release** reruns a publish for an existing `vX.Y.Z`
tag; it publishes only the crates crates.io lacks at that version.

## Changing the protocol

`crates/proto/src/inodedb.v1.rs` holds the code generated from
`proto/inodedb.proto`, so the published crate builds without `protoc`. After
editing the proto file, run `cargo test -p inodedb-proto`: its test rewrites
the generated file and fails once, and the next run passes. Commit both files.

## One-time setup

### 1. Trusted Publishing on crates.io

Trusted Publishing lets GitHub Actions publish with a short-lived OIDC token.
It is configured per crate, once the crate exists on crates.io, so the first
release goes out by hand from a machine logged in with `cargo login`:

```console
cargo publish -p inodedb-core -p inodedb-proto -p inodedb-transport -p inodedb-client
```

Then, for each of the four crates, open **crates.io → the crate → Settings →
Trusted Publishing** and add two GitHub publishers:

- Repository owner `danthegoodman1`, repository name `inodedb`, workflow
  filename `release.yml`, environment blank.
- The same with workflow filename `bump.yml`. The OIDC claim names the
  **top-level** workflow, and the auto-release path starts in `bump.yml`,
  which calls `release.yml` as a reusable workflow.

Add no `CARGO_REGISTRY_TOKEN` secret: the workflow mints a token per run with
`rust-lang/crates-io-auth-action`.

### 2. Repository settings

The bump job pushes the `chore(release): vX.Y.Z` commit, the tag and the
GitHub release with the built-in `GITHUB_TOKEN`, under the `contents: write`
permission its workflow asks for. If `main` becomes protected, allow
`github-actions[bot]` to push to it.

## What runs where

- **bump.yml** (push to `main`): decide the bump level → check that the crates
  exist on crates.io → `cargo test` → escalate the version with
  `cargo-semver-checks` → commit, tag and GitHub release → call `release.yml`.
- **release.yml** (tag push, called, or manual): `fmt`, `clippy` and
  `cargo test` on Linux x86_64 → check the tag against
  `Cargo.toml`'s version → find the crates missing at that version → OIDC
  auth → `cargo publish` of those crates, in dependency order.
- **ci.yml** (every pull request and push): among its checks, `cargo package`
  of the four crates, which builds each from its package alone as crates.io
  would.

The release workflows test the workspace's default members: the published
crates, the server, and the API tests on memory, SQLite and lionturtle. CI
tests RocksDB and FoundationDB on every pull request.
