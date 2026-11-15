# Releasing

Releases are automated. Four crates publish to crates.io, sharing the
workspace's version in `Cargo.toml`'s `[workspace.package]`:

- `inodedb-client`, the Rust client, imported as `inodedb`
- `inodedb-core`, `inodedb-proto` and `inodedb-transport`, which it builds on

The server, CLI, storage backends, simulator and tests stay unpublished. On
every push to `main`, `.github/workflows/bump.yml` ("Auto Release") decides a
version, commits it to `main` and tags it. The tag starts
`.github/workflows/release.yml`, which publishes the crates over **Trusted
Publishing** (OIDC, with no long-lived token secret) and creates the GitHub
release.

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
release went out by hand from a machine logged in with `cargo login`:

```console
cargo publish -p inodedb-core -p inodedb-proto -p inodedb-transport -p inodedb-client
```

Each of the four crates lists one GitHub publisher under **crates.io → the
crate → Settings → Trusted Publishing**: repository owner `danthegoodman1`,
repository name `inodedb`, workflow filename `release.yml`, environment
blank. The OIDC claim names the workflow a run started in, and every publish
starts in `release.yml`. Add no `CARGO_REGISTRY_TOKEN` secret: the workflow
mints a token per run with `rust-lang/crates-io-auth-action`.

### 2. Repository settings

- **The `main` ruleset** (Settings → Rules → Rulesets) blocks deleting `main`
  and force pushes to it, and takes changes only through pull requests,
  merged or squashed, whose `test`, `foundationdb` and `simulate` checks
  pass. Squash and merge commits keep the pull request's title in the head
  commit message, where the bump reads its controls. Only deploy keys
  bypass it.
- **The `Auto Release` deploy key** (Settings → Deploy keys) has write
  access, and its private half is the `RELEASE_DEPLOY_KEY` secret. `bump.yml`
  checks out with it and pushes the `chore(release): vX.Y.Z` commit and the
  tag over SSH, past the ruleset. A push with a deploy key starts workflows,
  where one with `GITHUB_TOKEN` would not: the tag starts `release.yml`, and
  the commit starts `bump.yml` again, which skips it by its message. To
  replace the key, add a new one with write access, set the secret to its
  private half, and delete the old key.
- **Fork pull requests** (Settings → Actions → General) from anyone who
  isn't a collaborator wait for a maintainer's approval before their
  workflows run. Workflows on fork pull requests never see the secret.

## What runs where

- **bump.yml** (push to `main`): decide the bump level → check that the crates
  exist on crates.io → `cargo test` → escalate the version with
  `cargo-semver-checks` → commit and tag, pushed with the deploy key.
- **release.yml** (tag push, or manual): `fmt`, `clippy` and `cargo test` on
  Linux x86_64 → check the tag against `Cargo.toml`'s version → find the
  crates missing at that version → OIDC auth → `cargo publish` of those
  crates, in dependency order → create the tag's GitHub release.
- **ci.yml** (every pull request and push): among its checks, `cargo package`
  of the four crates, which builds each from its package alone as crates.io
  would.

The release workflows test the workspace's default members: the published
crates, the server, and the API tests on memory, SQLite and lionturtle. CI
tests RocksDB and FoundationDB on every pull request.
