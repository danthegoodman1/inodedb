//! The gRPC service: checks keys, converts messages and asks the engine.

use crate::auth::{self, Auth, Grant};
use crate::convert;
use crate::engine::Engine;
use crate::feed::{self, Log};
use inodedb_core::auth::{Caller, KeyError, KeyOp};
use inodedb_core::delta::Freed;
use inodedb_core::lease::{ClaimError, LeaseError, LeaseRecord};
use inodedb_core::ops::BatchError;
use inodedb_core::query::{Query, QueryResult, Target};
use inodedb_core::types::{Error, Kind, Version};
use inodedb_core::writer::Answer;
use inodedb_proto as pb;
use inodedb_proto::inode_db_server::InodeDb;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

/// The most targets one `Read` names, so that 64 KiB values answer within
/// gRPC's default 4 MiB message.
pub const MAX_READ_TARGETS: usize = 60;
/// The most targets one `Stat` names; each answer is about 100 bytes.
pub const MAX_STAT_TARGETS: usize = 1_000;
/// The most entries one claim returns, each with its value, within gRPC's
/// default 4 MiB message.
pub const MAX_CLAIM: usize = 60;
/// Entries per `List` page.
pub const PAGE_ENTRIES: usize = 1024;
/// Bytes per `List` page, as the core counts them, leaving room under
/// gRPC's default 4 MiB message.
pub const PAGE_BYTES: usize = 3 * 1024 * 1024;
/// The metadata key that names why a batch was rejected.
pub const ERROR_KEY: &str = "inodedb-error";

/// Faults a test asks for: each count is how many more times to inject
/// one. A server outside tests leaves them at zero.
#[derive(Debug, Default)]
pub struct Faults {
    /// Commits that apply but whose answer is replaced by `Unavailable`.
    pub lost_commit_answers: AtomicU32,
    /// Listings cut off with `Unavailable` after their first page.
    pub cut_listings: AtomicU32,
    /// Lease openings held 200 ms before they reach the writer.
    pub slow_lease_opens: AtomicU32,
}

impl Faults {
    fn take(count: &AtomicU32) -> bool {
        count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok()
    }
}

pub struct Service {
    engine: Engine,
    auth: Auth,
    log: Arc<dyn Log>,
    tail: Arc<feed::Tail>,
    faults: Arc<Faults>,
    /// Turns true when the server shuts down, ending streams.
    closing: tokio::sync::watch::Receiver<bool>,
}

/// The most problems a check reports.
const MAX_PROBLEMS: usize = 1_000;

/// Checks the log up to `head`, read after the snapshot a check took. Commits
/// since add entries past `head`, which it leaves alone, and truncation may
/// cut the oldest while it reads, so it starts again from the new oldest.
async fn check_log(
    log: &dyn Log,
    head: Version,
) -> Result<Vec<String>, inodedb_storage::StorageError> {
    const BATCH: usize = 1_024;
    let mut checked = inodedb_core::fsck::LogCheck::default();
    let mut from = 0;
    loop {
        let entries = log.read(from, BATCH).await?;
        if from > 0
            && let Some((first, _)) = entries.first()
            && *first > from
            && log
                .read(0, 1)
                .await?
                .first()
                .is_some_and(|(oldest, _)| oldest >= first)
        {
            checked = inodedb_core::fsck::LogCheck::default();
        }
        let full = entries.len() == BATCH;
        for (version, bytes) in &entries {
            if *version > head {
                return Ok(checked.finish(head));
            }
            checked.entry(*version, bytes);
            from = version + 1;
        }
        if !full {
            // Truncation may have taken the rest while the check read.
            let last = from.checked_sub(1);
            let oldest = log.read(0, 1).await?.first().map(|(version, _)| *version);
            if last.is_some_and(|last| last < head)
                && oldest.is_none_or(|oldest| Some(oldest) > last)
            {
                return Ok(checked.cut());
            }
            return Ok(checked.finish(head));
        }
    }
}

impl Service {
    pub fn new(
        engine: Engine,
        auth: Auth,
        log: Arc<dyn Log>,
        tail: Arc<feed::Tail>,
        faults: Arc<Faults>,
        closing: tokio::sync::watch::Receiver<bool>,
    ) -> Service {
        Service {
            engine,
            auth,
            log,
            tail,
            faults,
            closing,
        }
    }
}

fn rejected(error: BatchError) -> Status {
    let (code, message) = match error {
        BatchError::Malformed => (
            "malformed",
            "an Applied predicate names an op at or after its own",
        ),
        BatchError::TooManyOps => ("too-many-ops", "a batch holds at most 10,000 ops"),
        BatchError::TooLarge => ("too-large", "the batch writes more than 8 MiB"),
    };
    tagged(Status::invalid_argument(message), code)
}

/// Whether a status is a standby's `NotWriter`.
pub fn not_writer(status: &Status) -> bool {
    status
        .metadata()
        .get(ERROR_KEY)
        .is_some_and(|code| code == "not-writer")
}

fn tagged(mut status: Status, code: &'static str) -> Status {
    status
        .metadata_mut()
        .insert(ERROR_KEY, MetadataValue::from_static(code));
    status
}

async fn ask(
    engine: &Engine,
    caller: Caller,
    query: Query,
    at: Option<pb::ReadAt>,
) -> Result<(Version, QueryResult), Status> {
    let at_least = at.map_or(0, |at| at.at_least);
    match engine.query(caller, query, at_least).await? {
        Answer::Query { version, result } => Ok((version, result)),
        Answer::NotYet { version } => Err(Status::out_of_range(format!(
            "version {at_least} is past this store's {version}"
        ))),
        Answer::Denied(denied) => Err(auth::denied(denied)),
        Answer::Unavailable => Err(Status::unavailable("the writer has stopped")),
        other => Err(Status::internal(format!("unexpected answer {other:?}"))),
    }
}

/// A key op's answer.
async fn key_op(engine: &Engine, caller: Caller, op: KeyOp) -> Result<(), Status> {
    match engine.key(caller, op).await? {
        Answer::Key(Ok(_)) => Ok(()),
        Answer::Key(Err(KeyError::Fs(Error::NotFound))) => {
            Err(Status::not_found("the root doesn't exist"))
        }
        Answer::Key(Err(KeyError::Fs(error))) => Err(Status::failed_precondition(format!(
            "the root isn't a directory: {error:?}"
        ))),
        Answer::Key(Err(KeyError::Exists)) => {
            Err(Status::aborted("the key id is taken; mint again"))
        }
        Answer::Key(Err(KeyError::NotFound)) => {
            Err(Status::not_found("no key this key may revoke has that id"))
        }
        Answer::Denied(denied) => Err(auth::denied(denied)),
        Answer::Unavailable => Err(Status::unavailable("the writer has stopped")),
        other => Err(Status::internal(format!("unexpected answer {other:?}"))),
    }
}

fn too_many(count: usize, max: usize) -> Result<(), Status> {
    if count > max {
        Err(Status::invalid_argument(format!("at most {max} targets")))
    } else {
        Ok(())
    }
}

type ListStream = Pin<Box<dyn Stream<Item = Result<pb::ListPage, Status>> + Send>>;

/// Where a listing has reached: each page asks the writer for the entries
/// after the last one sent, at least as new as the page before, in the
/// directory the first page found, wherever renames take it.
struct Pages {
    request: pb::ListRequest,
    target: Target,
    after: Option<Vec<u8>>,
    at: Option<pb::ReadAt>,
    sent: usize,
    total: usize,
}

impl Pages {
    async fn next(
        &mut self,
        engine: &Engine,
        caller: Caller,
        scope: &auth::Scope,
    ) -> Result<pb::ListPage, Status> {
        let want = PAGE_ENTRIES.min(self.total - self.sent);
        let query = convert::list_query(
            &self.request,
            self.target.clone(),
            self.after.clone(),
            want,
            PAGE_BYTES,
        )?;
        let (version, result) = ask(engine, caller, query, self.at).await?;
        self.at = Some(pb::ReadAt { at_least: version });
        match result {
            QueryResult::List { dir, entries, more } => {
                self.target = Target::Ino(dir);
                self.sent += entries.len();
                if let Some(last) = entries.last() {
                    self.after = Some(last.name.to_vec());
                }
                Ok(pb::ListPage {
                    version,
                    entries: entries
                        .into_iter()
                        .map(|listed| convert::listed(listed, scope))
                        .collect(),
                    more: more && self.sent < self.total,
                    error: None,
                    dir: Some(scope.handle(dir)),
                })
            }
            QueryResult::Failed(error) => Ok(pb::ListPage {
                version,
                entries: Vec::new(),
                more: false,
                error: Some(convert::error(error) as i32),
                dir: None,
            }),
            other => Err(Status::internal(format!("unexpected {other:?}"))),
        }
    }
}
type WatchStream = Pin<Box<dyn Stream<Item = Result<pb::Change, Status>> + Send>>;
type LeaseStream = Pin<Box<dyn Stream<Item = Result<pb::LeaseState, Status>> + Send>>;

#[tonic::async_trait]
impl InodeDb for Service {
    async fn commit(
        &self,
        request: Request<pb::CommitRequest>,
    ) -> Result<Response<pb::CommitResponse>, Status> {
        let (caller, scope) = self.auth.caller(&request)?;
        // An op the server can't read makes the batch malformed.
        let batch =
            convert::batch(request.into_inner()).map_err(|status| tagged(status, "malformed"))?;
        let response = match self.engine.submit(caller, batch).await? {
            Answer::Commit(result) => convert::commit_response(result, &scope),
            Answer::Rejected(error) => return Err(rejected(error)),
            Answer::Denied(denied) => return Err(auth::denied(denied)),
            Answer::Unavailable => return Err(Status::unavailable("the writer has stopped")),
            other => return Err(Status::internal(format!("unexpected answer {other:?}"))),
        };
        if Faults::take(&self.faults.lost_commit_answers) {
            return Err(Status::unavailable("the answer was lost, as a test asked"));
        }
        Ok(Response::new(response))
    }

    async fn stat(
        &self,
        request: Request<pb::StatRequest>,
    ) -> Result<Response<pb::StatResponse>, Status> {
        let (caller, scope) = self.auth.caller(&request)?;
        let request = request.into_inner();
        too_many(request.targets.len(), MAX_STAT_TARGETS)?;
        let queries = request
            .targets
            .into_iter()
            .map(|target| convert::target(Some(target), &scope).map(Query::Stat))
            .collect::<Result<_, _>>()?;
        let (version, result) = ask(&self.engine, caller, Query::Many(queries), request.at).await?;
        let QueryResult::Many(results) = result else {
            return Err(Status::internal("many queries answered one"));
        };
        Ok(Response::new(pb::StatResponse {
            version,
            results: results
                .iter()
                .map(|result| convert::stat_result(result, &scope))
                .collect(),
        }))
    }

    async fn read(
        &self,
        request: Request<pb::ReadRequest>,
    ) -> Result<Response<pb::ReadResponse>, Status> {
        let (caller, scope) = self.auth.caller(&request)?;
        let request = request.into_inner();
        too_many(request.targets.len(), MAX_READ_TARGETS)?;
        let queries = request
            .targets
            .into_iter()
            .map(|read| {
                convert::target(read.target, &scope).map(|target| Query::Read {
                    target,
                    version: read.value_version,
                })
            })
            .collect::<Result<_, _>>()?;
        let (version, result) = ask(&self.engine, caller, Query::Many(queries), request.at).await?;
        let QueryResult::Many(results) = result else {
            return Err(Status::internal("many queries answered one"));
        };
        Ok(Response::new(pb::ReadResponse {
            version,
            results: results
                .iter()
                .map(|result| convert::read_result(result, &scope))
                .collect(),
        }))
    }

    type ListStream = ListStream;

    async fn list(
        &self,
        request: Request<pb::ListRequest>,
    ) -> Result<Response<ListStream>, Status> {
        let (caller, scope) = self.auth.caller(&request)?;
        let request = request.into_inner();
        let target = match &request.dir {
            Some(handle) => Target::Ino(scope.check(handle)?),
            None => Target::Path(convert::path(&request.path)?),
        };
        let total = match request.limit {
            0 => usize::MAX,
            limit => usize::try_from(limit).unwrap_or(usize::MAX),
        };
        let cut = Faults::take(&self.faults.cut_listings);
        let mut listing = Pages {
            after: request.after.as_ref().map(|after| after.to_vec()),
            at: request.at,
            request,
            target,
            sent: 0,
            total,
        };
        // The first page goes out with the response's headers, and a
        // listing that fits in it, as most do, needs nothing more.
        let first = listing.next(&self.engine, caller, &scope).await;
        let more = matches!(&first, Ok(page) if page.more);
        let first = tokio_stream::once(first);
        if !more {
            return Ok(Response::new(Box::pin(first)));
        }
        let (pages, rest) = tokio::sync::mpsc::channel(2);
        let engine = self.engine.clone();
        tokio::spawn(async move {
            if cut {
                let _ = pages
                    .send(Err(Status::unavailable(
                        "the listing was cut, as a test asked",
                    )))
                    .await;
                return;
            }
            loop {
                let page = tokio::select! {
                    // The client left; stop asking the writer.
                    () = pages.closed() => return,
                    page = listing.next(&engine, caller, &scope) => page,
                };
                let more = matches!(&page, Ok(page) if page.more);
                if pages.send(page).await.is_err() || !more {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(
            first.chain(tokio_stream::wrappers::ReceiverStream::new(rest)),
        )))
    }

    type WatchStream = WatchStream;

    async fn watch(
        &self,
        request: Request<pb::WatchRequest>,
    ) -> Result<Response<WatchStream>, Status> {
        let (caller, scope) = self.auth.caller(&request)?;
        let key = self.engine.authorize(caller, Grant::Read).await?;
        let request = request.into_inner();
        let path = convert::path(&request.path)?;
        // A standby serves the stream once it reaches where it starts.
        let at = pb::ReadAt {
            at_least: request.from.saturating_sub(1).max(1),
        };
        let (version, result) = ask(
            &self.engine,
            caller,
            Query::Stat(Target::Path(path)),
            Some(at),
        )
        .await?;
        let root = match result {
            QueryResult::Stat { ino, inode } if inode.kind == Kind::Dir => ino,
            QueryResult::Stat { .. } => {
                return Err(Status::failed_precondition("Watch follows a directory"));
            }
            QueryResult::Failed(error) => {
                return Err(Status::not_found(format!("{error:?}")));
            }
            other => return Err(Status::internal(format!("unexpected {other:?}"))),
        };
        // The key was live at `version`, which the read's floor puts at
        // `from - 1` or later, so a revocation after it shows in the
        // entries the stream reads from `from` on.
        let from = match request.from {
            0 => version + 1,
            from => from,
        };
        let (changes, stream) = tokio::sync::mpsc::channel(16);
        tokio::spawn(feed::watch(
            self.log.clone(),
            self.tail.clone(),
            feed::Watcher {
                scope,
                key_root: key.root,
                key: key.key,
                expires: key.expires,
                closing: self.closing.clone(),
                committed: self.engine.committed(),
            },
            root,
            from,
            version,
            changes,
        ));
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        )))
    }

    type LeaseStream = LeaseStream;

    async fn lease(
        &self,
        request: Request<Streaming<pb::LeaseBeat>>,
    ) -> Result<Response<LeaseStream>, Status> {
        let (caller, _) = self.auth.caller(&request)?;
        let mut beats = request.into_inner();
        let engine = self.engine.clone();
        let faults = self.faults.clone();
        let mut closing = self.closing.clone();
        let (states, stream) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut lease = None;
            loop {
                let beat = tokio::select! {
                    beat = beats.message() => beat,
                    _ = async { closing.wait_for(|closing| *closing).await.is_ok() } => {
                        let _ = states.send(Err(Status::unavailable("the server is shutting down"))).await;
                        return;
                    }
                };
                // A dropped stream leaves the lease to run out.
                let Ok(Some(beat)) = beat else {
                    return;
                };
                let named = (beat.lease != 0).then_some(beat.lease).or(lease);
                let record = LeaseRecord {
                    oldest: beat.oldest,
                    position: beat.position,
                };
                if named.is_none() && Faults::take(&faults.slow_lease_opens) {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                let answer = engine
                    .lease(caller, named, record, beat.current, beat.release)
                    .await;
                let state = match answer {
                    Ok(Answer::Lease(Ok(granted))) => {
                        lease = Some(granted.lease);
                        Ok(pb::LeaseState {
                            lease: granted.lease,
                            expires_in_ms: granted.expires_in,
                            expired: false,
                            oldest: granted.oldest,
                        })
                    }
                    Ok(Answer::Lease(Err(LeaseError::Expired))) => Ok(pb::LeaseState {
                        lease: named.unwrap_or(0),
                        expired: true,
                        ..pb::LeaseState::default()
                    }),
                    Ok(Answer::Lease(Err(LeaseError::TooOld { horizon }))) => {
                        Err(Status::failed_precondition(format!(
                            "a lease starts at the release horizon, {horizon}, or later"
                        )))
                    }
                    Ok(Answer::Lease(Err(LeaseError::Lowered { oldest }))) => {
                        Err(Status::failed_precondition(format!(
                            "the lease holds {oldest}; a heartbeat may not lower it"
                        )))
                    }
                    Ok(Answer::Denied(denied)) => Err(auth::denied(denied)),
                    Ok(Answer::Unavailable) => Err(Status::unavailable("the writer has stopped")),
                    Ok(other) => Err(Status::internal(format!("unexpected answer {other:?}"))),
                    Err(status) => Err(status),
                };
                let ended = !matches!(&state, Ok(state) if !state.expired);
                if states.send(state).await.is_err() || ended {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        )))
    }

    async fn claim_freed(
        &self,
        request: Request<pb::ClaimRequest>,
    ) -> Result<Response<pb::ClaimResponse>, Status> {
        let (caller, scope) = self.auth.caller(&request)?;
        let limit = (request.into_inner().limit as usize).clamp(1, MAX_CLAIM);
        let (claim, entries) = match self.engine.claim(caller, limit).await? {
            Answer::Claimed { claim, entries } => (claim, entries),
            Answer::Denied(denied) => return Err(auth::denied(denied)),
            Answer::Unavailable => return Err(Status::unavailable("the writer has stopped")),
            other => return Err(Status::internal(format!("unexpected answer {other:?}"))),
        };
        // What each entry kept: the inode while its record stands, and the
        // value it frees, within the root the entry recorded.
        let reads = entries.iter().map(|entry| Query::Kept {
            freed: entry.freed,
            above: entry.ancestors.clone(),
        });
        let (_, kept) = ask(&self.engine, caller, Query::Many(reads.collect()), None).await?;
        let QueryResult::Many(kept) = kept else {
            return Err(Status::internal("many queries answered one"));
        };
        let freed = entries
            .iter()
            .zip(&kept)
            .map(|(entry, kept)| convert::freed(entry, kept, &scope))
            .collect();
        Ok(Response::new(pb::ClaimResponse { claim, freed }))
    }

    async fn ack_freed(
        &self,
        request: Request<pb::AckRequest>,
    ) -> Result<Response<pb::AckResponse>, Status> {
        let (caller, scope) = self.auth.caller(&request)?;
        let request = request.into_inner();
        let entries = request
            .freed
            .iter()
            .map(|freed| {
                let ino =
                    scope.check(freed.handle.as_ref().ok_or_else(|| {
                        Status::invalid_argument("a freed entry names its inode")
                    })?)?;
                Ok((
                    freed.version,
                    match freed.value_version {
                        Some(value) => Freed::Value(ino, value),
                        None => Freed::Inode(ino),
                    },
                ))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        match self.engine.ack(caller, request.claim, entries).await? {
            Answer::Acked(Ok(_)) => Ok(Response::new(pb::AckResponse {})),
            Answer::Acked(Err(ClaimError::Unknown)) => Err(Status::not_found(
                "the claim ran out or never was; claim the entries again",
            )),
            Answer::Denied(denied) => Err(auth::denied(denied)),
            Answer::Unavailable => Err(Status::unavailable("the writer has stopped")),
            other => Err(Status::internal(format!("unexpected answer {other:?}"))),
        }
    }

    async fn mint_key(
        &self,
        request: Request<pb::MintKeyRequest>,
    ) -> Result<Response<pb::MintKeyResponse>, Status> {
        let (caller, _) = self.auth.caller(&request)?;
        let request = request.into_inner();
        let grant = match pb::Grant::try_from(request.grant) {
            Ok(pb::Grant::Read) => Grant::Read,
            Ok(pb::Grant::Write) => Grant::Write,
            Ok(pb::Grant::Admin) => Grant::Admin,
            Err(_) => return Err(Status::invalid_argument("unknown grant")),
        };
        let (id, secret) = auth::generate();
        let op = KeyOp::Mint {
            id,
            hash: auth::hash(&secret),
            root: convert::path(&request.root)?,
            grant,
            ttl: request.ttl_ms,
        };
        key_op(&self.engine, caller, op).await?;
        Ok(Response::new(pb::MintKeyResponse {
            key: auth::format(id, &secret),
        }))
    }

    async fn check(
        &self,
        request: Request<pb::CheckRequest>,
    ) -> Result<Response<pb::CheckResponse>, Status> {
        let (caller, _) = self.auth.caller(&request)?;
        let scope = self.engine.authorize(caller, Grant::Admin).await?;
        if scope.root != inodedb_core::types::ROOT {
            return Err(Status::permission_denied(
                "a check reads the whole store; it takes an admin key rooted at the tree's root",
            ));
        }
        let pairs = self
            .log
            .dump()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut checked = tokio::task::spawn_blocking(move || {
            let problems = inodedb_core::fsck::check(
                pairs
                    .iter()
                    .map(|(key, value)| (key.as_slice(), value.as_slice())),
            );
            let version = pairs
                .iter()
                .find(|(key, _)| *key == inodedb_core::keys::head())
                .and_then(|(_, value)| inodedb_core::writer::Head::decode(value).ok())
                .map_or(0, |head| head.version);
            pb::CheckResponse {
                version,
                keys: pairs.len() as u64,
                problems,
            }
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        let log = check_log(&*self.log, checked.version)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        checked.problems.extend(log);
        // The first problems say enough, and all of them could pass the
        // message limit.
        if checked.problems.len() > MAX_PROBLEMS {
            let more = checked.problems.len() - MAX_PROBLEMS;
            checked.problems.truncate(MAX_PROBLEMS);
            checked.problems.push(format!("and {more} more"));
        }
        Ok(Response::new(checked))
    }

    async fn revoke_key(
        &self,
        request: Request<pb::RevokeKeyRequest>,
    ) -> Result<Response<pb::RevokeKeyResponse>, Status> {
        let (caller, _) = self.auth.caller(&request)?;
        let id = request.into_inner().id;
        let id = match id.split_once('.').map_or(id.as_str(), |(id, _)| id) {
            id if id.len() == 16 => u64::from_str_radix(id, 16).ok(),
            _ => None,
        }
        .ok_or_else(|| Status::invalid_argument("a key id is 16 hex digits"))?;
        key_op(&self.engine, caller, KeyOp::Revoke { id }).await?;
        Ok(Response::new(pb::RevokeKeyResponse {}))
    }
}
