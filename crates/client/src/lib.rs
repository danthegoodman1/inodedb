//! InodeDB's Rust client: connections, request ids and retries.
//!
//! ```no_run
//! # async fn example() -> Result<(), inodedb::Error> {
//! use inodedb::{Client, Cond, Op};
//!
//! let db = Client::connect("http://127.0.0.1:7070", "00000000000000a1.0123456789abcdef").await?;
//! let mut batch = db.batch().when(Cond::is_dir("tables/t1")).all_or_nothing();
//! let part = batch.push(Op::mkdir("tables/t1/all_5_5_0"));
//! batch.push(Op::put("tables/t1/all_5_5_0/checksums.txt", "…").if_applied(part));
//! let commit = batch.commit().await?;
//! assert!(commit.all_applied());
//! # Ok(())
//! # }
//! ```

mod cache;
mod lease;
mod ops;
mod types;

pub use bytes::Bytes;
pub use cache::{Cached, Snapshot};
pub use inodedb_core::auth::Grant;
pub use inodedb_core::cache::Budget;
pub use lease::{Claim, Freed, Lease};
pub use ops::{Cond, Op, OpRef};
pub use types::*;

use inodedb_proto as pb;
use inodedb_proto::inode_db_client::InodeDbClient;
use inodedb_transport::Channel;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Code, Request, Status};

/// The largest message either side sends.
const MAX_MESSAGE: usize = 16 * 1024 * 1024;
/// The most targets one `Read` request names.
const MAX_READ_TARGETS: usize = 60;
/// The most targets one `Stat` request names.
const MAX_STAT_TARGETS: usize = 1_000;
/// The metadata key that names why the server rejected a batch.
const ERROR_KEY: &str = "inodedb-error";
/// The metadata key that names the writer on a standby's `NotWriter`.
const WRITER_KEY: &str = "inodedb-writer";

/// How long a call keeps retrying.
#[derive(Clone, Debug)]
pub struct Retry {
    /// Time allowed for one attempt.
    pub attempt_timeout: Duration,
    /// Time allowed for every attempt together. Keep it well under the
    /// server's request-id lifetime, ten minutes by default, so that a
    /// retried commit is always recognized.
    pub deadline: Duration,
    pub first_delay: Duration,
    pub max_delay: Duration,
}

impl Default for Retry {
    fn default() -> Retry {
        Retry {
            attempt_timeout: Duration::from_secs(30),
            deadline: Duration::from_secs(120),
            first_delay: Duration::from_millis(20),
            max_delay: Duration::from_secs(2),
        }
    }
}

pub struct Builder {
    endpoint: String,
    key: String,
    retry: Retry,
}

impl Builder {
    pub fn retry(mut self, retry: Retry) -> Builder {
        self.retry = retry;
        self
    }

    pub async fn connect(self) -> Result<Client, Error> {
        let channel = Channel::connect(&self.endpoint).await?;
        let auth = format!("Bearer {}", self.key).parse().map_err(|_| {
            Error::Status(Status::invalid_argument(
                "the key holds characters a header cannot",
            ))
        })?;
        Ok(Client {
            conn: Arc::new(RwLock::new(Conn {
                home: self.endpoint.clone(),
                endpoint: self.endpoint,
                rpc: rpc(channel),
            })),
            auth,
            floor: Arc::new(AtomicU64::new(0)),
            retry: self.retry,
        })
    }
}

fn rpc(channel: Channel) -> InodeDbClient<Channel> {
    InodeDbClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE)
}

/// The server a client talks to, and the one it was built for, which it
/// goes back to when a writer it was sent to fails.
struct Conn {
    endpoint: String,
    rpc: InodeDbClient<Channel>,
    home: String,
}

/// A connection to InodeDB under one key. Clones share the connection
/// and the version floor.
#[derive(Clone)]
pub struct Client {
    conn: Arc<RwLock<Conn>>,
    auth: MetadataValue<Ascii>,
    /// The newest version this client has seen; reads reflect at least it.
    floor: Arc<AtomicU64>,
    retry: Retry,
}

/// Whether an attempt's failure may have left the request unanswered
/// rather than refused. A connection that fails reads as `Unavailable`.
pub(crate) fn retriable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::DeadlineExceeded | Code::Cancelled
    )
}

fn jitter(delay: Duration) -> Duration {
    let spread = getrandom::u32().unwrap_or(0) % 1000;
    delay / 2 + delay * spread / 2000
}

/// A rejected batch's reason, when the server named one.
fn rejection(error: Error) -> Error {
    let Error::Status(status) = &error else {
        return error;
    };
    match status
        .metadata()
        .get(ERROR_KEY)
        .and_then(|code| code.to_str().ok())
    {
        Some("malformed") => Error::Rejected(Rejection::Malformed),
        Some("too-many-ops") => Error::Rejected(Rejection::TooManyOps),
        Some("too-large") => Error::Rejected(Rejection::TooLarge),
        _ => error,
    }
}

fn request_id() -> Bytes {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).expect("the system's random source");
    Bytes::copy_from_slice(&id)
}

impl Client {
    pub async fn connect(
        endpoint: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Client, Error> {
        Client::builder(endpoint, key).connect().await
    }

    /// This client, retrying as `retry` says; it shares the connection and
    /// the versions seen.
    pub fn with_retry(&self, retry: Retry) -> Client {
        Client {
            retry,
            ..self.clone()
        }
    }

    pub fn builder(endpoint: impl Into<String>, key: impl Into<String>) -> Builder {
        Builder {
            endpoint: endpoint.into(),
            key: key.into(),
            retry: Retry::default(),
        }
    }

    pub(crate) fn rpc(&self) -> InodeDbClient<Channel> {
        self.conn
            .read()
            .expect("no panics holding the connection")
            .rpc
            .clone()
    }

    /// The server this client talks to.
    pub fn endpoint(&self) -> String {
        self.conn
            .read()
            .expect("no panics holding the connection")
            .endpoint
            .clone()
    }

    /// Follows a standby's `NotWriter` to the writer it names, which every
    /// clone of the client then talks to. Whether the client now talks to
    /// a server it didn't before.
    pub(crate) fn redirect(&self, status: &Status) -> bool {
        let named = |key: &str| {
            status
                .metadata()
                .get(key)
                .and_then(|value| value.to_str().ok())
        };
        if named(ERROR_KEY) != Some("not-writer") {
            return false;
        }
        let Some(address) = named(WRITER_KEY) else {
            return false;
        };
        self.switch(address)
    }

    /// Talks to `address` from now on; whether that is a change.
    fn switch(&self, address: &str) -> bool {
        let mut conn = self.conn.write().expect("no panics holding the connection");
        if conn.endpoint == address {
            return false;
        }
        let Ok(channel) = Channel::lazy(address) else {
            return false;
        };
        conn.rpc = rpc(channel);
        conn.endpoint = address.to_string();
        true
    }

    /// After a failure worth retrying, goes back to the endpoint the
    /// client was built for, which names the writer anew.
    pub(crate) fn go_home(&self) {
        let home = self
            .conn
            .read()
            .expect("no panics holding the connection")
            .home
            .clone();
        self.switch(&home);
    }

    /// The newest version this client has seen.
    pub fn version(&self) -> Version {
        self.floor.load(Ordering::Acquire)
    }

    fn saw(&self, version: Version) {
        self.floor.fetch_max(version, Ordering::AcqRel);
    }

    fn at(&self) -> Option<pb::ReadAt> {
        Some(pb::ReadAt {
            at_least: self.version(),
        })
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", self.auth.clone());
        request.set_timeout(self.retry.attempt_timeout);
        request
    }

    /// Runs `call` until it succeeds, fails for good, or the deadline
    /// passes. Every call is safe to repeat: reads change nothing, and a
    /// commit carries a request id.
    async fn retry<T, F, Fut>(&self, mut call: F) -> Result<T, Error>
    where
        F: FnMut(InodeDbClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let start = Instant::now();
        let mut delay = self.retry.first_delay;
        let mut redirected = false;
        loop {
            match call(self.rpc()).await {
                Ok(value) => return Ok(value),
                Err(status) if start.elapsed() < self.retry.deadline && self.redirect(&status) => {
                    // A second redirect waits, so servers that name each
                    // other while a takeover settles don't spin.
                    if redirected {
                        tokio::time::sleep(jitter(delay)).await;
                        delay = (delay * 2).min(self.retry.max_delay);
                    }
                    redirected = true;
                }
                Err(status)
                    if retriable(&status) && start.elapsed() + delay < self.retry.deadline =>
                {
                    self.go_home();
                    tokio::time::sleep(jitter(delay)).await;
                    delay = (delay * 2).min(self.retry.max_delay);
                }
                Err(status) => return Err(Error::Status(status)),
            }
        }
    }

    /// Makes later reads through this client reflect at least `version`,
    /// as when another process reports a commit. It fails, leaving the
    /// client as it was, when the store has never reached `version`.
    pub async fn wait_for(&self, version: Version) -> Result<(), Error> {
        let at_least = version.max(self.version());
        let response = self
            .retry(|mut rpc| {
                let request = self.request(pb::StatRequest {
                    targets: vec![Target::from("").to_pb()],
                    at: Some(pb::ReadAt { at_least }),
                });
                async move { rpc.stat(request).await }
            })
            .await?
            .into_inner();
        self.saw(response.version);
        Ok(())
    }

    /// Runs `call` on each chunk of `items` at once, and returns the
    /// answers in order.
    async fn in_chunks<T, R, F, Fut>(
        &self,
        items: Vec<T>,
        size: usize,
        call: F,
    ) -> Result<Vec<R>, Error>
    where
        T: Clone,
        R: Send + 'static,
        F: Fn(Client, Vec<T>) -> Fut,
        Fut: Future<Output = Result<R, Error>> + Send + 'static,
    {
        if items.len() <= size {
            return Ok(vec![call(self.clone(), items).await?]);
        }
        let mut running = tokio::task::JoinSet::new();
        for (index, chunk) in items.chunks(size).enumerate() {
            let answer = call(self.clone(), chunk.to_vec());
            running.spawn(async move { (index, answer.await) });
        }
        let mut answers: Vec<Option<R>> = (0..running.len()).map(|_| None).collect();
        while let Some(joined) = running.join_next().await {
            let (index, answer) =
                joined.map_err(|error| Error::Status(Status::internal(error.to_string())))?;
            answers[index] = Some(answer?);
        }
        Ok(answers
            .into_iter()
            .map(|answer| answer.expect("every chunk answered"))
            .collect())
    }

    pub fn batch(&self) -> Batch<'_> {
        Batch {
            client: self,
            request: pb::CommitRequest::default(),
        }
    }

    /// Commits ops as one batch.
    pub async fn commit(&self, ops: impl IntoIterator<Item = Op>) -> Result<Commit, Error> {
        let mut batch = self.batch();
        for op in ops {
            batch.push(op);
        }
        batch.commit().await
    }

    async fn send(&self, mut request: pb::CommitRequest) -> Result<Commit, Error> {
        request.request_id = request_id();
        let response = self
            .retry(|mut rpc| {
                let request = self.request(request.clone());
                async move { rpc.commit(request).await }
            })
            .await
            .map_err(rejection)?
            .into_inner();
        self.saw(response.version);
        Ok(Commit {
            version: response.version,
            committed: response.committed,
            outcomes: response
                .outcomes
                .into_iter()
                .map(Outcome::from_pb)
                .collect(),
        })
    }

    /// Mints a key rooted at a directory under this key's root, with a
    /// grant no wider than this key's, and returns it as `<id>.<secret>`.
    /// It expires `ttl` from now or with this key, whichever comes first;
    /// a zero `ttl` expires with this key. A retry after a lost answer
    /// mints another key, and the lost one stays unknown to anyone.
    pub async fn mint_key(
        &self,
        root: impl AsRef<[u8]>,
        grant: Grant,
        ttl: Duration,
    ) -> Result<String, Error> {
        let request = pb::MintKeyRequest {
            root: root.as_ref().to_vec().into(),
            grant: match grant {
                Grant::Read => pb::Grant::Read,
                Grant::Write => pb::Grant::Write,
                Grant::Admin => pb::Grant::Admin,
            } as i32,
            ttl_ms: u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX),
        };
        let response = self
            .retry(|mut rpc| {
                let request = self.request(request.clone());
                async move { rpc.mint_key(request).await }
            })
            .await?;
        Ok(response.into_inner().key)
    }

    /// Revokes a key, named by its id or the whole key, with every key
    /// minted from it. A key may revoke itself and the keys minted from
    /// it.
    pub async fn revoke_key(&self, key: &str) -> Result<(), Error> {
        let id = key.split_once('.').map_or(key, |(id, _)| id).to_string();
        self.retry(|mut rpc| {
            let request = self.request(pb::RevokeKeyRequest { id: id.clone() });
            async move { rpc.revoke_key(request).await }
        })
        .await?;
        Ok(())
    }

    pub async fn stat(&self, target: impl Into<Target>) -> Result<Inode, Error> {
        let mut results = self.stat_many([target]).await?;
        results.pop().expect("one result")
    }

    /// Stats one target, with the version the answer reflects.
    pub(crate) async fn stat_at(&self, target: Target) -> Result<(Version, Inode), Error> {
        let target = target.to_pb();
        let response = self
            .retry(|mut rpc| {
                let request = self.request(pb::StatRequest {
                    targets: vec![target.clone()],
                    at: self.at(),
                });
                async move { rpc.stat(request).await }
            })
            .await?
            .into_inner();
        self.saw(response.version);
        match response
            .results
            .into_iter()
            .next()
            .and_then(|result| result.kind)
        {
            Some(pb::stat_result::Kind::Inode(inode)) => {
                Ok((response.version, Inode::from_pb(inode)))
            }
            Some(pb::stat_result::Kind::Error(code)) => Err(Error::Fs(FsError::from_pb(code))),
            None => Err(Error::Status(Status::internal("an empty stat result"))),
        }
    }

    /// Reads one value, with the version the answer reflects.
    pub(crate) async fn read_at(
        &self,
        handle: &Handle,
        value_version: Version,
    ) -> Result<(Version, Value), Error> {
        let target = pb::ReadTarget {
            target: Some(Target::from(handle).to_pb()),
            value_version: Some(value_version),
        };
        let response = self
            .retry(|mut rpc| {
                let request = self.request(pb::ReadRequest {
                    targets: vec![target.clone()],
                    at: self.at(),
                });
                async move { rpc.read(request).await }
            })
            .await?
            .into_inner();
        self.saw(response.version);
        match response
            .results
            .into_iter()
            .next()
            .and_then(|result| result.kind)
        {
            Some(pb::read_result::Kind::Value(value)) => Ok((
                response.version,
                Value {
                    inode: Inode::from_pb(value.inode.unwrap_or_default()),
                    value: value.value,
                },
            )),
            Some(pb::read_result::Kind::Error(code)) => Err(Error::Fs(FsError::from_pb(code))),
            None => Err(Error::Status(Status::internal("an empty read result"))),
        }
    }

    /// Stats many targets; each group of a thousand reflects one version.
    pub async fn stat_many<T: Into<Target>>(
        &self,
        targets: impl IntoIterator<Item = T>,
    ) -> Result<Vec<Result<Inode, Error>>, Error> {
        let targets: Vec<pb::Target> = targets
            .into_iter()
            .map(|target| target.into().to_pb())
            .collect();
        let responses = self
            .in_chunks(targets, MAX_STAT_TARGETS, |client, chunk| async move {
                let response = client
                    .retry(|mut rpc| {
                        let request = client.request(pb::StatRequest {
                            targets: chunk.clone(),
                            at: client.at(),
                        });
                        async move { rpc.stat(request).await }
                    })
                    .await?
                    .into_inner();
                client.saw(response.version);
                Ok(response)
            })
            .await?;
        let mut results = Vec::new();
        for response in responses {
            results.extend(
                response
                    .results
                    .into_iter()
                    .map(|result| match result.kind {
                        Some(pb::stat_result::Kind::Inode(inode)) => Ok(Inode::from_pb(inode)),
                        Some(pb::stat_result::Kind::Error(code)) => {
                            Err(Error::Fs(FsError::from_pb(code)))
                        }
                        None => Err(Error::Status(Status::internal("an empty stat result"))),
                    }),
            );
        }
        Ok(results)
    }

    pub async fn read(&self, target: impl Into<Target>) -> Result<Value, Error> {
        let mut results = self.read_many([(target.into(), None)]).await?;
        results.pop().expect("one result")
    }

    /// Reads a file's value as of `value_version`, which a snapshot holds.
    pub async fn read_version(
        &self,
        handle: &Handle,
        value_version: Version,
    ) -> Result<Value, Error> {
        let mut results = self
            .read_many([(Target::from(handle), Some(value_version))])
            .await?;
        results.pop().expect("one result")
    }

    /// Reads many values, each current or at a value version; each group
    /// of sixty reflects one version, and the groups go at once.
    pub async fn read_many(
        &self,
        targets: impl IntoIterator<Item = (Target, Option<Version>)>,
    ) -> Result<Vec<Result<Value, Error>>, Error> {
        let targets: Vec<pb::ReadTarget> = targets
            .into_iter()
            .map(|(target, value_version)| pb::ReadTarget {
                target: Some(target.to_pb()),
                value_version,
            })
            .collect();
        let responses = self
            .in_chunks(targets, MAX_READ_TARGETS, |client, chunk| async move {
                let response = client
                    .retry(|mut rpc| {
                        let request = client.request(pb::ReadRequest {
                            targets: chunk.clone(),
                            at: client.at(),
                        });
                        async move { rpc.read(request).await }
                    })
                    .await?
                    .into_inner();
                client.saw(response.version);
                Ok(response)
            })
            .await?;
        let mut results = Vec::new();
        for response in responses {
            results.extend(
                response
                    .results
                    .into_iter()
                    .map(|result| match result.kind {
                        Some(pb::read_result::Kind::Value(value)) => Ok(Value {
                            inode: Inode::from_pb(value.inode.unwrap_or_default()),
                            value: value.value,
                        }),
                        Some(pb::read_result::Kind::Error(code)) => {
                            Err(Error::Fs(FsError::from_pb(code)))
                        }
                        None => Err(Error::Status(Status::internal("an empty read result"))),
                    }),
            );
        }
        Ok(results)
    }

    /// Checks the store's invariants over one snapshot, under an admin key
    /// rooted at the tree's root. A check reads the whole store, so it runs
    /// once, for up to `CHECK_TIMEOUT`, rather than starting over.
    pub async fn check(&self) -> Result<Checked, Error> {
        let mut redirects = 0;
        let response = loop {
            let mut request = self.request(pb::CheckRequest {});
            request.set_timeout(CHECK_TIMEOUT);
            match self.rpc().check(request).await {
                Ok(response) => break response.into_inner(),
                Err(status) if redirects < 4 && self.redirect(&status) => {
                    redirects += 1;
                    tokio::time::sleep(jitter(self.retry.first_delay * redirects)).await;
                }
                Err(status) => return Err(Error::Status(status)),
            }
        };
        Ok(Checked {
            version: response.version,
            keys: response.keys,
            problems: response.problems,
        })
    }

    /// Streams the changes inside the subtree at `path` from version `from`
    /// on, or from after the current version with zero. The stream follows
    /// the directory through renames, and a standby serves it.
    pub async fn watch(&self, path: impl AsRef<[u8]>, from: Version) -> Result<Watch, Error> {
        let path = Bytes::copy_from_slice(path.as_ref());
        let stream = self
            .retry(|mut rpc| {
                // The timeout covers the stream's headers alone.
                let request = self.request(pb::WatchRequest {
                    path: path.clone(),
                    from,
                });
                async move { rpc.watch(request).await }
            })
            .await?
            .into_inner();
        Ok(Watch {
            client: self.clone(),
            stream,
        })
    }

    /// A directory's entries.
    pub async fn list(&self, path: impl AsRef<[u8]>) -> Result<Listing, Error> {
        self.list_with(path, &List::default()).await
    }

    /// A directory's entries with their attributes and values.
    pub async fn list_with_values(&self, path: impl AsRef<[u8]>) -> Result<Listing, Error> {
        self.list_with(
            path,
            &List {
                attrs: true,
                values: true,
                ..List::default()
            },
        )
        .await
    }

    pub async fn list_with(
        &self,
        path: impl AsRef<[u8]>,
        options: &List,
    ) -> Result<Listing, Error> {
        self.list_from(Bytes::copy_from_slice(path.as_ref()), None, options)
            .await
    }

    /// Lists the directory a handle names, wherever it now is.
    pub async fn list_dir(&self, dir: &Handle, options: &List) -> Result<Listing, Error> {
        self.list_from(Bytes::new(), Some(dir.to_pb()), options)
            .await
    }

    /// Lists `path`'s directory, or `dir` when given.
    async fn list_from(
        &self,
        path: Bytes,
        mut dir: Option<pb::Handle>,
        options: &List,
    ) -> Result<Listing, Error> {
        let mut entries: Vec<Entry> = Vec::new();
        let mut first: Option<Version> = None;
        // A resumed listing goes on with the directory it started on.
        // The deadline counts from the last page, so a long listing that
        // keeps making progress keeps going.
        let mut start = Instant::now();
        let mut delay = self.retry.first_delay;
        let mut redirected = false;
        loop {
            let after = entries
                .last()
                .map(|entry| entry.name.clone())
                .or_else(|| options.after.clone());
            let limit = match options.limit {
                0 => 0,
                limit => limit - entries.len() as u64,
            };
            let request = self.request(pb::ListRequest {
                path: path.clone(),
                dir: dir.clone(),
                after,
                prefix: options.prefix.clone(),
                limit,
                attrs: options.attrs,
                values: options.values,
                at: self.at(),
            });
            let failed = match self.rpc().list(request).await {
                Ok(response) => {
                    let mut pages = response.into_inner();
                    loop {
                        // A stream's timeout covers only its headers, so
                        // each page gets one of its own.
                        let page =
                            tokio::time::timeout(self.retry.attempt_timeout, pages.message());
                        match page.await {
                            Ok(Ok(Some(page))) => {
                                if let Some(code) = page.error {
                                    return Err(Error::Fs(FsError::from_pb(code)));
                                }
                                self.saw(page.version);
                                first.get_or_insert(page.version);
                                if dir.is_none() {
                                    dir = page.dir.clone();
                                }
                                entries.extend(page.entries.into_iter().map(Entry::from_pb));
                                if !page.more {
                                    return Ok(Listing {
                                        version: first.unwrap_or(page.version),
                                        newest: page.version,
                                        dir: Handle::from_pb(dir),
                                        entries,
                                    });
                                }
                                start = Instant::now();
                                delay = self.retry.first_delay;
                            }
                            Ok(Ok(None)) => break Status::unavailable("the listing ended early"),
                            Ok(Err(status)) => break status,
                            Err(_) => break Status::deadline_exceeded("a page took too long"),
                        }
                    }
                }
                Err(status) => status,
            };
            if self.redirect(&failed) && start.elapsed() < self.retry.deadline {
                if redirected {
                    tokio::time::sleep(jitter(delay)).await;
                    delay = (delay * 2).min(self.retry.max_delay);
                }
                redirected = true;
                continue;
            }
            if !retriable(&failed) || start.elapsed() + delay >= self.retry.deadline {
                return Err(Error::Status(failed));
            }
            self.go_home();
            tokio::time::sleep(jitter(delay)).await;
            delay = (delay * 2).min(self.retry.max_delay);
        }
    }
}

/// How long a check may take.
pub const CHECK_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Options for a listing.
#[derive(Clone, Debug, Default)]
pub struct List {
    /// Starts after this name.
    pub after: Option<Bytes>,
    /// Lists only names with this prefix.
    pub prefix: Option<Bytes>,
    /// Entries in all; zero lists every entry.
    pub limit: u64,
    pub attrs: bool,
    pub values: bool,
}

/// Ops gathered for one commit.
pub struct Batch<'a> {
    client: &'a Client,
    request: pb::CommitRequest,
}

impl Batch<'_> {
    /// Joins `cond` to every op's condition.
    pub fn when(mut self, cond: Cond) -> Self {
        self.request.when.push(cond.0);
        self
    }

    /// Applies every op or none: any skipped op aborts the batch.
    pub fn all_or_nothing(mut self) -> Self {
        self.request.all_or_nothing = true;
        self
    }

    /// Adds an op and returns its position, for `if_applied`.
    pub fn push(&mut self, op: Op) -> OpRef {
        self.request.ops.push(op.0);
        OpRef(self.request.ops.len() as u32 - 1)
    }

    pub fn len(&self) -> usize {
        self.request.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.request.ops.is_empty()
    }

    /// Commits the batch once, retrying lost answers under one request id.
    pub async fn commit(self) -> Result<Commit, Error> {
        self.client.send(self.request).await
    }
}

/// The changes inside a subtree, in version order.
pub struct Watch {
    client: Client,
    stream: tonic::Streaming<pb::Change>,
}

impl Watch {
    /// The next change; None once the server ends the stream, as when it
    /// shuts down.
    pub async fn next(&mut self) -> Result<Option<Changed>, Error> {
        match self.stream.message().await? {
            Some(change) if change.compacted => Err(Error::Compacted),
            Some(change) => {
                self.client.saw(change.version);
                Ok(Some(Changed::from_pb(change)))
            }
            None => Ok(None),
        }
    }
}
