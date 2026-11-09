//! Leases, and the free queue's consumers.
//!
//! A lease keeps the inodes and values freed after its oldest version
//! readable by handle until it moves on or runs out. A consumer claims
//! released entries under its root, deletes what they reference, and
//! acknowledges them. A listing reads every entry still in the queue,
//! released or not, and claims none.

use crate::{Client, Error, Handle, Inode, Version};
use bytes::Bytes;
use inodedb_proto as pb;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::{Code, Status};

/// A reader's lease. A lost stream resumes on a new one at the next
/// heartbeat, and dropping the lease releases it.
pub struct Lease {
    client: Client,
    id: u64,
    oldest: Version,
    expires_in: Duration,
    stream: Option<(
        mpsc::Sender<pb::LeaseBeat>,
        tonic::Streaming<pb::LeaseState>,
    )>,
    released: bool,
}

/// What a heartbeat on an open stream came to.
enum Exchange {
    Answered(pb::LeaseState),
    /// The stream broke, so a new one may resume the lease.
    Lost,
    Failed(Status),
}

/// Whether a failed heartbeat is the server's answer rather than a lost
/// stream.
fn answered(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::FailedPrecondition
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::InvalidArgument
    )
}

impl Lease {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The oldest version the lease holds.
    pub fn oldest(&self) -> Version {
        self.oldest
    }

    /// How long the lease lasts past its last heartbeat.
    pub fn expires_in(&self) -> Duration {
        self.expires_in
    }

    /// Renews the lease, reporting the oldest version the reader still
    /// uses, which may not go down, and its changefeed position. A lease
    /// that ran out fails with `Error::LeaseExpired`; the reader then drops
    /// its handles and reloads.
    pub async fn beat(&mut self, oldest: Version, position: Version) -> Result<(), Error> {
        self.send(pb::LeaseBeat {
            lease: self.id,
            oldest,
            position,
            ..pb::LeaseBeat::default()
        })
        .await
    }

    async fn send(&mut self, beat: pb::LeaseBeat) -> Result<(), Error> {
        if let Some((beats, states)) = &mut self.stream {
            let exchange = match beats.send(beat).await {
                Err(_) => Exchange::Lost,
                Ok(()) => match states.message().await {
                    Ok(Some(state)) => Exchange::Answered(state),
                    Ok(None) => Exchange::Lost,
                    Err(status) if answered(&status) => Exchange::Failed(status),
                    Err(_) => Exchange::Lost,
                },
            };
            match exchange {
                Exchange::Answered(state) => return self.answer(state),
                Exchange::Failed(status) => return Err(Error::Status(status)),
                Exchange::Lost => self.stream = None,
            }
        }
        // Opens the lease, or resumes it on a new stream, at the writer a
        // standby names.
        loop {
            let (beats, inbox) = mpsc::channel(1);
            beats
                .send(pb::LeaseBeat {
                    lease: self.id,
                    ..beat
                })
                .await
                .map_err(|_| Error::LeaseExpired)?;
            let mut rpc = self.client.rpc();
            let mut request =
                tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(inbox));
            request
                .metadata_mut()
                .insert("authorization", self.client.auth.clone());
            let opened = match rpc.lease(request).await {
                Ok(response) => {
                    let mut states = response.into_inner();
                    states.message().await.map(|state| (state, states))
                }
                Err(status) => Err(status),
            };
            return match opened {
                Ok((Some(state), states)) => {
                    self.stream = Some((beats, states));
                    self.answer(state)
                }
                Ok((None, _)) => Err(Error::Status(Status::unavailable("the lease stream ended"))),
                Err(status) if self.client.redirect(&status) => continue,
                Err(status) => {
                    if crate::retriable(&status) {
                        self.client.go_home();
                    }
                    Err(Error::Status(status))
                }
            };
        }
    }

    fn answer(&mut self, state: pb::LeaseState) -> Result<(), Error> {
        if state.expired {
            self.stream = None;
            return Err(Error::LeaseExpired);
        }
        self.id = state.lease;
        self.oldest = state.oldest;
        self.expires_in = Duration::from_millis(state.expires_in_ms);
        Ok(())
    }

    fn release_beat(&self) -> pb::LeaseBeat {
        pb::LeaseBeat {
            lease: self.id,
            release: true,
            ..pb::LeaseBeat::default()
        }
    }

    /// Ends the lease at once.
    pub async fn release(mut self) {
        self.released = true;
        let beat = self.release_beat();
        if let Some((beats, states)) = &mut self.stream
            && beats.send(beat).await.is_ok()
        {
            let _ = states.message().await;
        }
    }
}

/// How long a dropped lease's release may take before it is left to run
/// out.
const RELEASE_WAIT: Duration = Duration::from_secs(5);

impl Drop for Lease {
    fn drop(&mut self) {
        let beat = self.release_beat();
        // A task takes the stream, since dropping it would end it before
        // the release goes out.
        if !self.released
            && let Some((beats, mut states)) = self.stream.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(tokio::time::timeout(RELEASE_WAIT, async move {
                if beats.send(beat).await.is_ok() {
                    let _ = states.message().await;
                }
            }));
        }
    }
}

/// A free-queue entry: what was freed, at which version, and what it kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Freed {
    pub version: Version,
    pub handle: Handle,
    /// Set when a replaced value was freed rather than an inode.
    pub value_version: Option<Version>,
    pub inode: Option<Inode>,
    pub value: Option<Bytes>,
}

impl Freed {
    fn from_pb(freed: pb::Freed) -> Freed {
        Freed {
            version: freed.version,
            handle: Handle::from_pb(freed.handle),
            value_version: freed.value_version,
            inode: freed.inode.map(Inode::from_pb),
            value: freed.value,
        }
    }

    fn to_pb(&self) -> pb::Freed {
        pb::Freed {
            version: self.version,
            handle: Some(self.handle.to_pb()),
            value_version: self.value_version,
            inode: None,
            value: None,
        }
    }
}

/// Entries claimed together, until acknowledged or the claim runs out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub id: u64,
    pub entries: Vec<Freed>,
}

/// A page of the free queue under the key's root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreedPage {
    /// The page's entries in the order freed; possibly none while `next`
    /// is set.
    pub entries: Vec<Freed>,
    /// Where the next page starts, None once this page reached the queue's
    /// end.
    pub next: Option<Bytes>,
    /// Every subtree under the root detached at or before this version has
    /// all its contents in the queue as of this page.
    pub freed_through: Version,
    /// The page reflects this version or a later one.
    pub version: Version,
}

impl Client {
    /// Opens a lease that holds `oldest` and later versions' freed records.
    /// It fails with `FailedPrecondition` below the release horizon.
    pub async fn lease(&self, oldest: Version, position: Version) -> Result<Lease, Error> {
        let mut lease = self.unopened();
        lease.beat(oldest, position).await?;
        Ok(lease)
    }

    /// Opens a lease at the newest committed version, which the release
    /// horizon never passes, so it always opens; `Lease::oldest` reports
    /// the version.
    pub async fn lease_current(&self) -> Result<Lease, Error> {
        let mut lease = self.unopened();
        lease
            .send(pb::LeaseBeat {
                current: true,
                ..pb::LeaseBeat::default()
            })
            .await?;
        Ok(lease)
    }

    fn unopened(&self) -> Lease {
        Lease {
            client: self.clone(),
            id: 0,
            oldest: 0,
            expires_in: Duration::ZERO,
            stream: None,
            released: false,
        }
    }

    /// Claims up to `limit` released entries under this key's root; no
    /// entries when none are released.
    pub async fn claim_freed(&self, limit: u32) -> Result<Claim, Error> {
        let response = self
            .retry(|mut rpc| {
                let request = self.request(pb::ClaimRequest { limit });
                async move { rpc.claim_freed(request).await }
            })
            .await?
            .into_inner();
        Ok(Claim {
            id: response.claim,
            entries: response.freed.into_iter().map(Freed::from_pb).collect(),
        })
    }

    /// Lists up to `limit` free-queue entries under this key's root, from
    /// the queue's start or after a page's `next`. It lists entries a lease
    /// holds and entries a consumer has claimed, and claims none. An entry
    /// freed after a page went out lands past that page's `next`, so pages
    /// read to the end see every entry freed while they ran, unless it was
    /// acknowledged first.
    pub async fn list_freed(&self, after: Option<&[u8]>, limit: u32) -> Result<FreedPage, Error> {
        let after = Bytes::copy_from_slice(after.unwrap_or_default());
        let response = self
            .retry(|mut rpc| {
                let request = self.request(pb::ListFreedRequest {
                    after: after.clone(),
                    limit,
                });
                async move { rpc.list_freed(request).await }
            })
            .await?
            .into_inner();
        Ok(FreedPage {
            entries: response.freed.into_iter().map(Freed::from_pb).collect(),
            next: (!response.next.is_empty()).then_some(response.next),
            freed_through: response.freed_through,
            version: response.version,
        })
    }

    /// Acknowledges claimed entries once what they reference is deleted,
    /// which deletes them and the records they kept; acknowledging none
    /// renews the claim. A claim that ran out fails with `NotFound`, and
    /// its entries return to the queue.
    pub async fn ack_freed(&self, claim: u64, entries: &[Freed]) -> Result<(), Error> {
        let freed: Vec<pb::Freed> = entries.iter().map(Freed::to_pb).collect();
        self.retry(|mut rpc| {
            let request = self.request(pb::AckRequest {
                claim,
                freed: freed.clone(),
            });
            async move { rpc.ack_freed(request).await }
        })
        .await?;
        Ok(())
    }
}
