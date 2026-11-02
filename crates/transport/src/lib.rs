//! gRPC's transport, directly on HTTP/2: a client channel and a server loop
//! for tonic's generated code, with nothing between them and the connection.
//!
//! What it speaks is plain gRPC, so any gRPC client or server talks to it.
//! Three things make it cheaper than tonic's own transport:
//!
//! - The client sends a request's headers and message in one write, from the
//!   caller's task, so a call takes one trip to the connection's task, and the
//!   server finds the whole request when it reads.
//! - The server polls a request once on the connection's task. A request that
//!   needs nothing else, such as a read the cache holds, answers there, with
//!   no task of its own, and goes out in one write. Only a request that has to
//!   wait gets a task.
//! - Neither side has a router, a layer of middleware or a buffer task.

use bytes::Bytes;
use h2::{Ping, RecvStream, SendStream};
use http_body::{Body, Frame};
use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tonic::Status;
use tower_service::Service;

/// How often an idle connection is checked, and how long a check may take.
const KEEPALIVE: Duration = Duration::from_secs(20);
/// What a stream may have in flight; large batches take several windows.
const STREAM_WINDOW: u32 = 2 << 20;
/// What a connection may have in flight, over all its streams.
const CONNECTION_WINDOW: u32 = 16 << 20;
/// The largest frame either side takes.
const MAX_FRAME: u32 = 1 << 20;
/// The most requests a connection runs at once.
const MAX_STREAMS: u32 = 16 * 1024;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A gRPC status for what HTTP/2 reported. A stream the peer canceled reads as
/// canceled, and anything else as a connection that failed, which is safe to
/// retry since every call is.
fn status(error: h2::Error) -> Status {
    if error.reason() == Some(h2::Reason::CANCEL) {
        Status::cancelled(error.to_string())
    } else {
        Status::unavailable(format!("the connection failed: {error}"))
    }
}

/// A stream's data and trailers, as a body. It hands back flow-control window
/// as its reader takes data.
pub struct Incoming {
    recv: RecvStream,
    data_done: bool,
}

impl Incoming {
    fn new(recv: RecvStream) -> Incoming {
        Incoming {
            recv,
            data_done: false,
        }
    }
}

impl Body for Incoming {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        if !this.data_done {
            match this.recv.poll_data(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(data))) => {
                    let _ = this.recv.flow_control().release_capacity(data.len());
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(status(error)))),
                Poll::Ready(None) => this.data_done = true,
            }
        }
        match this.recv.poll_trailers(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Some(trailers))) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(status(error)))),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.recv.is_end_stream()
    }
}

/// A body's next frame, or None at its end.
type Next<B> = Option<Result<Frame<<B as Body>::Data>, <B as Body>::Error>>;

/// Polls a body once.
fn poll_body<B: Body + Unpin>(body: &mut B, cx: &mut Context<'_>) -> Poll<Next<B>> {
    Pin::new(body).poll_frame(cx)
}

/// Sends `data` as the window allows.
async fn send_data(send: &mut SendStream<Bytes>, mut data: Bytes) -> Result<(), h2::Error> {
    while !data.is_empty() {
        send.reserve_capacity(data.len());
        let granted = match poll_fn(|cx| send.poll_capacity(cx)).await {
            Some(granted) => granted?,
            None => return Err(h2::Error::from(h2::Reason::CANCEL)),
        };
        let chunk = data.split_to(granted.min(data.len()));
        send.send_data(chunk, false)?;
    }
    Ok(())
}

/// Sends `ready`, already taken from a body, and what remains of the body,
/// ending when the peer resets the stream.
async fn pump<B>(mut body: B, mut send: SendStream<Bytes>, ready: [Option<Bytes>; 2])
where
    B: Body<Data = Bytes> + Unpin,
{
    for data in ready.into_iter().flatten() {
        if send_data(&mut send, data).await.is_err() {
            return;
        }
    }
    loop {
        let next = poll_fn(|cx| {
            if send.poll_reset(cx).is_ready() {
                return Poll::Ready(None);
            }
            poll_body(&mut body, cx).map(Some)
        })
        .await;
        match next {
            // The peer left.
            None => return,
            // The body has ended.
            Some(None) => {
                let _ = send.send_data(Bytes::new(), true);
                return;
            }
            Some(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => {
                    if send_data(&mut send, data).await.is_err() {
                        return;
                    }
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        let _ = send.send_trailers(trailers);
                    }
                    return;
                }
            },
            Some(Some(Err(_))) => {
                send.send_reset(h2::Reason::INTERNAL_ERROR);
                return;
            }
        }
    }
}

// The client.

/// A client's connection, made when first needed and made again after one
/// fails. Clones share it.
#[derive(Clone)]
pub struct Channel {
    shared: Arc<Connector>,
}

struct Connector {
    address: String,
    authority: http::uri::Authority,
    live: RwLock<Option<Connection>>,
    /// Held while connecting, so one failed connection makes one new one.
    connecting: Mutex<()>,
}

#[derive(Clone)]
struct Connection {
    send: h2::client::SendRequest<Bytes>,
    /// The task that runs the connection. It ends once every channel and every
    /// stream on the connection has gone, so a stream outlives its channel.
    task: Arc<JoinHandle<()>>,
}

/// Why an address can't be connected to.
#[derive(Debug)]
pub struct BadAddress(String);

impl std::fmt::Display for BadAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BadAddress {}

impl Channel {
    /// A channel to `http://host:port`, connecting when first used.
    pub fn lazy(endpoint: &str) -> Result<Channel, BadAddress> {
        let uri: http::Uri = endpoint
            .parse()
            .map_err(|error| BadAddress(format!("{endpoint}: {error}")))?;
        if uri.scheme_str() != Some("http") {
            return Err(BadAddress(format!("{endpoint}: only http:// is served")));
        }
        let authority = uri
            .authority()
            .ok_or_else(|| BadAddress(format!("{endpoint}: no host")))?
            .clone();
        Ok(Channel {
            shared: Arc::new(Connector {
                address: format!(
                    "{}:{}",
                    authority.host(),
                    authority.port_u16().unwrap_or(80)
                ),
                authority,
                live: RwLock::new(None),
                connecting: Mutex::new(()),
            }),
        })
    }

    /// A channel to `http://host:port`, connected.
    pub async fn connect(endpoint: &str) -> Result<Channel, Status> {
        let channel =
            Channel::lazy(endpoint).map_err(|error| Status::invalid_argument(error.to_string()))?;
        channel.connection(None).await?;
        Ok(channel)
    }

    /// The live connection, or a new one in place of `failed`.
    async fn connection(&self, failed: Option<&Connection>) -> Result<Connection, Status> {
        let current = || self.shared.live.read().expect("no panics hold it").clone();
        let is_failed = |connection: &Connection| {
            failed.is_some_and(|failed| Arc::ptr_eq(&failed.task, &connection.task))
        };
        if let Some(connection) = current().filter(|connection| !is_failed(connection)) {
            return Ok(connection);
        }
        let _connecting = self.shared.connecting.lock().await;
        if let Some(connection) = current().filter(|connection| !is_failed(connection)) {
            return Ok(connection);
        }
        let unavailable = |error: &dyn std::fmt::Display| {
            Status::unavailable(format!("connecting to {}: {error}", self.shared.address))
        };
        let tcp = TcpStream::connect(&self.shared.address)
            .await
            .map_err(|error| unavailable(&error))?;
        tcp.set_nodelay(true).map_err(|error| unavailable(&error))?;
        let (send, mut connection) = h2::client::Builder::new()
            .initial_window_size(STREAM_WINDOW)
            .initial_connection_window_size(CONNECTION_WINDOW)
            .max_frame_size(MAX_FRAME)
            .max_header_list_size(1 << 20)
            .handshake::<_, Bytes>(tcp)
            .await
            .map_err(|error| unavailable(&error))?;
        let ping = connection.ping_pong();
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let task = Arc::new(task);
        if let Some(ping) = ping {
            tokio::spawn(keepalive(ping, Arc::downgrade(&task)));
        }
        let connection = Connection { send, task };
        let replaced = self
            .shared
            .live
            .write()
            .expect("no panics hold it")
            .replace(connection.clone());
        if let Some(replaced) = replaced {
            replaced.task.abort();
        }
        Ok(connection)
    }

    /// Sends a request, on a new connection if the live one has failed.
    async fn open(&self) -> Result<h2::client::SendRequest<Bytes>, Status> {
        let connection = self.connection(None).await?;
        match connection.send.clone().ready().await {
            Ok(send) => Ok(send),
            Err(_) => {
                let connection = self.connection(Some(&connection)).await?;
                connection.send.clone().ready().await.map_err(status)
            }
        }
    }

    async fn call(
        self,
        request: http::Request<tonic::body::Body>,
    ) -> Result<http::Response<Incoming>, Status> {
        let timeout = attempt_timeout(request.headers());
        let (mut parts, mut body) = request.into_parts();
        let mut uri = http::uri::Parts::from(std::mem::take(&mut parts.uri));
        uri.scheme = Some(http::uri::Scheme::HTTP);
        uri.authority = Some(self.shared.authority.clone());
        parts.uri =
            http::Uri::from_parts(uri).map_err(|error| Status::internal(error.to_string()))?;
        // A unary call's message is ready now, and goes out with its headers.
        // A stream's messages follow on a task of their own.
        let (ready, ended) = ready_frames(&mut body)?;
        let answer = async {
            let mut send = self.open().await?;
            let head = http::Request::from_parts(parts, ());
            let (response, mut stream) = send
                .send_request(head, ended && ready[0].is_none())
                .map_err(status)?;
            if ended {
                if let Some(first) = ready.into_iter().flatten().next() {
                    stream.send_data(first, true).map_err(status)?;
                }
            } else {
                tokio::spawn(pump(body, stream, ready));
            }
            Ok::<_, Status>(response.await.map_err(status)?.map(Incoming::new))
        };
        match timeout {
            None => answer.await,
            Some(timeout) => tokio::time::timeout(timeout, answer)
                .await
                .unwrap_or_else(|_| Err(Status::deadline_exceeded("the attempt timed out"))),
        }
    }
}

/// What a request's body holds already: up to two messages, and whether that
/// is all of it, which it can be only with room for the end after a message:
/// a call's one message is followed by the end, and any more wait for `pump`.
fn ready_frames(body: &mut tonic::body::Body) -> Result<([Option<Bytes>; 2], bool), Status> {
    let mut ready = [None, None];
    let mut cx = Context::from_waker(Waker::noop());
    for slot in &mut ready {
        match poll_body(body, &mut cx) {
            Poll::Pending => return Ok((ready, false)),
            Poll::Ready(None) => return Ok((ready, true)),
            Poll::Ready(Some(frame)) => *slot = frame?.into_data().ok(),
        }
    }
    Ok((ready, false))
}

/// Checks a connection every `KEEPALIVE`, and ends one that stops answering.
/// It ends with the connection.
async fn keepalive(mut ping: h2::PingPong, connection: std::sync::Weak<JoinHandle<()>>) {
    loop {
        tokio::time::sleep(KEEPALIVE).await;
        let answered = tokio::time::timeout(KEEPALIVE, ping.ping(Ping::opaque())).await;
        match (answered, connection.upgrade()) {
            (Ok(Ok(_)), Some(_)) => {}
            (_, Some(task)) => return task.abort(),
            (_, None) => return,
        }
    }
}

/// What a request's `grpc-timeout` header allows, which tonic's requests carry
/// from `set_timeout`.
fn attempt_timeout(headers: &http::HeaderMap) -> Option<Duration> {
    let text = headers.get("grpc-timeout")?.to_str().ok()?;
    let (digits, unit) = text.split_at(text.len().checked_sub(1)?);
    let count: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(count * 3600),
        "M" => Duration::from_secs(count * 60),
        "S" => Duration::from_secs(count),
        "m" => Duration::from_millis(count),
        "u" => Duration::from_micros(count),
        "n" => Duration::from_nanos(count),
        _ => return None,
    })
}

impl Service<http::Request<tonic::body::Body>> for Channel {
    type Response = http::Response<Incoming>;
    type Error = Status;
    type Future = BoxFuture<Result<http::Response<Incoming>, Status>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        Box::pin(self.clone().call(request))
    }
}

// The server.

/// Serves `service` on `listener` until `shutdown` resolves, then lets each
/// connection finish what it has in progress.
pub async fn serve<S, B>(
    listener: TcpListener,
    service: S,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()>
where
    S: Service<http::Request<Incoming>, Response = http::Response<B>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Send,
{
    let (close, closing) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut shutdown = pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok((tcp, _)) => {
                    connections.spawn(connection(tcp, service.clone(), closing.clone()));
                }
                // Out of descriptors, or a connection already gone: the next
                // accept may fare better.
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            },
        }
    }
    drop(listener);
    close.send_replace(true);
    while connections.join_next().await.is_some() {}
    Ok(())
}

/// Makes a socket send each write at once. A reply goes out as several
/// writes when it streams, and Nagle's algorithm would hold each for the
/// client's delayed acknowledgment, 40 ms on Linux.
fn prepare(tcp: &TcpStream) -> std::io::Result<()> {
    tcp.set_nodelay(true)
}

/// Runs one connection: it reads requests, polls each once, and writes what
/// their answers leave for it.
async fn connection<S, B>(tcp: TcpStream, mut service: S, mut closing: watch::Receiver<bool>)
where
    S: Service<http::Request<Incoming>, Response = http::Response<B>, Error = Infallible>
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Send,
{
    if prepare(&tcp).is_err() {
        return;
    }
    let handshake = h2::server::Builder::new()
        .initial_window_size(STREAM_WINDOW)
        .initial_connection_window_size(CONNECTION_WINDOW)
        .max_frame_size(MAX_FRAME)
        .max_concurrent_streams(MAX_STREAMS)
        .max_header_list_size(1 << 20)
        .handshake::<_, Bytes>(tcp);
    let Ok(mut connection) = handshake.await else {
        return;
    };
    let (dead, mut died) = oneshot::channel();
    if let Some(ping) = connection.ping_pong() {
        tokio::spawn(check(ping, dead));
    }
    let mut closed = pin!(async {
        let _ = closing.wait_for(|closing| *closing).await;
    });
    let mut stopping = false;
    loop {
        tokio::select! {
            biased;
            accepted = connection.accept() => match accepted {
                Some(Ok((request, respond))) => {
                    start(&mut service, request.map(Incoming::new), respond);
                }
                // The connection ended, or failed and took its streams along.
                Some(Err(_)) | None => return,
            },
            () = &mut closed, if !stopping => {
                stopping = true;
                connection.graceful_shutdown();
            }
            // The client stopped answering, so what it has open ends here.
            _ = &mut died => return,
        }
    }
}

/// Checks a server's connection every `KEEPALIVE`, and reports one that stops
/// answering. It ends with the connection.
async fn check(mut ping: h2::PingPong, dead: oneshot::Sender<()>) {
    loop {
        tokio::time::sleep(KEEPALIVE).await;
        let answered = tokio::time::timeout(KEEPALIVE, ping.ping(Ping::opaque())).await;
        if !matches!(answered, Ok(Ok(_))) {
            let _ = dead.send(());
            return;
        }
    }
}

/// Takes a request: polls it once here, and gives it a task only if it has to
/// wait. A first poll with no waker is safe, since the task polls again.
fn start<S, B>(
    service: &mut S,
    request: http::Request<Incoming>,
    respond: h2::server::SendResponse<Bytes>,
) where
    S: Service<http::Request<Incoming>, Response = http::Response<B>, Error = Infallible>,
    S::Future: Send + 'static,
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Send,
{
    let mut future = Box::pin(service.call(request));
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(response)) => answer(response, respond),
        Poll::Ready(Err(never)) => match never {},
        Poll::Pending => {
            tokio::spawn(wait(future, respond));
        }
    }
}

/// Answers a request that had to wait, unless the client leaves first.
async fn wait<F, B>(mut future: Pin<Box<F>>, mut respond: h2::server::SendResponse<Bytes>)
where
    F: Future<Output = Result<http::Response<B>, Infallible>>,
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Send,
{
    let response = poll_fn(|cx| {
        if respond.poll_reset(cx).is_ready() {
            return Poll::Ready(None);
        }
        future.as_mut().poll(cx).map(Some)
    })
    .await;
    match response {
        Some(Ok(response)) => answer(response, respond),
        Some(Err(never)) => match never {},
        None => {}
    }
}

/// Sends a response as far as it is ready, all into one write, and leaves
/// anything it must wait for to a task.
fn answer<B>(response: http::Response<B>, mut respond: h2::server::SendResponse<Bytes>)
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Send,
{
    let (parts, mut body) = response.into_parts();
    let ended = body.is_end_stream();
    let head = http::Response::from_parts(parts, ());
    let Ok(mut send) = respond.send_response(head, ended) else {
        return;
    };
    if ended {
        return;
    }
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        match poll_body(&mut body, &mut cx) {
            Poll::Pending => {
                tokio::spawn(pump(body, send, [None, None]));
                return;
            }
            Poll::Ready(None) => {
                let _ = send.send_data(Bytes::new(), true);
                return;
            }
            Poll::Ready(Some(Err(_))) => {
                send.send_reset(h2::Reason::INTERNAL_ERROR);
                return;
            }
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => {
                    send.reserve_capacity(data.len());
                    if send.capacity() < data.len() {
                        tokio::spawn(pump(body, send, [Some(data), None]));
                        return;
                    }
                    if send.send_data(data, false).is_err() {
                        return;
                    }
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        let _ = send.send_trailers(trailers);
                    }
                    return;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn accepted_connections_send_at_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("bound");
        let _client = TcpStream::connect(addr).await.expect("connects");
        let (stream, _) = listener.accept().await.expect("accepts");
        prepare(&stream).expect("sets the option");
        assert!(stream.nodelay().expect("reads the option"));
    }

    #[test]
    fn attempts_read_their_timeout() {
        let timeout = |text: &'static str| {
            let mut headers = http::HeaderMap::new();
            headers.insert("grpc-timeout", http::HeaderValue::from_static(text));
            attempt_timeout(&headers)
        };
        assert_eq!(timeout("2S"), Some(Duration::from_secs(2)));
        assert_eq!(timeout("150m"), Some(Duration::from_millis(150)));
        assert_eq!(timeout("9u"), Some(Duration::from_micros(9)));
        assert_eq!(timeout("oops"), None);
        assert_eq!(timeout(""), None);
    }

    #[test]
    fn channels_take_plain_http_addresses() {
        assert!(Channel::lazy("http://127.0.0.1:7070").is_ok());
        assert!(Channel::lazy("https://127.0.0.1:7070").is_err());
        assert!(Channel::lazy("not an address").is_err());
    }
}
