//! A TCP recording relay for HTTP/1 fixtures that close their connections.
//!
//! The relay does not parse HTTP. It forwards request bytes to wiremock, buffers its response until
//! EOF, and optionally alters those bytes before forwarding them to the client. Each connection
//! carries one exchange. Buffering makes this unsuitable for testing streaming or response timing.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long a single socket operation may block before the relay treats the peer as unresponsive.
const SOCKET_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a client read waits before the relay re-checks whether wiremock has replied.
///
/// A client may hold its write side open while it waits for the response, leaving the copying
/// thread blocked on a request that is already complete. Shutting the socket down from another
/// thread releases that read on Unix but not on Windows, so the wait is bounded and retried.
const CLIENT_POLL: Duration = Duration::from_millis(50);

/// Bytes observed on one connection, before any HTTP parsing by the client.
#[derive(Debug)]
pub struct Exchange {
    /// Bytes forwarded from the client to wiremock.
    pub request: Vec<u8>,
    /// The complete response offered to the client, after any requested alteration.
    ///
    /// A client that times out or stops reading early may receive only a prefix of these bytes.
    pub response: Vec<u8>,
}

/// A TCP relay with bounded socket waits and one worker per connection.
pub struct RecordingProxy {
    port: u16,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<std::io::Result<Vec<Exchange>>>>,
}

impl RecordingProxy {
    /// Forward connections to a loopback port, optionally altering each complete response.
    ///
    /// `edit` receives the recorded request and wiremock's serialized response. It runs on a
    /// dedicated connection thread, so a test can gate one response without blocking other clients.
    /// Any waits in `edit` must have their own timeout. Socket operations time out after ten seconds.
    pub fn start(
        upstream_port: u16,
        edit: impl Fn(&[u8], &mut Vec<u8>) + Send + Sync + 'static,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let port = listener.local_addr()?.port();
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopping);
        let edit = Arc::new(edit);
        let worker = std::thread::spawn(move || {
            let mut workers = Vec::new();
            for stream in listener.incoming() {
                let stream = stream?;
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let edit = Arc::clone(&edit);
                workers.push(std::thread::spawn(move || {
                    exchange(stream, upstream_port, &*edit)
                }));
            }
            workers
                .into_iter()
                .map(join)
                .collect::<Vec<_>>()
                .into_iter()
                .collect()
        });
        Ok(Self {
            port,
            stopping,
            worker: Some(worker),
        })
    }

    /// The proxy's listening port on `127.0.0.1`.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Stop accepting connections and collect exchanges in connection acceptance order.
    ///
    /// Call after the client finishes and before dropping its wiremock server.
    pub fn finish(mut self) -> std::io::Result<Vec<Exchange>> {
        self.stop();
        self.worker.take().map_or_else(|| Ok(Vec::new()), join)
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        // Wake the accept loop. The wake-up connection is never forwarded or recorded.
        let _ = TcpStream::connect((Ipv4Addr::LOCALHOST, self.port));
    }
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.stop();
            let _ = worker.join();
        }
    }
}

fn join<T>(worker: JoinHandle<std::io::Result<T>>) -> std::io::Result<T> {
    worker
        .join()
        .map_err(|_| std::io::Error::other("recording proxy worker panicked"))?
}

fn exchange(
    mut client: TcpStream,
    upstream_port: u16,
    edit: &impl Fn(&[u8], &mut Vec<u8>),
) -> std::io::Result<Exchange> {
    let mut upstream = TcpStream::connect((Ipv4Addr::LOCALHOST, upstream_port))?;
    client.set_read_timeout(Some(CLIENT_POLL))?;
    client.set_write_timeout(Some(SOCKET_TIMEOUT))?;
    upstream.set_read_timeout(Some(SOCKET_TIMEOUT))?;
    upstream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
    let replied = Arc::new(AtomicBool::new(false));
    let answered = Arc::clone(&replied);
    let mut reader = client.try_clone()?;
    let mut writer = upstream.try_clone()?;
    let requests = std::thread::spawn(move || {
        let mut request = Vec::new();
        let mut buffer = [0; 8192];
        let started = Instant::now();
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    writer.write_all(&buffer[..read])?;
                    request.extend_from_slice(&buffer[..read]);
                }
                // An idle client has sent everything it means to send once wiremock has replied,
                // since wiremock replies only to a complete request.
                Err(error) if waited(&error) => {
                    if answered.load(Ordering::Acquire) {
                        break;
                    }
                    if started.elapsed() > SOCKET_TIMEOUT {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        let _ = writer.shutdown(Shutdown::Write);
        Ok(request)
    });
    let mut response = Vec::new();
    let received = upstream.read_to_end(&mut response);
    // Wiremock has finished reading the request, so the copying thread can stop waiting on a
    // client that is holding its connection open for the response.
    replied.store(true, Ordering::Release);
    let request = join(requests)?;
    received?;
    edit(&request, &mut response);
    if let Err(error) = client.write_all(&response) {
        // Tests deliberately truncate captures and disconnect before the full response arrives.
        if !matches!(
            error.kind(),
            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
        ) {
            return Err(error);
        }
    }
    Ok(Exchange { request, response })
}

/// Whether a socket operation returned because its timeout expired rather than because it failed.
///
/// A read that times out reports `WouldBlock` on Unix and `TimedOut` on Windows.
fn waited(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}
