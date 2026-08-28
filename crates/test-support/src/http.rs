//! Scripted loopback HTTP servers for testing clients with fixed responses.
//!
//! Each server listens on an ephemeral `127.0.0.1` port and handles a fixed number of connections.
//! Joining the server thread returns values produced by the script, allowing tests to inspect
//! requests after the client finishes.

use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// An HTTP/1.1 request captured by a server.
///
/// The server reads the header section and the number of body bytes declared by `Content-Length`.
/// If the client disconnects early, the request retains the bytes received before disconnection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    bytes: Vec<u8>,
    head: String,
    body_start: usize,
}

impl Request {
    fn read(stream: &mut impl Read) -> Self {
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        let mut expected = None;

        loop {
            if expected.is_none() {
                expected = head_end(&bytes).map(|end| {
                    let head = String::from_utf8_lossy(&bytes[..end]);
                    (end, end + content_length(&head))
                });
            }
            if let Some((_, total)) = expected.filter(|(_, total)| bytes.len() >= *total) {
                bytes.truncate(total);
                break;
            }
            match stream.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => bytes.extend_from_slice(&buffer[..read]),
            }
        }

        let body_start = expected.map_or(bytes.len(), |(end, _)| end);
        Self {
            head: String::from_utf8_lossy(&bytes[..body_start]).into_owned(),
            bytes,
            body_start,
        }
    }

    /// The raw request bytes captured by the server.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The header section decoded as UTF-8, replacing invalid byte sequences.
    ///
    /// The terminating blank line is included when it was received.
    #[must_use]
    pub fn head(&self) -> &str {
        &self.head
    }

    /// The body declared by `Content-Length`.
    ///
    /// This is empty if the header section is incomplete or `Content-Length` is absent or invalid.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.bytes[self.body_start..]
    }

    /// The request method, or an empty string if none was received.
    #[must_use]
    pub fn method(&self) -> &str {
        self.head.split(' ').next().unwrap_or_default()
    }

    /// The request target, or `/` if it is unavailable.
    #[must_use]
    pub fn path(&self) -> &str {
        self.head.split(' ').nth(1).unwrap_or("/")
    }

    /// The trimmed value of the first matching header.
    ///
    /// Header names are compared case-insensitively.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        header_value(&self.head, name)
    }
}

/// The index after the blank line that ends the header section.
fn head_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

/// The declared body length, or zero if `Content-Length` is absent or invalid.
fn content_length(head: &str) -> usize {
    header_value(head, "content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (field, value) = line.split_once(':')?;
        field.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// A scripted response with an optional delay before the connection closes.
///
/// Delaying closure lets tests verify that a client respects response framing instead of waiting
/// for the server to close the connection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Reply {
    bytes: Vec<u8>,
    linger: Duration,
}

impl Reply {
    /// A reply that closes the connection immediately after `bytes` are written.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            linger: Duration::ZERO,
        }
    }

    /// Delay closing the connection for `linger` after the reply is written.
    #[must_use]
    pub const fn lingering(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }
}

impl From<Vec<u8>> for Reply {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

/// Build an HTTP/1.1 text response with `Content-Length` and `Connection: close` headers.
#[must_use]
pub fn response(status: &str, headers: &[(&str, &str)], body: &str) -> Vec<u8> {
    let headers = headers
        .iter()
        .fold(String::new(), |mut text, (name, value)| {
            write!(text, "{name}: {value}\r\n").expect("a String accepts any write");
            text
        });

    format!(
        "HTTP/1.1 {status}\r\n{headers}content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// Serve a fixed number of connections sequentially.
///
/// For each request, `script` returns the reply to send and a value to retain. Joining the returned
/// handle yields the retained values in acceptance order. If accepting a connection fails, the
/// server stops and returns the values collected so far. Response write errors are ignored because
/// tests may deliberately disconnect early.
pub fn serve_with<R: Into<Reply>, N: Send + 'static>(
    connections: usize,
    script: impl Fn(&Request) -> (R, N) + Send + 'static,
) -> io::Result<(u16, JoinHandle<Vec<N>>)> {
    let (listener, port) = listen()?;
    let handle = thread::spawn(move || {
        listener
            .incoming()
            .take(connections)
            .map_while(Result::ok)
            .map(|stream| answer(stream, &script))
            .collect()
    });

    Ok((port, handle))
}

/// Serve a fixed number of connections concurrently.
///
/// Each connection runs on a separate thread, so a stalled reply cannot block later connections.
/// The script receives the request's index in acceptance order. Joining the returned handle yields
/// retained values in the same order; a handler that panics contributes no value.
pub fn serve_concurrently_with<R: Into<Reply>, N: Send + 'static>(
    connections: usize,
    script: impl Fn(usize, &Request) -> (R, N) + Send + Sync + 'static,
) -> io::Result<(u16, JoinHandle<Vec<N>>)> {
    let (listener, port) = listen()?;
    let script = Arc::new(script);
    let handle = thread::spawn(move || {
        #[expect(
            clippy::needless_collect,
            reason = "all connections must be accepted before any handler is joined"
        )]
        let handlers = listener
            .incoming()
            .take(connections)
            .map_while(Result::ok)
            .enumerate()
            .map(|(index, stream)| {
                let script = Arc::clone(&script);
                thread::spawn(move || answer(stream, |request| script(index, request)))
            })
            .collect::<Vec<_>>();

        handlers
            .into_iter()
            .filter_map(|handler| handler.join().ok())
            .collect()
    });

    Ok((port, handle))
}

/// Bind and release an ephemeral loopback port for testing connection failures.
///
/// The port is not reserved; another process could bind it before the test connects.
pub fn dead_port() -> io::Result<u16> {
    listen().map(|(_, port)| port)
}

fn listen() -> io::Result<(TcpListener, u16)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();

    Ok((listener, port))
}

fn answer<R: Into<Reply>, N>(mut stream: TcpStream, script: impl FnOnce(&Request) -> (R, N)) -> N {
    let request = Request::read(&mut stream);
    let (reply, note) = script(&request);
    let reply = reply.into();
    let _ = stream.write_all(&reply.bytes);
    thread::sleep(reply.linger);

    note
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{Reply, Request, dead_port, response, serve_concurrently_with, serve_with};

    fn exchange(port: u16, request: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("a connection");
        stream.write_all(request).expect("a sent request");
        let mut received = Vec::new();
        stream.read_to_end(&mut received).expect("a response");

        received
    }

    #[test]
    fn response_frames_the_body_and_closes() {
        assert_eq!(
            response("200 OK", &[("content-type", "text/plain")], "hello"),
            b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 5\r\n\
              connection: close\r\n\r\nhello"
        );
    }

    #[test]
    fn a_request_is_read_through_its_announced_body() {
        let mut bytes: &[u8] =
            b"POST /submit?x=1 HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nhelloEXTRA";
        let request = Request::read(&mut bytes);

        assert_eq!(request.method(), "POST");
        assert_eq!(request.path(), "/submit?x=1");
        assert_eq!(request.header("host"), Some("test"));
        assert_eq!(request.header("missing"), None);
        assert_eq!(request.body(), b"hello");
        assert_eq!(request.bytes().len(), request.head().len() + 5);
    }

    #[test]
    fn abandoned_request_retains_received_bytes() {
        let mut bytes: &[u8] = b"GET /partial HTTP/1.1\r\nContent-Length: 10\r\n\r\nabc";
        let request = Request::read(&mut bytes);

        assert_eq!(request.path(), "/partial");
        assert_eq!(request.body(), b"abc");

        let mut bytes: &[u8] = b"GE";
        let request = Request::read(&mut bytes);

        assert_eq!(request.method(), "GE");
        assert_eq!(request.path(), "/");
        assert_eq!(request.body(), b"");
    }

    #[test]
    fn serve_with_answers_in_order_and_returns_the_notes() {
        let (port, server) = serve_with(2, |request| {
            (
                response("200 OK", &[], request.path()),
                (request.method().to_owned(), request.body().to_vec()),
            )
        })
        .expect("a server");

        let first = exchange(port, b"GET /one HTTP/1.1\r\n\r\n");
        let second = exchange(port, b"POST /two HTTP/1.1\r\ncontent-length: 2\r\n\r\nhi");

        assert!(first.ends_with(b"\r\n\r\n/one"));
        assert!(second.ends_with(b"\r\n\r\n/two"));
        assert_eq!(
            server.join().expect("a finished server"),
            vec![
                ("GET".to_owned(), Vec::new()),
                ("POST".to_owned(), b"hi".to_vec())
            ]
        );
    }

    #[test]
    fn serve_with_stops_after_the_requested_connection_count() {
        let (port, server) =
            serve_with(1, |_| (response("200 OK", &[], ""), ())).expect("a server");

        exchange(port, b"GET / HTTP/1.1\r\n\r\n");
        assert_eq!(server.join().expect("a finished server"), vec![()]);
        assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    }

    #[test]
    fn a_lingering_reply_delays_connection_close() {
        let linger = Duration::from_millis(100);
        let (port, server) = serve_with(1, move |_| {
            (
                Reply::new(b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()).lingering(linger),
                (),
            )
        })
        .expect("a server");

        let started = Instant::now();
        let received = exchange(port, b"GET / HTTP/1.1\r\n\r\n");

        assert_eq!(received, b"HTTP/1.1 204 No Content\r\n\r\n");
        assert!(started.elapsed() >= linger);
        server.join().expect("a finished server");
    }

    #[test]
    fn concurrent_connections_are_not_blocked_by_a_stalled_one() {
        let (port, server) = serve_concurrently_with(2, |index, request| {
            if index == 0 {
                thread::sleep(Duration::from_millis(200));
            }
            (
                response("200 OK", &[], ""),
                (index, request.path().to_owned()),
            )
        })
        .expect("a server");

        let mut stalled = TcpStream::connect(("127.0.0.1", port)).expect("a connection");
        stalled
            .write_all(b"GET /stalled HTTP/1.1\r\n\r\n")
            .expect("a sent request");
        stalled
            .shutdown(Shutdown::Write)
            .expect("a half-closed connection");
        let started = Instant::now();
        exchange(port, b"GET /prompt HTTP/1.1\r\n\r\n");
        assert!(started.elapsed() < Duration::from_millis(200));

        let mut notes = server.join().expect("a finished server");
        notes.sort_unstable();
        assert_eq!(
            notes,
            vec![(0, "/stalled".to_owned()), (1, "/prompt".to_owned())]
        );
    }

    #[test]
    fn dead_port_refuses_connections() {
        let port = dead_port().expect("a port");

        assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    }
}
