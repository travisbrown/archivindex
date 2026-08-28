//! Synchronous wiremock helpers for client tests.
//!
//! Scripts return wiremock response templates and observations to inspect after the client finishes.
//! Use [`proxy::RecordingProxy`] when a test needs the actual HTTP/1 bytes or a transport fault that
//! wiremock's HTTP serializer cannot express.

use std::any::Any;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::executor::block_on;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

pub mod proxy;

/// Convenient access to the encoded request target and textual headers of a wiremock request.
pub trait RequestExt {
    /// The encoded path and query, including the leading slash.
    fn path(&self) -> &str;

    /// The first value of a header, if present and valid text.
    fn header(&self, name: &str) -> Option<&str>;
}

impl RequestExt for Request {
    fn path(&self) -> &str {
        &self.url[url::Position::BeforePath..url::Position::AfterQuery]
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }
}

/// A wiremock server and the observations produced by its response script.
pub struct Server<N> {
    server: MockServer,
    observations: Arc<Mutex<Observations<N>>>,
}

struct Observations<N> {
    notes: Vec<N>,
    panic: Option<Box<dyn Any + Send>>,
}

impl<N> Server<N> {
    /// The server's listening port on `127.0.0.1`.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.server.address().port()
    }

    /// Verify the request count and return observations in the order wiremock handled requests.
    ///
    /// Call this after the client finishes. Unlike joining a fixed-count accept loop, verification
    /// fails immediately if the client made fewer requests than expected.
    ///
    /// # Panics
    ///
    /// Panics if the request count differs from the expectation or the response script panicked.
    #[must_use]
    pub fn finish(self) -> Vec<N> {
        let Observations { notes, panic } = std::mem::replace(
            &mut *self.observations.lock().expect("observations lock"),
            Observations {
                notes: Vec::new(),
                panic: None,
            },
        );
        if let Some(panic) = panic {
            resume_unwind(panic);
        }
        block_on(self.server.verify());
        notes
    }
}

/// Serve responses using wiremock, retaining an observation for each request.
///
/// Returns a handle for verifying and inspecting requests. Its [`Server::port`] method provides
/// the listening port on `127.0.0.1`.
///
/// The script runs serially on wiremock's runtime and must not block. Use
/// [`ResponseTemplate::set_delay`] to delay a response without blocking other requests.
/// Requests can run concurrently. Call `finish` after the client finishes its requests; deliberately
/// delayed responses to clients that timed out may still be pending.
///
/// # Panics
///
/// Panics if wiremock cannot start its runtime. Script panics are caught and resumed by `finish`.
pub fn serve_with<N: Send + 'static>(
    requests: usize,
    script: impl FnMut(&Request) -> (ResponseTemplate, N) + Send + 'static,
) -> std::io::Result<Server<N>> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let server = block_on(MockServer::builder().listener(listener).start());
    let observations = Arc::new(Mutex::new(Observations {
        notes: Vec::new(),
        panic: None,
    }));
    let captured = Arc::clone(&observations);
    let script = Mutex::new(script);
    block_on(
        Mock::given(wiremock::matchers::any())
            .respond_with(move |request: &Request| {
                let mut observations = captured.lock().expect("observations lock");
                if observations.panic.is_some() {
                    return response(500, &[], "response script panicked");
                }
                let mut script = script.lock().expect("script lock");
                match catch_unwind(AssertUnwindSafe(|| script(request))) {
                    Ok((response, note)) => {
                        observations.notes.push(note);
                        response
                    }
                    Err(panic) => {
                        observations.panic = Some(panic);
                        drop(observations);
                        response(500, &[], "response script panicked")
                    }
                }
            })
            .expect(u64::try_from(requests).expect("request count fits u64"))
            .mount(&server),
    );
    Ok(Server {
        server,
        observations,
    })
}

/// Build a wiremock response with explicit headers and a byte body.
///
/// Connections close after each response, so recorded connections contain exactly one exchange.
/// A fixed date makes serialized fixtures reproducible across requests and test runs.
#[must_use]
pub fn response(status: u16, headers: &[(&str, &str)], body: impl AsRef<[u8]>) -> ResponseTemplate {
    headers.iter().fold(
        ResponseTemplate::new(status)
            .insert_header("connection", "close")
            .insert_header("date", "Wed, 01 Jan 2025 00:00:00 GMT")
            .set_body_bytes(body.as_ref()),
        |response, &(name, value)| response.append_header(name, value),
    )
}

/// Serialize a response through wiremock, for fixtures that embed a complete HTTP/1 response.
///
/// The template must close its connection, as templates from [`response`] do.
pub fn response_bytes(response: ResponseTemplate) -> std::io::Result<Vec<u8>> {
    let server = serve_with(1, move |_| (response.clone(), ()))?;
    let mut stream = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, server.port()))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    let _ = server.finish();
    Ok(bytes)
}

/// An ephemeral loopback port with no listener, for connection failure tests.
///
/// Another process could claim the port after this function returns.
pub fn dead_port() -> std::io::Result<u16> {
    Ok(TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?
        .local_addr()?
        .port())
}
