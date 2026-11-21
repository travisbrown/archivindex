//! Byte-exact capture of live HTTP exchanges.
//!
//! [`Recorder`] returns the exact HTTP/1.1 request and response bytes in [`Exchange`], preserving
//! header spelling, reason phrases, chunk framing, and trailers.
//!
//! Each fetch opens one connection for one request and response. It does not follow redirects,
//! decode content, or reuse the connection. It adds `host` when absent and defaults a missing
//! `connection` header to `close`. Interim (`1xx`) responses are discarded. An unframed response,
//! or one whose final transfer coding is not `chunked`, ends when the connection closes.
//!
//! [`max_response_length`](Recorder::max_response_length) limits captured response bytes. After a
//! complete head, a size limit, disconnect, or read timeout returns a truncated response.
//! Before that point, failures return an error. [`Recorder::new`] sets [`DEFAULT_TIMEOUT`] per
//! connection step and [`DEFAULT_MAX_RESPONSE_LENGTH`] per response. Timeout and size setters
//! accept `None` to remove their bounds. [`Client::fetch_with_deadline`] adds a deadline, excluding DNS
//! resolution.

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use archivindex_http::message::ResponseMetadata;
use http::{Method, Version};
use jiff::Timestamp;
use rustls::pki_types::ServerName;

use crate::framing::ResponseError;
use crate::read::read_response;
use crate::reconstruct::reconstruct_request;
use crate::{
    Client, DEFAULT_MAX_RESPONSE_LENGTH, DEFAULT_TIMEOUT, Engine, Error, Exchange, Fidelity,
    HttpProtocol, ProxyError, Request, TlsVersion, request, socks, tls,
};

/// An HTTP/1.1 client that records the exact bytes of one exchange per fetch.
#[derive(Clone, Debug)]
pub struct Recorder {
    tls: Arc<rustls::ClientConfig>,
    proxy: Option<socks::Proxy>,
    connect_timeout: Option<Duration>,
    io_timeout: Option<Duration>,
    max_response_length: Option<u64>,
}

impl Recorder {
    /// Create a recorder using `webpki-roots` and `aws-lc-rs`, with [`DEFAULT_TIMEOUT`] for each
    /// connection step and [`DEFAULT_MAX_RESPONSE_LENGTH`] for the response.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tls: tls::default_config(),
            proxy: None,
            connect_timeout: Some(DEFAULT_TIMEOUT),
            io_timeout: Some(DEFAULT_TIMEOUT),
            max_response_length: Some(DEFAULT_MAX_RESPONSE_LENGTH),
        }
    }

    /// Set a SOCKS5 proxy, or use direct connections with `None` (the default).
    ///
    /// `socks5://` resolves target hostnames locally; `socks5h://` sends them to the proxy.
    /// Both accept optional username and password credentials. Environment proxy settings are
    /// ignored. Invalid or unsupported URIs return [`ProxyError`].
    pub fn proxy(mut self, proxy: Option<&str>) -> Result<Self, ProxyError> {
        self.proxy = proxy.map(socks::Proxy::parse).transpose()?;
        Ok(self)
    }

    /// Replace the TLS client configuration, for example to trust a private certificate authority.
    ///
    /// The recorder offers only `http/1.1` in ALPN, whatever `config` offers.
    #[must_use]
    pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.tls = tls::http1(config);

        self
    }

    /// Set the connection timeout for each resolved address, or lift it with `None`.
    #[must_use]
    pub const fn connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.connect_timeout = timeout;

        self
    }

    /// Set the timeout for each connection read or write, or lift it with `None`.
    ///
    /// A read timeout after the head truncates the response instead of failing. Name
    /// resolution is not timed.
    #[must_use]
    pub const fn io_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.io_timeout = timeout;

        self
    }

    /// Set the maximum captured response length, including the head, or lift it with
    /// `None`.
    ///
    /// A response that ends exactly at the limit is complete. A head larger than the
    /// limit is an error because it cannot be partially captured.
    #[must_use]
    pub const fn max_response_length(mut self, length: Option<u64>) -> Self {
        self.max_response_length = length;

        self
    }

    /// Connect to the first resolved address that succeeds.
    fn connect(
        &self,
        host: &str,
        port: u16,
        deadline: Option<Instant>,
    ) -> Result<TcpStream, Error> {
        let mut failure = None;
        for address in (host, port).to_socket_addrs()? {
            let attempt = bound(self.connect_timeout, deadline)?.map_or_else(
                || TcpStream::connect(address),
                |timeout| TcpStream::connect_timeout(&address, timeout),
            );
            match attempt {
                Ok(stream) => return Ok(stream),
                Err(error) => failure = Some(error),
            }
        }

        Err(failure
            .unwrap_or_else(|| {
                std::io::Error::new(ErrorKind::NotFound, "the host resolved to no addresses")
            })
            .into())
    }
}

impl Client for Recorder {
    fn engine(&self) -> Engine {
        Engine::RECORDER
    }

    /// Perform one HTTP/1.1 exchange and record its exact bytes.
    ///
    /// The request is serialized from its parts. Missing `host` and `connection` headers are added.
    /// The caller's `transfer-encoding` and `content-length` headers are removed, and a provided
    /// body is framed with `content-length`. The response is recorded verbatim from its final
    /// status line through the message boundary.
    ///
    /// Connection and I/O timeouts are limited by the time remaining before `deadline`. DNS
    /// resolution is not timed, so the deadline is not a strict wall-clock limit.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] for an invalid target, a connection or TLS failure, an incomplete response
    /// head, or malformed response framing. A passed deadline is reported as a timed-out I/O
    /// operation. A size limit, disconnect, or timeout after the head instead returns a
    /// response with [`Exchange::truncated`] set.
    fn fetch_with_deadline(
        &self,
        request: Request<'_>,
        deadline: Option<Instant>,
    ) -> Result<Exchange, Error> {
        let Request {
            method,
            target,
            headers,
            body,
        } = request;
        let target_uri = request::validate(target, deadline)?;
        let tls = target.scheme_str() == Some("https");
        let sent_target = request::target(target);
        let authority = sent_target.authority().ok_or(Error::MissingHost)?;
        let raw_host = authority.host();
        // URIs bracket IPv6 hosts; DNS resolution and SNI do not.
        let host = raw_host
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .unwrap_or(raw_host);
        let port = sent_target.port_u16().unwrap_or(if tls { 443 } else { 80 });

        let prepared = request::headers(authority, headers);
        let request = reconstruct_request(method, &sent_target, Version::HTTP_11, &prepared, body);

        let started_at = Timestamp::now();
        let clock = Instant::now();

        let stream = if let Some(proxy) = &self.proxy {
            self.connect(&proxy.host, proxy.port, deadline)?
        } else {
            self.connect(host, port, deadline)?
        };
        let ip_address = if self.proxy.is_none() {
            Some(stream.peer_addr()?.ip())
        } else {
            None
        };
        let mut socket = Socket::new(stream, self.io_timeout, deadline)?;
        if let Some(proxy) = &self.proxy {
            proxy.tunnel(&mut socket, host, port)?;
        }

        let mut transport = if tls {
            let server_name = ServerName::try_from(host.to_owned())?;
            let connection = rustls::ClientConnection::new(Arc::clone(&self.tls), server_name)?;
            Stream::Tls(Box::new(rustls::StreamOwned::new(connection, socket)))
        } else {
            Stream::Plain(socket)
        };

        transport.write_all(&request)?;
        transport.flush()?;

        let (response, truncated) = read_response(
            &mut transport,
            *method == Method::HEAD,
            self.max_response_length,
        )?;
        let ResponseMetadata {
            status,
            body_offset: response_body_offset,
            ..
        } = ResponseMetadata::parse(&response).ok_or(ResponseError::MalformedStatusLine)?;
        let fetch_time = clock.elapsed();

        Ok(Exchange {
            request,
            response,
            fidelity: Fidelity::Exact,
            http_protocol: HttpProtocol::Http1,
            tls_version: transport.tls_version(),
            status,
            response_body_offset,
            target_uri,
            ip_address,
            started_at,
            fetch_time,
            truncated,
        })
    }
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

/// The tighter of a step's timeout and the time left to a deadline.
///
/// A deadline that has passed is a timed-out operation, since the socket refuses a zero timeout.
fn bound(
    timeout: Option<Duration>,
    deadline: Option<Instant>,
) -> std::io::Result<Option<Duration>> {
    let Some(deadline) = deadline else {
        return Ok(timeout);
    };
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(std::io::Error::new(
            ErrorKind::TimedOut,
            "the fetch deadline has passed",
        ));
    }

    Ok(Some(timeout.map_or(left, |timeout| timeout.min(left))))
}

enum Stream {
    Plain(Socket),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, Socket>>),
}

impl Stream {
    /// The TLS version the connection negotiated, when it uses TLS.
    fn tls_version(&self) -> Option<TlsVersion> {
        let Self::Tls(stream) = self else {
            return None;
        };

        match stream.conn.protocol_version()? {
            rustls::ProtocolVersion::TLSv1_2 => Some(TlsVersion::V1_2),
            rustls::ProtocolVersion::TLSv1_3 => Some(TlsVersion::V1_3),
            _ => None,
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

/// Enforce deadlines beneath TLS, which may perform several socket operations in one call.
struct Socket {
    stream: TcpStream,
    io_timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl Socket {
    fn new(
        stream: TcpStream,
        io_timeout: Option<Duration>,
        deadline: Option<Instant>,
    ) -> std::io::Result<Self> {
        stream.set_read_timeout(io_timeout)?;
        stream.set_write_timeout(io_timeout)?;
        Ok(Self {
            stream,
            io_timeout,
            deadline,
        })
    }

    fn arm(&self) -> std::io::Result<()> {
        if self.deadline.is_some() {
            let timeout = bound(self.io_timeout, self.deadline)?;
            self.stream.set_read_timeout(timeout)?;
            self.stream.set_write_timeout(timeout)?;
        }
        Ok(())
    }
}

impl Read for Socket {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.arm()?;
        self.stream.read(buffer)
    }
}

impl Write for Socket {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.arm()?;
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.arm()?;
        self.stream.flush()
    }
}
