#![cfg_attr(docsrs, feature(doc_cfg))]
//! HTTP clients that capture request and response messages.
//!
//! Choose a client according to the protocols and capture fidelity you need:
//!
//! - [`Recorder`](recorder::Recorder) performs HTTP/1.1 over its own connection and captures the
//!   exact bytes sent and received.
//! - [`ReqwestClient`](reqwest::ReqwestClient) performs HTTP/1.1 with `reqwest` and reconstructs
//!   both messages from the parts `reqwest` exposes.
//! - `WreqClient` (in the `wreq` module, behind the `wreq` feature) uses `BoringSSL` with browser
//!   emulation. It captures HTTP/1 bytes exactly and reconstructs HTTP/2 exchanges, which it
//!   negotiates only when asked to.
//!
//! [`Exchange::fidelity`] says whether a captured exchange is exact or reconstructed, and
//! [`Exchange::http_protocol`] says which HTTP version it used. Once a complete response head is
//! captured, a size limit, disconnect, or timeout retains the response received so far and records
//! why it ended.
//!
//! # Fetching
//!
//! Calls are synchronous, including when called from a Tokio runtime. Each fetch makes one request
//! on a new connection. Clients do not follow redirects, retry, keep cookies, decompress content,
//! or read proxy settings from the environment.
//!
//! ```no_run
//! use archivindex_http_client::recorder::Recorder;
//! use archivindex_http_client::{Client, Request};
//! use http::{HeaderMap, Method};
//!
//! let captured = Recorder::new().fetch(Request {
//!     method: &Method::GET,
//!     target: &"https://www.example.com/".parse()?,
//!     headers: &HeaderMap::new(),
//!     body: None,
//! })?;
//! println!("{}: {} body bytes", captured.status, captured.entity_body()?.len());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Use [`Client::fetch_with_deadline`] to set an absolute deadline. Use [`prepare`], [`redirect`],
//! [`retry`], and [`conditional`] when managing a request sequence yourself.
//!
//! # Limits and timeouts
//!
//! Connection and I/O timeouts default to [`DEFAULT_TIMEOUT`], and the response size limit to
//! [`DEFAULT_MAX_RESPONSE_LENGTH`]. Setters accept `None` to remove a bound. The response limit counts
//! captured bytes, including the final head and generated framing in reconstructed messages. Each
//! head also has a separate 64 KiB limit. Interim responses are discarded and do not consume the
//! final response limit. A response ending exactly at its limit is complete.
//!
//! Before a complete final head arrives, a timeout or disconnect returns an error. Afterward, a size
//! limit, timeout, or disconnect returns the captured prefix with [`Exchange::truncated`] set.
//! Malformed framing remains an error.
//!
//! | Client | Connection timeout | I/O timeout | Deadline |
//! | --- | --- | --- | --- |
//! | `Recorder` | Each address tried | Each socket read or write | Excludes DNS |
//! | `ReqwestClient` | DNS, TCP, and TLS | Response head, then each body frame | Wall clock |
//! | `WreqClient` | DNS, TCP, and TLS | Idle progress after connecting | Wall clock |
//!
//! Reqwest's first I/O wait includes connecting and sending the request. Wreq's HTTP/2 control
//! traffic cannot keep a stalled exchange alive. A system DNS lookup may finish in the background
//! after a reqwest or wreq fetch times out.
//!
//! # TLS and proxies
//!
//! The recorder and reqwest use `webpki-roots` and `aws-lc-rs`, independently of the operating system
//! trust store and process-wide provider. Their `tls_config` setters accept replacement trust
//! settings; both restrict ALPN to `http/1.1`. Wreq accepts a replacement certificate store through
//! `tls_cert_store`. Default configurations verify certificates and hostnames.
//!
//! All clients accept `socks5://` for local DNS and `socks5h://` for proxy DNS. The default port is
//! 1080. Username and password credentials are percent-encoded, for example
//! `socks5h://user:p%40ssword@host:1080`. Other proxy schemes are rejected. Proxy failures never fall
//! back to direct connections, and SOCKS negotiation is excluded from the captured messages.
//!
//! ```no_run
//! use archivindex_http_client::recorder::Recorder;
//!
//! let client = Recorder::new().proxy(Some("socks5h://127.0.0.1:1080"))?;
//! # Ok::<(), archivindex_http_client::InvalidProxy>(())
//! ```

mod chunked;
pub mod conditional;
mod failure;
pub mod framing;
pub mod prepare;
mod read;
pub mod reconstruct;
pub mod recorder;
pub mod redirect;
mod request;
pub mod reqwest;
pub mod retry;
mod runtime;
mod socks;
mod tls;
#[cfg(feature = "wreq")]
#[cfg_attr(docsrs, doc(cfg(feature = "wreq")))]
pub mod wreq;

use std::borrow::Cow;
use std::fmt::Debug;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use archivindex_http::body;
use archivindex_http::message::{RequestMetadata, ResponseMetadata};
use fluent_uri::Uri;
use http::{HeaderMap, Method, Uri as HttpUri};
use jiff::Timestamp;

use crate::framing::Truncation;

/// The default connection and I/O timeout: 30 seconds.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// The default response limit: 256 MiB, including the head and transfer framing.
pub const DEFAULT_MAX_RESPONSE_LENGTH: u64 = 256 * 1024 * 1024;

/// Errors returned by a client while performing a captured exchange.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The target is not an absolute HTTP or HTTPS URI.
    #[error("the target URI must be absolute, with an `http` or `https` scheme")]
    UnsupportedScheme,
    /// The target names no host.
    #[error("the target URI names no host")]
    MissingHost,

    /// A request URL could not be parsed.
    #[error(transparent)]
    InvalidUrl(#[from] url::ParseError),
    /// A normalized request target could not be represented as an HTTP URI.
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    /// The target is not a URI as RFC 3986 defines one.
    #[error("the target is not a valid URI")]
    TargetUri(#[from] fluent_uri::ParseError),
    /// A value could not be represented as an HTTP header value.
    #[error(transparent)]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

    /// A request URL could not be prepared.
    #[error(transparent)]
    Preparation(#[from] crate::prepare::Error),
    /// Response framing is malformed.
    #[error(transparent)]
    Response(#[from] crate::framing::ResponseError),
    /// An HTTP message could not be reconstructed.
    #[error(transparent)]
    Reconstruction(#[from] crate::reconstruct::Error),

    /// The host cannot name a TLS server.
    #[error("the host cannot name a TLS server")]
    ServerName(#[from] rustls::pki_types::InvalidDnsNameError),
    /// The TLS session could not be created.
    #[error(transparent)]
    Tls(#[from] rustls::Error),

    /// A reqwest operation failed.
    #[error(transparent)]
    Reqwest(#[from] ::reqwest::Error),
    /// A wreq operation failed.
    #[cfg(feature = "wreq")]
    #[cfg_attr(docsrs, doc(cfg(feature = "wreq")))]
    #[error(transparent)]
    Wreq(#[from] ::wreq::Error),

    /// An I/O operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A client failed in a way that has no variant of its own.
    ///
    /// Available for external [`Client`] implementations. The built-in clients use typed variants.
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Why a proxy URI cannot be used.
///
/// Every client accepts the same proxies: `socks5://` and `socks5h://` URIs with a host, an
/// optional port, and optional username and password credentials.
#[derive(Debug, thiserror::Error)]
pub enum InvalidProxy {
    /// The scheme is neither `socks5` nor `socks5h`.
    #[error("invalid proxy: expected socks5:// or socks5h://")]
    UnsupportedScheme,
    /// The URI has no host.
    #[error("invalid proxy: missing host")]
    MissingHost,
    /// The port is zero.
    #[error("invalid proxy: port must be nonzero")]
    ZeroPort,
    /// The URI has a path other than an empty path or `/`.
    #[error("invalid proxy: paths other than / are not supported")]
    UnsupportedPath,
    /// The URI has a query.
    #[error("invalid proxy: queries are not supported")]
    UnsupportedQuery,
    /// The URI has a fragment.
    #[error("invalid proxy: fragments are not supported")]
    UnsupportedFragment,

    /// Userinfo has no colon separating the username and password.
    #[error("invalid proxy: authentication requires a username and password")]
    MissingPassword,
    /// The decoded username is empty or exceeds 255 bytes.
    #[error("invalid proxy: username must contain 1 to 255 bytes")]
    InvalidUsernameLength,
    /// The decoded password is empty or exceeds 255 bytes.
    #[error("invalid proxy: password must contain 1 to 255 bytes")]
    InvalidPasswordLength,

    /// The proxy URL cannot be parsed.
    #[error("invalid proxy URL")]
    InvalidUrl(#[from] url::ParseError),
    /// The parsed URL is not a valid RFC 3986 URI.
    #[error("invalid proxy URI")]
    InvalidUri(#[from] fluent_uri::ParseError),

    /// Reqwest rejected the proxy configuration.
    #[error("invalid proxy: rejected by reqwest")]
    Reqwest(#[from] ::reqwest::Error),
    /// Wreq rejected the proxy configuration.
    #[cfg(feature = "wreq")]
    #[cfg_attr(docsrs, doc(cfg(feature = "wreq")))]
    #[error("invalid proxy: rejected by wreq")]
    Wreq(#[from] ::wreq::Error),
}

/// Whether the captured messages of an exchange are the bytes that crossed the connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Fidelity {
    /// The captured messages are the exact HTTP/1 bytes sent and received.
    Exact,
    /// Both messages were rebuilt from parsed parts.
    ///
    /// Header names are lowercased, and chunk boundaries follow the body data delivered by the
    /// HTTP library. HTTP/1 reason phrases are preserved; HTTP/2 uses canonical reason phrases.
    Reconstructed,
}

/// The HTTP version used on the connection, independent of the captured messages' format.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HttpProtocol {
    /// HTTP/1.0 or HTTP/1.1.
    Http1,
    /// HTTP/2.
    Http2,
}

/// A negotiated TLS protocol version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TlsVersion {
    /// TLS 1.0.
    V1_0,
    /// TLS 1.1.
    V1_1,
    /// TLS 1.2.
    V1_2,
    /// TLS 1.3.
    V1_3,
}

/// One captured exchange and its transport metadata.
///
/// Messages use HTTP/1 syntax even when the connection used HTTP/2. [`fidelity`](Self::fidelity)
/// distinguishes exact from reconstructed bytes; [`http_protocol`](Self::http_protocol) records
/// the connection's protocol. Interim `1xx` responses are discarded.
///
/// Use [`request_metadata`](Self::request_metadata) and [`response_metadata`](Self::response_metadata)
/// to inspect headers, [`message_body`](Self::message_body) for the response bytes after the head,
/// or [`entity_body`](Self::entity_body) to remove transfer framing while preserving content coding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Exchange {
    /// The requested URI.
    pub target_uri: Uri<String>,
    /// When network activity began.
    pub started_at: Timestamp,
    /// Time from starting network activity to finishing the response.
    pub fetch_time: Duration,
    /// Whether the captured messages are the bytes that crossed the connection.
    pub fidelity: Fidelity,
    /// The HTTP version the exchange used. The captured messages are HTTP/1 messages either way.
    pub http_protocol: HttpProtocol,
    /// The negotiated TLS version, when available. Absent for plaintext exchanges and reqwest's
    /// SOCKS connections.
    pub tls_version: Option<TlsVersion>,
    /// The origin IP address, when known. Proxied captures omit it because the socket peer is
    /// the proxy and the tunnel does not reliably identify the origin address.
    pub ip_address: Option<IpAddr>,
    /// Status from the captured response's first line.
    pub status: u16,
    /// Captured request message, with accuracy described by `fidelity`.
    pub request: Vec<u8>,
    /// Captured response message, from the final status line through the recorded end.
    pub response: Vec<u8>,
    /// Offset in `response` at which the message body begins.
    pub response_body_offset: usize,
    /// Why the response was truncated, if applicable.
    pub truncated: Option<Truncation>,
}

impl Exchange {
    /// Parse the head of the captured request, borrowing from [`request`](Self::request).
    ///
    /// Returns `None` if `request` does not begin with a well-formed request head.
    #[must_use]
    pub fn request_metadata(&self) -> Option<RequestMetadata<'_>> {
        RequestMetadata::parse(&self.request)
    }

    /// Parse the head of the captured response, borrowing from [`response`](Self::response).
    ///
    /// Returns `None` if `response` does not begin with a well-formed response head. An exchange
    /// returned by a [`Client`] always does.
    #[must_use]
    pub fn response_metadata(&self) -> Option<ResponseMetadata<'_>> {
        ResponseMetadata::parse(&self.response)
    }

    /// Return the response entity-body with transfer coding removed and content coding preserved.
    ///
    /// Responses to `HEAD`, and `204` and `304` responses, have an empty entity-body regardless
    /// of their framing headers. Incomplete chunk framing is an error; use
    /// [`message_body`](Self::message_body) to inspect a truncated chunked response.
    pub fn entity_body(&self) -> Result<Cow<'_, [u8]>, body::Error> {
        if self.request.starts_with(b"HEAD ") || matches!(self.status, 204 | 304) {
            Ok(Cow::Borrowed(&[]))
        } else {
            body::entity_body(&self.response)
        }
    }

    /// Return the response message-body unchanged, including any chunk framing and trailers.
    ///
    /// This returns all bytes after the head in [`response`](Self::response), without enforcing
    /// `Content-Length` or checking whether a body is permitted. See [`fidelity`](Self::fidelity)
    /// for whether they match the bytes received on the connection.
    #[must_use]
    pub fn message_body(&self) -> &[u8] {
        &self.response[self.response_body_offset..]
    }
}

/// Borrowed inputs for one HTTP exchange.
///
/// Targets must be absolute HTTP or HTTPS URIs and are validated before network activity. Userinfo
/// is removed from the transport target; supply origin credentials in an explicit `authorization`
/// header. [`Exchange::target_uri`] retains the URI supplied here.
///
/// Clients add a missing `host` header and default HTTP/1.1 requests to `connection: close`.
/// Caller-supplied `content-length` and `transfer-encoding` fields are removed; a supplied body is
/// framed with its actual length. `None` supplies no body; `Some(&[])` supplies an empty body.
#[derive(Clone, Copy, Debug)]
pub struct Request<'a> {
    /// The request method.
    pub method: &'a Method,
    /// The absolute `http` or `https` URI to request.
    pub target: &'a HttpUri,
    /// The request headers.
    pub headers: &'a HeaderMap,
    /// The request body. `Some(&[])` explicitly supplies an empty body.
    ///
    /// Clients replace caller-supplied framing headers with framing for this body.
    pub body: Option<&'a [u8]>,
}

/// The HTTP implementation that performs a client's exchanges.
///
/// Displays in the `User-Agent` product syntax of [RFC 9110], for example
/// `wreq/0.16.1 (chrome_136)`. The version and profile are optional.
///
/// [RFC 9110]: https://www.rfc-editor.org/rfc/rfc9110#section-10.1.5
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Engine {
    /// The implementation's name.
    pub name: &'static str,
    /// The implementation's exact version or release series, when known.
    ///
    /// A series such as `0.13.x` leaves the patch version unspecified.
    pub version: Option<&'static str>,
    /// The browser emulation profile, for an implementation that has one.
    pub profile: Option<&'static str>,
}

impl Engine {
    /// The engine of the [`Recorder`](recorder::Recorder), which is part of this crate.
    pub const RECORDER: Self = Self {
        name: "recorder",
        version: None,
        profile: None,
    };

    /// The engine of the [`ReqwestClient`](reqwest::ReqwestClient).
    ///
    /// Reports the release series because consuming applications may resolve different compatible
    /// patch versions.
    pub const REQWEST: Self = Self {
        name: "reqwest",
        version: Some("0.13.x"),
        profile: None,
    };
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)?;
        if let Some(version) = self.version {
            write!(f, "/{version}")?;
        }
        if let Some(profile) = self.profile {
            write!(f, " ({profile})")?;
        }

        Ok(())
    }
}

/// Performs and captures one HTTP exchange.
///
/// Clients can be shared across threads.
pub trait Client: Debug + Send + Sync + 'static {
    /// The HTTP implementation that performs this client's exchanges.
    fn engine(&self) -> Engine;

    /// Perform one exchange, finishing before `deadline` when one is given.
    ///
    /// Passing `None` is equivalent to [`fetch`](Self::fetch). Configured connection and I/O
    /// timeouts apply in either case.
    ///
    /// A deadline that passes before a response head arrives is an error; one that
    /// passes afterwards truncates the response with [`Truncation::Time`]. Report failures that
    /// have no [`Error`] variant of their own as [`Error::Other`].
    ///
    /// # Errors
    ///
    /// Fails when the target is unusable, the transport fails before a usable response
    /// head, or the response cannot be framed.
    fn fetch_with_deadline(
        &self,
        request: Request<'_>,
        deadline: Option<Instant>,
    ) -> Result<Exchange, Error>;

    /// Perform one exchange without a deadline.
    ///
    /// # Errors
    ///
    /// As for [`fetch_with_deadline`](Self::fetch_with_deadline).
    fn fetch(&self, request: Request<'_>) -> Result<Exchange, Error> {
        self.fetch_with_deadline(request, None)
    }
}

impl<C: Client + ?Sized> Client for std::sync::Arc<C> {
    fn engine(&self) -> Engine {
        (**self).engine()
    }

    fn fetch_with_deadline(
        &self,
        request: Request<'_>,
        deadline: Option<Instant>,
    ) -> Result<Exchange, Error> {
        (**self).fetch_with_deadline(request, deadline)
    }
}
