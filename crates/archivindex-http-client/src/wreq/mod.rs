//! HTTP/1 wire capture and reconstructed HTTP/2 capture with browser TLS emulation.
//!
//! Each fetch makes one request on a new connection. Redirects, retries, cookies, automatic
//! proxies, decompression, and pooling are disabled. The selected profile supplies TLS and HTTP/2
//! settings; configured request headers override profile headers.
//!
//! HTTP/1 messages are captured exactly, with [`Fidelity::Exact`](crate::Fidelity::Exact). HTTP/2
//! is used only by a client that enables it with [`WreqClient::http2`]. An HTTP/2 exchange is
//! reconstructed as HTTP/1.1 messages, with
//! [`Fidelity::Reconstructed`](crate::Fidelity::Reconstructed), and
//! [`HttpProtocol::Http2`](crate::HttpProtocol::Http2) identifies its original protocol. Content
//! coding is preserved and chunked framing retains response trailers. Both HTTP versions also
//! record the negotiated TLS version when available.
//!
//! Calls are synchronous and can run inside an existing Tokio runtime. See the
//! [crate documentation](crate) for shared limits, timeouts, and proxy configuration.
//!
//! # Configuration
//!
//! Enable the `wreq` feature and apply the patches in the [build guide]. Building BoringSSL also
//! requires C and C++ compilers, CMake, and libclang.
//!
//! ```no_run
//! use archivindex_http_client::wreq::{WreqClient, parse_profile};
//!
//! let client = WreqClient::new(parse_profile("chrome_136")?);
//! let http2_client = client.clone().http2(true);
//! let firefox = client.profile(parse_profile("firefox_136")?);
//! # Ok::<(), archivindex_http_client::wreq::UnknownProfile>(())
//! ```
//!
//! Profiles supply TLS settings, default headers, header ordering and casing, and HTTP/2 settings.
//! Explicit request headers override profile values. [`parse_profile`] rejects unknown names; you
//! can also pass a `wreq_util::Profile` from the pinned dependency directly.
//!
//! # HTTP/2 capture
//!
//! Enable [`WreqClient::http2`] to offer the profile's protocols. The server may still choose
//! HTTP/1.1. HTTP/2 pseudo-headers become the request line, `host`, or response status. Responses
//! with bodies use generated chunk framing to keep trailers separate. `HEAD`, `204`, and `304`
//! keep their bodyless form and representation headers. Captures omit binary frames, HPACK state,
//! and original framing declarations.
//!
//! [build guide]: https://github.com/travisbrown/archivindex/blob/main/crates/archivindex-http-client/BUILDING.md

mod capture;
mod variant_name;

use std::time::{Duration, Instant};

use serde::de::Deserialize;
use serde::de::value::StrDeserializer;
use serde::ser::Serialize;
use wreq_util::Profile;

use crate::{
    Client, DEFAULT_MAX_RESPONSE_LENGTH, DEFAULT_TIMEOUT, Engine, Error, Exchange, InvalidProxy,
    Request, failure, runtime, socks,
};

/// An isolated HTTP/1 and HTTP/2 client using `BoringSSL` and browser emulation.
#[derive(Clone)]
pub struct WreqClient {
    profile: Profile,
    http2: bool,
    proxy: Option<wreq::Proxy>,
    connect_timeout: Option<Duration>,
    io_timeout: Option<Duration>,
    max_response_length: Option<u64>,
    cert_store: Option<wreq::tls::trust::CertStore>,
}

impl std::fmt::Debug for WreqClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WreqClient")
            .field("profile", &self.profile)
            .field("http2", &self.http2)
            .field("proxied", &self.proxy.is_some())
            .field("connect_timeout", &self.connect_timeout)
            .field("io_timeout", &self.io_timeout)
            .field("max_response_length", &self.max_response_length)
            .field("custom_cert_store", &self.cert_store.is_some())
            .finish()
    }
}

impl WreqClient {
    /// Select a profile, including its TLS and HTTP/2 settings.
    ///
    /// The client starts with HTTP/2 disabled, [`DEFAULT_TIMEOUT`] for connecting and for idle
    /// progress, and [`DEFAULT_MAX_RESPONSE_LENGTH`] for the response.
    #[must_use]
    pub const fn new(profile: Profile) -> Self {
        Self {
            profile,
            http2: false,
            proxy: None,
            connect_timeout: Some(DEFAULT_TIMEOUT),
            io_timeout: Some(DEFAULT_TIMEOUT),
            max_response_length: Some(DEFAULT_MAX_RESPONSE_LENGTH),
            cert_store: None,
        }
    }

    /// Replace the profile used for every request.
    #[must_use]
    pub const fn profile(mut self, profile: Profile) -> Self {
        self.profile = profile;
        self
    }

    /// Allow HTTP/2, which is off unless this is called with `true`.
    ///
    /// When enabled, the client offers the profile's ALPN protocols and uses HTTP/2 when the
    /// origin selects it. When disabled, it offers only `http/1.1`. A browser profile normally
    /// offers `h2` as well, so the `ClientHello` of a client without HTTP/2 differs from the
    /// emulated browser's in its ALPN extension.
    #[must_use]
    pub const fn http2(mut self, enabled: bool) -> Self {
        self.http2 = enabled;
        self
    }

    /// Set an explicit proxy for every request, or use direct connections with `None`.
    ///
    /// Supports `socks5://` for local DNS and `socks5h://` for proxy DNS, with optional username
    /// and password credentials. Environment proxy settings remain disabled. This client accepts
    /// exactly the proxies the [`Recorder`](crate::recorder::Recorder) accepts.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidProxy`] for a malformed or unsupported URI.
    pub fn proxy(mut self, proxy: Option<&str>) -> Result<Self, InvalidProxy> {
        self.proxy = proxy
            .map(|proxy| {
                socks::Proxy::parse(proxy)?;
                wreq::Proxy::all(proxy).map_err(InvalidProxy::Wreq)
            })
            .transpose()?;
        Ok(self)
    }

    /// Bound connecting, including DNS and TLS, or remove the bound with `None`.
    #[must_use]
    pub const fn connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Bound idle exchange progress after connecting, or remove the bound with `None`.
    ///
    /// HTTP/2 connection control traffic does not count as response progress.
    #[must_use]
    pub const fn io_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.io_timeout = timeout;
        self
    }

    /// Bound captured response bytes, including the final head and transfer framing.
    ///
    /// HTTP/2 counts the reconstructed message, not binary connection traffic.
    #[must_use]
    pub const fn max_response_length(mut self, limit: Option<u64>) -> Self {
        self.max_response_length = limit;
        self
    }

    /// Replace the default Mozilla root certificate store.
    #[must_use]
    pub fn tls_cert_store(mut self, store: wreq::tls::trust::CertStore) -> Self {
        self.cert_store = Some(store);
        self
    }
}

/// A profile name that no known browser/client profile matches.
#[derive(Clone, Debug, thiserror::Error)]
#[error("unknown profile `{0}`")]
pub struct UnknownProfile(pub String);

/// Look up a profile by the name used in configuration, such as `chrome_136`.
///
/// # Errors
///
/// Fails when no profile has that name.
pub fn parse_profile(name: &str) -> Result<Profile, UnknownProfile> {
    Profile::deserialize(StrDeserializer::<serde::de::value::Error>::new(name))
        .map_err(|_| UnknownProfile(name.to_owned()))
}

impl Engine {
    /// The engine of a [`WreqClient`] emulating `profile`.
    ///
    /// The profile is reported under the name that [`parse_profile`] accepts.
    #[must_use]
    pub fn wreq_with_profile(profile: Profile) -> Self {
        Self {
            name: "wreq",
            version: Some("0.16.1"),
            profile: profile.serialize(variant_name::VariantName).ok(),
        }
    }
}

/// Preserve the underlying I/O error kind in a wreq failure.
fn transport_error(error: wreq::Error) -> Error {
    let timed_out = error.is_timeout();

    failure::error(error, timed_out)
}

impl Client for WreqClient {
    fn engine(&self) -> Engine {
        Engine::wreq_with_profile(self.profile)
    }

    /// Perform and retain exactly one application exchange.
    ///
    /// A deadline covers DNS, connecting, and response capture.
    fn fetch_with_deadline(
        &self,
        request: Request<'_>,
        deadline: Option<Instant>,
    ) -> Result<Exchange, Error> {
        runtime::fetch(request.target, deadline, |target_uri| {
            self.capture(request, target_uri, deadline)
        })
    }
}

#[cfg(test)]
mod tests {
    use wreq_util::Profile;

    use super::parse_profile;
    use crate::Engine;

    /// Every profile must report a name that `parse_profile` accepts as the same profile.
    #[test]
    fn every_profile_is_named_as_it_is_parsed() {
        for &profile in Profile::VARIANTS {
            let name = Engine::wreq_with_profile(profile).profile.unwrap();
            assert_eq!(parse_profile(name).unwrap(), profile);
        }
        assert_eq!(
            Engine::wreq_with_profile(Profile::Chrome136).profile,
            Some("chrome_136")
        );
    }
}
