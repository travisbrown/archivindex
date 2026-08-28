//! URL and header preparation for HTTP request sequences.

use std::borrow::Cow;
use std::fmt::Write as _;

use http::HeaderMap;
use url::{Position, Url};

/// A URL cannot be used as a credential-free HTTP target URI.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Credentials must be supplied explicitly through headers.
    #[error("URL contains credentials: {0}")]
    CredentialedUrl(String),
    /// The URL has no host.
    #[error("URL has no host: {0}")]
    MissingHost(String),

    /// The URL cannot be parsed.
    #[error(transparent)]
    InvalidUrl(#[from] url::ParseError),
    /// The normalized target is not an HTTP URI.
    #[error("URL is not a valid URI: {url}")]
    InvalidUri {
        /// The URL without credentials.
        url: String,
        /// The URI parsing failure.
        #[source]
        source: http::uri::InvalidUri,
    },
}

/// Validate and normalize a URL without embedding origin credentials in the request.
pub fn target(url: &Url) -> Result<http::Uri, Error> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::CredentialedUrl(redact_credentials(url)));
    }
    if url.host_str().is_none() {
        return Err(Error::MissingHost(url.to_string()));
    }
    target_uri_text(url)
        .parse()
        .map_err(|source| Error::InvalidUri {
            url: url.to_string(),
            source,
        })
}

/// Add a request's fields to defaults, replacing defaults with the same name.
#[must_use]
pub fn merged_headers(defaults: &HeaderMap, supplied: &HeaderMap) -> HeaderMap {
    let mut merged = defaults.clone();
    for name in supplied.keys() {
        merged.remove(name);
        for value in supplied.get_all(name) {
            merged.append(name.clone(), value.clone());
        }
    }
    merged
}

/// Serialize a URL as RFC 3986 URI text without a fragment.
///
/// Percent-encode the characters the WHATWG serializer leaves bare but RFC 3986 forbids: `|`, `^`,
/// `[`, `]`, `{`, `}`, and `` ` ``.
///
/// Userinfo is preserved here; [`target`] rejects it. Client preparation instead removes userinfo
/// from the transport target and accepts credentials only through explicit headers.
#[must_use]
pub fn target_uri_text(url: &Url) -> Cow<'_, str> {
    let text = &url[..Position::AfterQuery];
    let path_start = url[..Position::BeforePath].len();
    let needs_encoding =
        |character: char| matches!(character, '|' | '^' | '[' | ']' | '{' | '}' | '`');

    if !text[path_start..].contains(needs_encoding) {
        return Cow::Borrowed(text);
    }

    let mut encoded = String::with_capacity(text.len() + 8);
    encoded.push_str(&text[..path_start]);

    for character in text[path_start..].chars() {
        if needs_encoding(character) {
            // Writing to a `String` cannot fail.
            let _ = write!(encoded, "%{:02X}", u32::from(character));
        } else {
            encoded.push(character);
        }
    }

    Cow::Owned(encoded)
}

/// Render a URL with credentials removed for error messages and capture metadata.
#[must_use]
pub fn redact_credentials(url: &Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.to_string()
}
