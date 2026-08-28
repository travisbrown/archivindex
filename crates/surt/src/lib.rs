//! Sort-friendly URI Reordering Transform (SURT) keys and URL canonicalization operations.
//!
//! A SURT reorders a URL's host so that keys for related pages sort together: `www.example.com`
//! becomes `com,example,www`, and everything under `example.com` shares the prefix `com,example`.
//!
//! [`Surt::from_url`] applies the default canonicalizer ([`url::Canonicalizer::WAYBACK`]):
//!
//! ```
//! use archivindex_surt::Surt;
//!
//! let key = Surt::from_url("https://www.Example.com:443/Movies/?b=2&a=1#top")?;
//!
//! assert_eq!(key.as_str(), "com,example)/movies?a=1&b=2");
//! assert_eq!(key.labels().collect::<Vec<_>>(), ["com", "example"]);
//! assert_eq!(key.url().to_string(), "https://example.com/movies?a=1&b=2");
//! # Ok::<_, archivindex_surt::url::Error>(())
//! ```
//!
//! Conversions are available for `fluent_uri::Uri` under the default `fluent-uri` feature, and for
//! `url::Url` under the `url` feature.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod canonicalize;
mod escape;
#[cfg(test)]
mod prop;
mod session;
pub mod url;

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

/// Text is not a SURT key.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("not a SURT: `{key}` {kind}")]
pub struct Error {
    /// The offending text.
    pub key: String,
    /// Why it was rejected.
    pub kind: ErrorKind,
}

/// The reason text was not a SURT key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ErrorKind {
    /// The key has no `)` ending its host.
    #[error("has no `)`")]
    MissingHostTerminator,
    /// The host has no labels, or an IPv6 literal is unclosed or followed by something other than a
    /// port.
    #[error("has a malformed host")]
    MalformedHost,
    /// The port is not a number between 0 and 65535.
    #[error("has an invalid port")]
    InvalidPort,
    /// The key contains ASCII whitespace or a control character.
    #[error("contains whitespace or a control character")]
    InvalidCharacter,
}

/// Byte offsets of a key's components within its text.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Shape {
    /// Index just past the comma-separated labels.
    labels_end: usize,
    /// Index of the `)` that ends the host.
    host_end: usize,
    /// Index of the `?` that begins the query.
    query_start: Option<usize>,
}

/// A SURT key: `com,example[:port])[path][?query]`.
///
/// A domain name's labels are reversed, separated by commas, and followed by `)`.
/// IPv6 addresses are kept whole. Brackets distinguish an address from a following port:
/// `[2001:db8::1]:8080)/path` has host `2001:db8::1` and port `8080`. Wayback-style keys omit
/// the brackets, so `2001:db8::1:8080)/path` is interpreted as the address `2001:db8::1:8080`
/// with no separate port.
///
/// Parsing borrows the text; [`Surt::into_owned`] detaches it.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Surt<'a> {
    key: Cow<'a, str>,
    shape: Shape,
}

impl<'a> Surt<'a> {
    /// Parse a key, borrowing the text.
    ///
    /// # Errors
    ///
    /// Fails when the text does not have the shape of a key: a comma-separated host, an optional
    /// numeric port, and `)`, with no ASCII whitespace or control characters.
    pub fn parse(key: &'a str) -> Result<Self, Error> {
        Ok(Self {
            key: Cow::Borrowed(key),
            shape: shape(key).map_err(|kind| Error {
                key: key.to_owned(),
                kind,
            })?,
        })
    }

    /// Canonicalize a URL with the Wayback Machine's rules and transform it into a key.
    ///
    /// # Errors
    ///
    /// Fails when the URL cannot be split into components; see [`url::Error`].
    pub fn from_url(url: &str) -> Result<Surt<'static>, url::Error> {
        url::Canonicalizer::WAYBACK.surt(url)
    }

    /// Wrap a key rendered from a [`url::Url`], which always has a key's shape.
    pub(crate) fn from_canonical_key(key: String) -> Surt<'static> {
        let shape = shape(&key).expect("keys rendered from URLs are well-formed");

        Surt {
            key: Cow::Owned(key),
            shape,
        }
    }

    /// The key text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.key
    }

    /// The comma-separated labels, in reverse DNS order: `com,example`.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.key[..self.shape.labels_end]
    }

    /// The host's labels, in reverse DNS order; `.rev()` yields them in DNS order.
    #[must_use]
    pub fn labels(&self) -> Labels<'_> {
        Labels(self.host().split(','))
    }

    /// The port, if the key has one.
    #[must_use]
    pub fn port(&self) -> Option<u16> {
        // The shape check has already confirmed the digits parse.
        (self.shape.labels_end < self.shape.host_end)
            .then(|| {
                self.key[self.shape.labels_end + 1..self.shape.host_end]
                    .parse()
                    .ok()
            })
            .flatten()
    }

    /// The path, which is either empty or starts with `/`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.key[self.shape.host_end + 1..self.shape.query_start.unwrap_or(self.key.len())]
    }

    /// The query, without the leading `?`, if the key has one.
    #[must_use]
    pub fn query(&self) -> Option<&str> {
        self.shape.query_start.map(|start| &self.key[start + 1..])
    }

    /// The URL the key stands for, using HTTPS.
    ///
    /// See [`Self::url_with_scheme`] if you need to specify a different scheme.
    #[must_use]
    pub fn url(&self) -> impl fmt::Display + '_ {
        self.url_with_scheme("https")
    }

    /// The URL the key stands for, given a scheme: `scheme://example.com[:port]/path[?query]`.
    ///
    /// The key does not record its scheme, user information, or fragment, so the result is the
    /// canonical URL rather than the original one.
    #[must_use]
    pub fn url_with_scheme<'s>(&'s self, scheme: &'s str) -> impl fmt::Display + 's {
        Url { key: self, scheme }
    }

    /// The URL the key stands for as a [`fluent_uri::Uri`], given a scheme.
    ///
    /// Parses the text rendered by [`url_with_scheme`](Self::url_with_scheme) as a [`fluent_uri::Uri`].
    ///
    /// # Errors
    ///
    /// Fails when the supplied scheme or rendered URL is not valid URI syntax. Escape normalization
    /// re-encodes only controls, space, `#`, `%`, and non-ASCII bytes, so a path can keep
    /// characters like `{` or `|` that RFC 3986 does not allow.
    ///
    /// # Examples
    ///
    /// ```
    /// use archivindex_surt::Surt;
    ///
    /// let key = Surt::parse("com,example)/movies?a=1")?;
    ///
    /// assert_eq!(key.to_uri("https")?.as_str(), "https://example.com/movies?a=1");
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    #[cfg(feature = "fluent-uri")]
    #[cfg_attr(docsrs, doc(cfg(feature = "fluent-uri")))]
    pub fn to_uri(&self, scheme: &str) -> Result<fluent_uri::Uri<String>, fluent_uri::ParseError> {
        // The owned parse returns the input alongside the error, which the caller still holds.
        fluent_uri::Uri::parse(self.url_with_scheme(scheme).to_string()).map_err(|(error, _)| error)
    }

    /// The URL the key stands for as a [`url::Url`](::url::Url), given a scheme.
    ///
    /// Parses the text rendered by [`url_with_scheme`](Self::url_with_scheme). The WHATWG parser
    /// may normalize or percent-encode that text, and its accepted input differs from
    /// [`to_uri`](Self::to_uri).
    ///
    /// # Errors
    ///
    /// Fails when the rendered URL, including the supplied scheme, is rejected by the WHATWG
    /// parser.
    #[cfg(feature = "url")]
    #[cfg_attr(docsrs, doc(cfg(feature = "url")))]
    pub fn to_url(&self, scheme: &str) -> Result<::url::Url, ::url::ParseError> {
        ::url::Url::parse(&self.url_with_scheme(scheme).to_string())
    }

    /// Detach the key from the text it was parsed from.
    #[must_use]
    pub fn into_owned(self) -> Surt<'static> {
        Surt {
            key: Cow::Owned(self.key.into_owned()),
            shape: self.shape,
        }
    }
}

/// The URL a key stands for, rendered on demand by [`Surt::url_with_scheme`].
struct Url<'s, 'a> {
    key: &'s Surt<'a>,
    scheme: &'s str,
}

impl fmt::Display for Url<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://", self.scheme)?;

        // The Wayback Machine writes IPv6 addresses bare, but a URL needs them bracketed.
        let host = self.key.host();
        let bare_ipv6 = host.contains(':') && !host.starts_with('[');

        if bare_ipv6 {
            f.write_str("[")?;
        }

        for (index, label) in self.key.labels().rev().enumerate() {
            if index > 0 {
                f.write_str(".")?;
            }

            f.write_str(label)?;
        }

        if bare_ipv6 {
            f.write_str("]")?;
        }

        if let Some(port) = self.key.port() {
            write!(f, ":{port}")?;
        }

        let path = self.key.path();
        f.write_str(if path.is_empty() { "/" } else { path })?;

        if let Some(query) = self.key.query() {
            write!(f, "?{query}")?;
        }

        Ok(())
    }
}

impl fmt::Display for Surt<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key)
    }
}

impl FromStr for Surt<'static> {
    type Err = Error;

    fn from_str(key: &str) -> Result<Self, Self::Err> {
        Surt::parse(key).map(Surt::into_owned)
    }
}

impl<'a> From<Surt<'a>> for Cow<'a, str> {
    fn from(surt: Surt<'a>) -> Self {
        surt.key
    }
}

impl AsRef<str> for Surt<'_> {
    fn as_ref(&self) -> &str {
        &self.key
    }
}

/// Use the Wayback Machine's rules, as [`Surt::from_url`] does.
///
/// To apply another convention, pass [`Uri::as_str`](fluent_uri::Uri::as_str) to
/// [`url::Canonicalizer::surt`].
#[cfg(feature = "fluent-uri")]
#[cfg_attr(docsrs, doc(cfg(feature = "fluent-uri")))]
impl TryFrom<&fluent_uri::Uri<String>> for Surt<'static> {
    type Error = url::Error;

    fn try_from(uri: &fluent_uri::Uri<String>) -> Result<Self, Self::Error> {
        Self::from_url(uri.as_str())
    }
}

/// See the implementation for [`fluent_uri::Uri<String>`].
#[cfg(feature = "fluent-uri")]
#[cfg_attr(docsrs, doc(cfg(feature = "fluent-uri")))]
impl TryFrom<fluent_uri::Uri<&str>> for Surt<'static> {
    type Error = url::Error;

    fn try_from(uri: fluent_uri::Uri<&str>) -> Result<Self, Self::Error> {
        Self::from_url(uri.as_str())
    }
}

/// Canonicalizes with the Wayback Machine's rules, as [`Surt::from_url`] does. The URL has already
/// been through the WHATWG parser, which normalizes some of the same things in its own way.
#[cfg(feature = "url")]
#[cfg_attr(docsrs, doc(cfg(feature = "url")))]
impl TryFrom<&::url::Url> for Surt<'static> {
    type Error = url::Error;

    fn try_from(url: &::url::Url) -> Result<Self, Self::Error> {
        Self::from_url(url.as_str())
    }
}

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
impl serde::Serialize for Surt<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.key)
    }
}

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
impl<'de: 'a, 'a> serde::Deserialize<'de> for Surt<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SurtVisitor;

        impl<'de> serde::de::Visitor<'de> for SurtVisitor {
            type Value = Surt<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a SURT key")
            }

            fn visit_borrowed_str<E: serde::de::Error>(
                self,
                key: &'de str,
            ) -> Result<Self::Value, E> {
                Surt::parse(key).map_err(E::custom)
            }

            fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<Self::Value, E> {
                Surt::parse(key).map(Surt::into_owned).map_err(E::custom)
            }

            fn visit_string<E: serde::de::Error>(self, key: String) -> Result<Self::Value, E> {
                let shape = shape(&key).map_err(|kind| {
                    E::custom(Error {
                        key: key.clone(),
                        kind,
                    })
                })?;

                Ok(Surt {
                    key: Cow::Owned(key),
                    shape,
                })
            }
        }

        // `Surt<'de>` coerces to `Surt<'a>` because `'de: 'a`.
        deserializer.deserialize_str(SurtVisitor)
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for Surt<'_> {
    type Static = Surt<'static>;

    fn to_static(&self) -> Self::Static {
        self.clone().into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Surt<'_> {
    type Static = Surt<'static>;

    fn into_static(self) -> Self::Static {
        self.into_owned()
    }
}

/// The labels of a key's host, in reverse DNS order; see [`Surt::labels`].
#[derive(Clone, Debug)]
pub struct Labels<'a>(std::str::Split<'a, char>);

impl<'a> Iterator for Labels<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DoubleEndedIterator for Labels<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

impl std::iter::FusedIterator for Labels<'_> {}

fn shape(key: &str) -> Result<Shape, ErrorKind> {
    if key
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(ErrorKind::InvalidCharacter);
    }

    let host_end = key.find(')').ok_or(ErrorKind::MissingHostTerminator)?;
    let host = &key[..host_end];
    let labels_end = if host.starts_with('[') {
        host.find(']').ok_or(ErrorKind::MalformedHost)? + 1
    } else if host.bytes().filter(|&byte| byte == b':').count() > 1 {
        // A bare IPv6 address: nothing after it can be told apart from the address.
        host_end
    } else {
        host.find(':').unwrap_or(host_end)
    };

    if labels_end == 0 {
        return Err(ErrorKind::MalformedHost);
    }

    if let Some(rest) = host.get(labels_end..).filter(|rest| !rest.is_empty()) {
        let digits = rest.strip_prefix(':').ok_or(ErrorKind::MalformedHost)?;

        if digits.is_empty()
            || !digits.bytes().all(|byte| byte.is_ascii_digit())
            || digits.parse::<u16>().is_err()
        {
            return Err(ErrorKind::InvalidPort);
        }
    }

    let query_start = key[host_end..].find('?').map(|index| host_end + index);

    Ok(Shape {
        labels_end,
        host_end,
        query_start,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    // Named explicitly because `proptest::prelude` exports a `prop` module of its own.
    use crate::prop;

    #[test]
    fn parses_keys() {
        let key = Surt::parse("com,example,www:8080)/path/to?a=1&b=2").unwrap();

        assert_eq!(key.host(), "com,example,www");
        assert_eq!(key.labels().collect::<Vec<_>>(), ["com", "example", "www"]);
        assert_eq!(
            key.labels().rev().collect::<Vec<_>>(),
            ["www", "example", "com"]
        );
        assert_eq!(key.port(), Some(8080));
        assert_eq!(key.path(), "/path/to");
        assert_eq!(key.query(), Some("a=1&b=2"));
        assert_eq!(
            key.url().to_string(),
            "https://www.example.com:8080/path/to?a=1&b=2"
        );
        assert_eq!(
            key.url_with_scheme("http").to_string(),
            "http://www.example.com:8080/path/to?a=1&b=2"
        );

        let key = Surt::parse("com,example)").unwrap();

        assert_eq!(key.port(), None);
        assert_eq!(key.path(), "");
        assert_eq!(key.query(), None);
        assert_eq!(key.url().to_string(), "https://example.com/");

        let key = Surt::parse("[2001:db8::1]:80)/?").unwrap();

        assert_eq!(key.labels().collect::<Vec<_>>(), ["[2001:db8::1]"]);
        assert_eq!(key.port(), Some(80));
        assert_eq!(key.query(), Some(""));
        assert_eq!(
            key.url_with_scheme("ws").to_string(),
            "ws://[2001:db8::1]:80/?"
        );
    }

    #[test]
    fn rejects_malformed_keys() {
        let kind = |key: &str| Surt::parse(key).unwrap_err().kind;

        assert_eq!(kind("com,example/"), ErrorKind::MissingHostTerminator);
        assert_eq!(kind(")/"), ErrorKind::MalformedHost);
        assert_eq!(kind("[::1)/"), ErrorKind::MalformedHost);
        assert_eq!(kind("[::1]x)/"), ErrorKind::MalformedHost);
        assert_eq!(kind("com,example:)/"), ErrorKind::InvalidPort);
        assert_eq!(kind("com,example:+5)/"), ErrorKind::InvalidPort);
        assert_eq!(kind("com,example:65536)/"), ErrorKind::InvalidPort);
        assert_eq!(kind("com,example)/a b"), ErrorKind::InvalidCharacter);

        assert_eq!(
            Surt::parse("com,example/").unwrap_err().to_string(),
            "not a SURT: `com,example/` has no `)`"
        );
    }

    #[test]
    fn converts_and_orders() {
        let key: Surt<'static> = "com,example)/".parse().unwrap();
        let cow: Cow<'_, str> = key.clone().into();

        assert_eq!(cow, "com,example)/");
        assert_eq!(key.to_string(), "com,example)/");
        assert!(key < Surt::parse("com,example)/a").unwrap());
        assert!(key < Surt::parse("com,example,www)/").unwrap());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn round_trips_through_serde() {
        let json = "\"com,example)/path\"";
        let key: Surt<'_> = serde_json::from_str(json).unwrap();

        assert!(matches!(key.key, Cow::Borrowed(_)));
        assert_eq!(serde_json::to_string(&key).unwrap(), json);
        assert!(serde_json::from_str::<Surt<'_>>("\"nope\"").is_err());
    }

    #[proptest::property_test]
    fn parsing_preserves_components(#[strategy = prop::key_parts()] parts: prop::KeyParts) {
        let text = parts.to_string();
        let key = Surt::parse(&text).unwrap();

        prop_assert_eq!(key.as_str(), text.as_str());
        prop_assert_eq!(key.labels().collect::<Vec<_>>(), parts.labels);
        prop_assert_eq!(key.port(), parts.port);
        prop_assert_eq!(key.path(), parts.path.as_str());
        prop_assert_eq!(key.query(), parts.query.as_deref());
        prop_assert_eq!(key.clone().into_owned(), key);
    }

    #[test]
    fn parses_bare_ipv6_keys() {
        let key = Surt::parse("2001:db8::1:8080)/p").unwrap();

        assert_eq!(key.host(), "2001:db8::1:8080");
        assert_eq!(key.labels().collect::<Vec<_>>(), ["2001:db8::1:8080"]);
        assert_eq!(key.port(), None);
        assert_eq!(key.path(), "/p");
        assert_eq!(
            key.url_with_scheme("http").to_string(),
            "http://[2001:db8::1:8080]/p"
        );
        assert_eq!(
            Surt::parse("[2001:db8::1]:8080)/p")
                .unwrap()
                .url_with_scheme("http")
                .to_string(),
            "http://[2001:db8::1]:8080/p"
        );
    }

    #[cfg(feature = "fluent-uri")]
    #[test]
    fn converts_to_and_from_uris() {
        let uri =
            fluent_uri::Uri::parse("https://www.Example.com:443/Movies/?b=2&a=1#top".to_string())
                .unwrap();
        let key = Surt::try_from(&uri).unwrap();

        assert_eq!(key.as_str(), "com,example)/movies?a=1&b=2");
        assert_eq!(
            key.to_uri("https").unwrap().as_str(),
            "https://example.com/movies?a=1&b=2"
        );
    }

    #[cfg(feature = "fluent-uri")]
    #[test]
    fn converts_from_borrowing_uris() {
        let uri = fluent_uri::Uri::parse("http://EXAMPLE.com:80/A/B/").unwrap();

        assert_eq!(Surt::try_from(uri).unwrap().as_str(), "com,example)/a/b");
    }

    #[cfg(feature = "url")]
    #[test]
    fn converts_to_and_from_whatwg_urls() {
        let url = ::url::Url::parse("https://www.Example.com:443/Movies/?b=2&a=1#top").unwrap();
        let key = Surt::try_from(&url).unwrap();

        assert_eq!(key.as_str(), "com,example)/movies?a=1&b=2");
        assert_eq!(
            key.to_url("https").unwrap().as_str(),
            "https://example.com/movies?a=1&b=2"
        );
    }
}
