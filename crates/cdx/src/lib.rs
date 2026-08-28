//! Data models for web archive capture indexes.
//!
//! [`Capture`] is the representation-neutral model every format converts into. Format-specific
//! models for classic CDX, CDXJ, and CDX server JSON live in [`format`](mod@format). The other
//! modules define field names, extension properties, timestamps, and the CDX server's query
//! model.
//!
//! Reading files, sorting indexes, looking up captures, and resolving WARC byte ranges belong to
//! higher-level crates.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod field;
pub mod format;
pub mod properties;
pub mod query;
pub mod timestamp;

mod capture;
#[cfg(test)]
mod prop;

use std::borrow::Cow;

use crate::properties::ExtraProperties;
use crate::timestamp::Timestamp;

/// The common capture semantics represented by classic CDX, CDXJ, and CDX server JSON.
///
/// The searchable key, timestamp, and original URL are required. Other standard fields are optional
/// because classic layouts and CDX server field selections vary. When converting text fields, a
/// single hyphen marks an absent value; a required field with that value is an error. Unmodeled
/// text fields with values other than `-` are retained in [`extra`](Self::extra).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Capture<'a> {
    /// Searchable URL key, usually a SURT but retained as text for non-URL records and legacy data.
    pub key: Cow<'a, str>,
    /// Capture timestamp.
    pub timestamp: Timestamp,
    /// Original captured URL.
    pub url: Cow<'a, str>,
    /// Response media type.
    pub mime: Option<Cow<'a, str>>,
    /// HTTP response status.
    pub status: Option<u16>,
    /// Payload digest in the encoding used by the index producer.
    pub digest: Option<Cow<'a, str>>,
    /// Redirect target.
    pub redirect: Option<Cow<'a, str>>,
    /// Robots or AIF meta flags.
    pub robot_flags: Option<Cow<'a, str>>,
    /// Stored record length.
    ///
    /// Negative lengths found in public CDX server results are represented as `None`.
    pub length: Option<u64>,
    /// Stored record offset.
    pub offset: Option<u64>,
    /// Archive filename.
    pub filename: Option<Cow<'a, str>>,
    /// Digest of the complete stored record.
    pub record_digest: Option<Cow<'a, str>>,
    /// Resolved original location for revisit records.
    pub original: Option<Location<'a>>,
    /// Fields that are not modeled, keyed by the names or legend markers they appeared with.
    pub extra: ExtraProperties,
}

/// The original payload-bearing record referenced by a revisit entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Location<'a> {
    /// Stored record length.
    pub length: Option<u64>,
    /// Stored record offset.
    pub offset: Option<u64>,
    /// Archive filename.
    pub filename: Option<Cow<'a, str>>,
}

impl Capture<'_> {
    /// Detach this capture from borrowed input.
    #[must_use]
    pub fn into_owned(self) -> Capture<'static> {
        Capture {
            key: Cow::Owned(self.key.into_owned()),
            timestamp: self.timestamp,
            url: Cow::Owned(self.url.into_owned()),
            mime: self.mime.map(|value| Cow::Owned(value.into_owned())),
            status: self.status,
            digest: self.digest.map(|value| Cow::Owned(value.into_owned())),
            redirect: self.redirect.map(|value| Cow::Owned(value.into_owned())),
            robot_flags: self.robot_flags.map(|value| Cow::Owned(value.into_owned())),
            length: self.length,
            offset: self.offset,
            filename: self.filename.map(|value| Cow::Owned(value.into_owned())),
            record_digest: self
                .record_digest
                .map(|value| Cow::Owned(value.into_owned())),
            original: self.original.map(Location::into_owned),
            extra: self.extra,
        }
    }
}

impl Location<'_> {
    fn into_owned(self) -> Location<'static> {
        Location {
            length: self.length,
            offset: self.offset,
            filename: self.filename.map(|value| Cow::Owned(value.into_owned())),
        }
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for Capture<'_> {
    type Static = Capture<'static>;

    fn to_static(&self) -> Self::Static {
        self.clone().into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Capture<'_> {
    type Static = Capture<'static>;

    fn into_static(self) -> Self::Static {
        self.into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for Location<'_> {
    type Static = Location<'static>;

    fn to_static(&self) -> Self::Static {
        self.clone().into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Location<'_> {
    type Static = Location<'static>;

    fn into_static(self) -> Self::Static {
        self.into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A capture whose every text field borrows, so that a detaching conversion has work to do.
    fn borrowed_capture() -> Capture<'static> {
        Capture {
            key: Cow::Borrowed("com,example)/"),
            timestamp: "20201007212236".parse().expect("a valid timestamp"),
            url: Cow::Borrowed("https://example.com/"),
            mime: Some(Cow::Borrowed("text/html")),
            status: Some(200),
            digest: Some(Cow::Borrowed("sha1:payload")),
            redirect: Some(Cow::Borrowed("https://example.com/next")),
            robot_flags: Some(Cow::Borrowed("A")),
            length: Some(1300),
            offset: Some(784),
            filename: Some(Cow::Borrowed("data.warc.gz")),
            record_digest: Some(Cow::Borrowed("sha1:record")),
            original: Some(Location {
                length: Some(1200),
                offset: Some(400),
                filename: Some(Cow::Borrowed("original.warc.gz")),
            }),
            extra: ExtraProperties::default(),
        }
    }

    #[test]
    fn ownership_conversion_detaches_all_text() {
        let capture = borrowed_capture().into_owned();

        assert!(matches!(capture.key, Cow::Owned(_)));
        assert!(matches!(capture.url, Cow::Owned(_)));
        assert!(matches!(capture.mime, Some(Cow::Owned(_))));
        assert!(matches!(capture.digest, Some(Cow::Owned(_))));
        assert!(matches!(capture.redirect, Some(Cow::Owned(_))));
        assert!(matches!(capture.robot_flags, Some(Cow::Owned(_))));
        assert!(matches!(capture.filename, Some(Cow::Owned(_))));
        assert!(matches!(capture.record_digest, Some(Cow::Owned(_))));
        assert!(matches!(
            capture.original.and_then(|location| location.filename),
            Some(Cow::Owned(_))
        ));
    }

    #[cfg(feature = "bounded-static")]
    #[test]
    fn bounded_static_conversions_detach_capture_and_location() {
        use bounded_static::{IntoBoundedStatic as _, ToBoundedStatic as _};

        let capture = borrowed_capture();
        assert_eq!(capture.to_static(), capture.clone().into_owned());
        assert_eq!(capture.clone().into_static(), capture.clone().into_owned());

        let location = capture.original.unwrap();
        assert_eq!(location.to_static(), location.clone().into_owned());
        assert_eq!(location.clone().into_static(), location.into_owned());
    }
}
