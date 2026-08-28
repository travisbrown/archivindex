//! Data models for web archive capture indexes.
//!
//! Web archive indexes describe when a URL was captured and where the archived record can be
//! found. This crate provides models for classic CDX, CDXJ, and CDX server JSON in
//! [`format`](mod@format). All three formats can convert to the shared [`Entry`] model.
//!
//! The other modules define field names, additional JSON properties, timestamps, and CDX server
//! query parameters. Other crates handle reading index files, sorting them, looking up entries,
//! and using the record offsets and lengths to read WARC data.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod field;
pub mod format;
pub mod properties;
pub mod query;
pub mod timestamp;

mod entry;
#[cfg(test)]
mod prop;

use std::borrow::Cow;

use crate::properties::ExtraProperties;
use crate::timestamp::Timestamp;

/// An index entry shared by classic CDX, CDXJ, and CDX server JSON.
///
/// The searchable key, timestamp, and original URL are required. Other standard fields are optional
/// because the fields in a classic CDX file or a CDX server response can vary. When converting text
/// fields, `-` means the value is absent. A required field with that value is an error.
/// Unrecognized text fields are kept in [`extra`](Self::extra) unless their value is `-`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    /// Searchable URL key, usually a SURT but retained as text for non-URL entries and legacy data.
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
    /// Stored archive record length.
    ///
    /// Negative lengths found in public CDX server results are represented as `None`.
    pub length: Option<u64>,
    /// Stored archive record offset.
    pub offset: Option<u64>,
    /// Archive filename.
    pub filename: Option<Cow<'a, str>>,
    /// Digest of the complete stored record.
    pub record_digest: Option<Cow<'a, str>>,
    /// Resolved original location for revisit records.
    pub original: Option<Location<'a>>,
    /// Additional fields, kept under their original names or legend markers.
    pub extra: ExtraProperties,
}

/// The location of the original record containing the payload referenced by a revisit entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Location<'a> {
    /// Stored archive record length.
    pub length: Option<u64>,
    /// Stored archive record offset.
    pub offset: Option<u64>,
    /// Archive filename.
    pub filename: Option<Cow<'a, str>>,
}

impl Entry<'_> {
    /// Return an owned entry that can outlive the input.
    #[must_use]
    pub fn into_owned(self) -> Entry<'static> {
        Entry {
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
impl bounded_static::ToBoundedStatic for Entry<'_> {
    type Static = Entry<'static>;

    fn to_static(&self) -> Self::Static {
        self.clone().into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Entry<'_> {
    type Static = Entry<'static>;

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

    /// Every text field borrows from the input so the test can check that all fields become owned.
    fn borrowed_entry() -> Entry<'static> {
        Entry {
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
        let entry = borrowed_entry().into_owned();

        assert!(matches!(entry.key, Cow::Owned(_)));
        assert!(matches!(entry.url, Cow::Owned(_)));
        assert!(matches!(entry.mime, Some(Cow::Owned(_))));
        assert!(matches!(entry.digest, Some(Cow::Owned(_))));
        assert!(matches!(entry.redirect, Some(Cow::Owned(_))));
        assert!(matches!(entry.robot_flags, Some(Cow::Owned(_))));
        assert!(matches!(entry.filename, Some(Cow::Owned(_))));
        assert!(matches!(entry.record_digest, Some(Cow::Owned(_))));
        assert!(matches!(
            entry.original.and_then(|location| location.filename),
            Some(Cow::Owned(_))
        ));
    }

    #[cfg(feature = "bounded-static")]
    #[test]
    fn bounded_static_conversions_detach_entry_and_location() {
        use bounded_static::{IntoBoundedStatic as _, ToBoundedStatic as _};

        let entry = borrowed_entry();
        assert_eq!(entry.to_static(), entry.clone().into_owned());
        assert_eq!(entry.clone().into_static(), entry.clone().into_owned());

        let location = entry.original.unwrap();
        assert_eq!(location.to_static(), location.clone().into_owned());
        assert_eq!(location.clone().into_static(), location.into_owned());
    }
}
