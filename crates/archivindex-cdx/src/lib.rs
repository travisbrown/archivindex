//! Data models for web archive capture indexes.
//!
//! Web archive indexes describe when a URL was captured and where the archived record can be
//! found. This crate provides models for classic CDX, CDXJ, and CDX server JSON in
//! [`format`](mod@format). All three formats can be converted to the shared [`Entry`] model.
//!
//! The other modules define field names, additional JSON properties, timestamps, and CDX server
//! query parameters.

pub mod field;
pub mod format;
pub mod properties;
pub mod query;
pub mod timestamp;

mod entry;
#[cfg(test)]
mod prop;

use std::borrow::Cow;

use bounded_static::{IntoBoundedStatic, ToBoundedStatic};

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

impl ToBoundedStatic for Entry<'_> {
    type Static = Entry<'static>;

    fn to_static(&self) -> Self::Static {
        Entry {
            key: self.key.to_static(),
            timestamp: self.timestamp,
            url: self.url.to_static(),
            mime: self.mime.to_static(),
            status: self.status,
            digest: self.digest.to_static(),
            redirect: self.redirect.to_static(),
            robot_flags: self.robot_flags.to_static(),
            length: self.length,
            offset: self.offset,
            filename: self.filename.to_static(),
            record_digest: self.record_digest.to_static(),
            original: self.original.to_static(),
            extra: self.extra.to_static(),
        }
    }
}

impl IntoBoundedStatic for Entry<'_> {
    type Static = Entry<'static>;

    fn into_static(self) -> Self::Static {
        Entry {
            key: self.key.into_static(),
            timestamp: self.timestamp,
            url: self.url.into_static(),
            mime: self.mime.into_static(),
            status: self.status,
            digest: self.digest.into_static(),
            redirect: self.redirect.into_static(),
            robot_flags: self.robot_flags.into_static(),
            length: self.length,
            offset: self.offset,
            filename: self.filename.into_static(),
            record_digest: self.record_digest.into_static(),
            original: self.original.into_static(),
            extra: self.extra.into_static(),
        }
    }
}

impl ToBoundedStatic for Location<'_> {
    type Static = Location<'static>;

    fn to_static(&self) -> Self::Static {
        Location {
            length: self.length,
            offset: self.offset,
            filename: self.filename.to_static(),
        }
    }
}

impl IntoBoundedStatic for Location<'_> {
    type Static = Location<'static>;

    fn into_static(self) -> Self::Static {
        Location {
            length: self.length,
            offset: self.offset,
            filename: self.filename.into_static(),
        }
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
    fn bounded_static_conversions_detach_all_text() {
        let borrowed = borrowed_entry();
        for entry in [borrowed.to_static(), borrowed.clone().into_static()] {
            assert_eq!(entry, borrowed);
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
    }
}
