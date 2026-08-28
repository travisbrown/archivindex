//! Canonical field names shared by CDX representations.

use std::borrow::Cow;
use std::fmt;

/// A semantic CDX field name.
///
/// Known aliases from CDXJ, the CDX server API, and classic CDX legends map to dedicated variants.
/// Any other name or legend marker is retained verbatim.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Field<'a> {
    /// Searchable URL key (`urlkey`, classic `N` or `A`).
    UrlKey,
    /// Capture timestamp (`timestamp`, classic `b`).
    Timestamp,
    /// Original captured URL (`url` or `original`, classic `a`).
    Url,
    /// Response media type (`mime` or `mimetype`, classic `m`).
    Mime,
    /// HTTP response status (`status` or `statuscode`, classic `s`).
    Status,
    /// Payload digest (`digest`, classic `k`).
    Digest,
    /// Redirect target (`redirect`, classic `r`).
    Redirect,
    /// Robots or AIF meta flags (`robotflags`, classic `M`).
    RobotFlags,
    /// Stored record length (`length`, classic `S`).
    Length,
    /// Stored record offset (`offset`, classic `V`).
    Offset,
    /// Archive filename (`filename`, classic `g`).
    Filename,
    /// Digest of the complete stored record (`recordDigest`).
    RecordDigest,
    /// Resolved revisit record length (`orig.length`).
    OriginalLength,
    /// Resolved revisit record offset (`orig.offset`).
    OriginalOffset,
    /// Resolved revisit archive filename (`orig.filename`).
    OriginalFilename,
    /// A field that is not modeled, under the name or legend marker it appeared with.
    Other(Cow<'a, str>),
}

impl<'a> Field<'a> {
    /// Interpret a named CDXJ or CDX server field.
    #[must_use]
    pub fn named(name: &'a str) -> Self {
        match name {
            "urlkey" => Self::UrlKey,
            "timestamp" => Self::Timestamp,
            "url" | "original" => Self::Url,
            "mime" | "mimetype" => Self::Mime,
            "status" | "statuscode" => Self::Status,
            "digest" => Self::Digest,
            "redirect" => Self::Redirect,
            "robotflags" | "meta" => Self::RobotFlags,
            "length" => Self::Length,
            "offset" => Self::Offset,
            "filename" => Self::Filename,
            "recordDigest" => Self::RecordDigest,
            "orig.length" => Self::OriginalLength,
            "orig.offset" => Self::OriginalOffset,
            "orig.filename" => Self::OriginalFilename,
            other => Self::Other(Cow::Borrowed(other)),
        }
    }

    /// Interpret a marker from a classic CDX legend.
    #[must_use]
    pub fn classic(marker: &'a str) -> Self {
        match marker {
            "N" | "A" => Self::UrlKey,
            "b" => Self::Timestamp,
            "a" => Self::Url,
            "m" => Self::Mime,
            "s" => Self::Status,
            "k" => Self::Digest,
            "R" | "r" => Self::Redirect,
            "M" => Self::RobotFlags,
            "S" => Self::Length,
            "V" => Self::Offset,
            "g" => Self::Filename,
            other => Self::Other(Cow::Borrowed(other)),
        }
    }

    /// The canonical field name used by CDXJ and CDX server JSON, or the name an unmodeled field
    /// appeared with.
    #[must_use]
    pub fn as_name(&self) -> &str {
        match self {
            Self::UrlKey => "urlkey",
            Self::Timestamp => "timestamp",
            Self::Url => "url",
            Self::Mime => "mime",
            Self::Status => "status",
            Self::Digest => "digest",
            Self::Redirect => "redirect",
            Self::RobotFlags => "robotflags",
            Self::Length => "length",
            Self::Offset => "offset",
            Self::Filename => "filename",
            Self::RecordDigest => "recordDigest",
            Self::OriginalLength => "orig.length",
            Self::OriginalOffset => "orig.offset",
            Self::OriginalFilename => "orig.filename",
            Self::Other(name) => name,
        }
    }

    /// Detach this field name from borrowed input.
    #[must_use]
    pub fn into_owned(self) -> Field<'static> {
        match self {
            Self::Other(value) => Field::Other(Cow::Owned(value.into_owned())),
            Self::UrlKey => Field::UrlKey,
            Self::Timestamp => Field::Timestamp,
            Self::Url => Field::Url,
            Self::Mime => Field::Mime,
            Self::Status => Field::Status,
            Self::Digest => Field::Digest,
            Self::Redirect => Field::Redirect,
            Self::RobotFlags => Field::RobotFlags,
            Self::Length => Field::Length,
            Self::Offset => Field::Offset,
            Self::Filename => Field::Filename,
            Self::RecordDigest => Field::RecordDigest,
            Self::OriginalLength => Field::OriginalLength,
            Self::OriginalOffset => Field::OriginalOffset,
            Self::OriginalFilename => Field::OriginalFilename,
        }
    }
}

impl fmt::Display for Field<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_name())
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for Field<'_> {
    type Static = Field<'static>;

    fn to_static(&self) -> Self::Static {
        self.clone().into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Field<'_> {
    type Static = Field<'static>;

    fn into_static(self) -> Self::Static {
        self.into_owned()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::sample::select;

    use super::*;
    use crate::strategies;

    fn canonical_fields() -> Vec<(&'static str, Field<'static>)> {
        vec![
            ("urlkey", Field::UrlKey),
            ("timestamp", Field::Timestamp),
            ("url", Field::Url),
            ("mime", Field::Mime),
            ("status", Field::Status),
            ("digest", Field::Digest),
            ("redirect", Field::Redirect),
            ("robotflags", Field::RobotFlags),
            ("length", Field::Length),
            ("offset", Field::Offset),
            ("filename", Field::Filename),
            ("recordDigest", Field::RecordDigest),
            ("orig.length", Field::OriginalLength),
            ("orig.offset", Field::OriginalOffset),
            ("orig.filename", Field::OriginalFilename),
        ]
    }

    #[test]
    fn named_aliases_resolve_to_canonical_fields() {
        let aliases = [
            ("original", Field::Url),
            ("mimetype", Field::Mime),
            ("statuscode", Field::Status),
            ("meta", Field::RobotFlags),
        ];

        for (name, expected) in canonical_fields().into_iter().chain(aliases) {
            assert_eq!(Field::named(name), expected);
            assert_eq!(expected.as_name(), expected.to_string());
        }
    }

    #[test]
    fn classic_aliases_resolve_to_canonical_fields() {
        let cases = [
            ("N", Field::UrlKey),
            ("A", Field::UrlKey),
            ("b", Field::Timestamp),
            ("a", Field::Url),
            ("m", Field::Mime),
            ("s", Field::Status),
            ("k", Field::Digest),
            ("R", Field::Redirect),
            ("r", Field::Redirect),
            ("M", Field::RobotFlags),
            ("S", Field::Length),
            ("V", Field::Offset),
            ("g", Field::Filename),
        ];

        for (marker, expected) in cases {
            assert_eq!(Field::classic(marker), expected);
        }
    }

    #[test]
    fn ownership_conversion_preserves_every_field() {
        for (_, field) in canonical_fields() {
            assert_eq!(field.clone().into_owned(), field);
        }

        let field = Field::Other(Cow::Borrowed("custom"));
        assert_eq!(
            field.into_owned(),
            Field::Other(Cow::Owned("custom".to_owned()))
        );
    }

    #[cfg(feature = "bounded-static")]
    #[test]
    fn bounded_static_conversions_own_unmodeled_names() {
        use bounded_static::{IntoBoundedStatic as _, ToBoundedStatic as _};

        let field = Field::Other(Cow::Borrowed("custom"));
        assert!(matches!(field.to_static(), Field::Other(Cow::Owned(name)) if name == "custom"));
        assert!(matches!(field.into_static(), Field::Other(Cow::Owned(name)) if name == "custom"));
    }

    /// Aliases resolve to a canonical name that names the same field.
    #[test_strategy::proptest]
    fn names_are_canonical(#[strategy(strategies::bare_text())] name: String) {
        let field = Field::named(&name);

        prop_assert_eq!(&Field::named(field.as_name()), &field);
    }

    #[test_strategy::proptest]
    fn classic_markers_are_canonical(
        #[strategy(select(vec!["N", "A", "b", "a", "m", "s", "k", "R", "r", "M", "S", "V", "g",
                               "c", "e", "n", "z"]))]
        marker: &'static str,
    ) {
        let field = Field::classic(marker);

        prop_assert_eq!(&Field::named(field.as_name()), &field);
    }

    #[test_strategy::proptest]
    fn unmodeled_names_are_borrowed_then_owned(#[strategy(strategies::bare_text())] name: String) {
        let field = Field::named(&name);
        prop_assume!(matches!(field, Field::Other(_)));
        prop_assert!(matches!(&field, Field::Other(Cow::Borrowed(value)) if *value == name));
        prop_assert_eq!(field.to_string(), name.as_str());
        prop_assert!(
            matches!(field.into_owned(), Field::Other(Cow::Owned(value)) if value == name)
        );
    }
}
