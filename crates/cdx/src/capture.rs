//! A format-neutral capture record.

use std::borrow::Cow;

use crate::field::Field;
use crate::properties::ExtraProperties;
use crate::timestamp::Timestamp;

/// A standard field in a CDX record is absent or malformed.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// A field required for a capture model is absent.
    #[error("missing CDX field `{0}`")]
    Missing(&'static str),
    /// A standard field cannot be parsed.
    #[error("invalid CDX field `{field}`: `{value}`")]
    Invalid {
        /// The field name.
        field: &'static str,
        /// The unparseable value.
        value: String,
    },
}

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

pub(crate) fn present(value: &str) -> Option<&str> {
    (value != "-").then_some(value)
}

pub(crate) fn from_fields<'a>(fields: &[(Field<'_>, Cow<'a, str>)]) -> Result<Capture<'a>, Error> {
    let key = required_value(fields, &Field::UrlKey, "urlkey")?;
    let timestamp = required_value(fields, &Field::Timestamp, "timestamp")?;
    let url = required_value(fields, &Field::Url, "url")?;

    let timestamp = timestamp
        .parse()
        .map_err(|_| invalid("timestamp", timestamp.as_ref()))?;
    let original = location(fields)?;
    let mut extra = ExtraProperties::default();

    for (field, value) in fields {
        if let (Field::Other(name), Some(value)) = (field, present(value)) {
            extra.insert(
                name.to_string(),
                serde_json::Value::String(value.to_owned()),
            );
        }
    }

    Ok(Capture {
        key,
        timestamp,
        url,
        mime: optional_value(fields, &Field::Mime),
        status: parse_optional(fields, &Field::Status, "status")?,
        digest: optional_value(fields, &Field::Digest),
        redirect: optional_value(fields, &Field::Redirect),
        robot_flags: optional_value(fields, &Field::RobotFlags),
        length: parse_length(fields, &Field::Length, "length")?,
        offset: parse_optional(fields, &Field::Offset, "offset")?,
        filename: optional_value(fields, &Field::Filename),
        record_digest: optional_value(fields, &Field::RecordDigest),
        original,
        extra,
    })
}

fn required_value<'a>(
    fields: &[(Field<'_>, Cow<'a, str>)],
    target: &Field<'_>,
    name: &'static str,
) -> Result<Cow<'a, str>, Error> {
    fields
        .iter()
        .find(|(field, _)| field == target)
        .and_then(|(_, value)| present(value).map(|_| value.clone()))
        .ok_or(Error::Missing(name))
}

fn optional_value<'a>(
    fields: &[(Field<'_>, Cow<'a, str>)],
    target: &Field<'_>,
) -> Option<Cow<'a, str>> {
    fields
        .iter()
        .find(|(field, _)| field == target)
        .and_then(|(_, value)| present(value).map(|_| value.clone()))
}

fn parse_optional<T: std::str::FromStr>(
    fields: &[(Field<'_>, Cow<'_, str>)],
    target: &Field<'_>,
    name: &'static str,
) -> Result<Option<T>, Error> {
    optional_value(fields, target)
        .map(|value| value.parse().map_err(|_| invalid(name, value.as_ref())))
        .transpose()
}

/// Parse a length, treating a negative value as absent.
fn parse_length(
    fields: &[(Field<'_>, Cow<'_, str>)],
    target: &Field<'_>,
    name: &'static str,
) -> Result<Option<u64>, Error> {
    optional_value(fields, target)
        .filter(|value| !value.starts_with('-'))
        .map(|value| value.parse().map_err(|_| invalid(name, value.as_ref())))
        .transpose()
}

fn location<'a>(fields: &[(Field<'_>, Cow<'a, str>)]) -> Result<Option<Location<'a>>, Error> {
    let length = parse_length(fields, &Field::OriginalLength, "orig.length")?;
    let offset = parse_optional(fields, &Field::OriginalOffset, "orig.offset")?;
    let filename = optional_value(fields, &Field::OriginalFilename);
    Ok(
        (length.is_some() || offset.is_some() || filename.is_some()).then_some(Location {
            length,
            offset,
            filename,
        }),
    )
}

fn invalid(field: &'static str, value: &str) -> Error {
    Error::Invalid {
        field,
        value: value.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn required_fields() -> Vec<(Field<'static>, Cow<'static, str>)> {
        vec![
            (Field::UrlKey, Cow::Borrowed("com,example)/")),
            (Field::Timestamp, Cow::Borrowed("20201007212236")),
            (Field::Url, Cow::Borrowed("https://example.com/")),
        ]
    }

    fn complete_capture() -> Capture<'static> {
        let mut fields = required_fields();
        fields.extend([
            (Field::Mime, Cow::Borrowed("text/html")),
            (Field::Status, Cow::Borrowed("200")),
            (Field::Digest, Cow::Borrowed("sha1:payload")),
            (Field::Redirect, Cow::Borrowed("https://example.com/next")),
            (Field::RobotFlags, Cow::Borrowed("A")),
            (Field::Length, Cow::Borrowed("1300")),
            (Field::Offset, Cow::Borrowed("784")),
            (Field::Filename, Cow::Borrowed("data.warc.gz")),
            (Field::RecordDigest, Cow::Borrowed("sha1:record")),
            (Field::OriginalLength, Cow::Borrowed("1200")),
            (Field::OriginalOffset, Cow::Borrowed("400")),
            (Field::OriginalFilename, Cow::Borrowed("original.warc.gz")),
            (
                Field::Other(Cow::Borrowed("custom")),
                Cow::Borrowed("value"),
            ),
            (Field::Other(Cow::Borrowed("absent")), Cow::Borrowed("-")),
        ]);
        from_fields(&fields).unwrap()
    }

    #[test]
    fn converts_every_field() {
        let capture = complete_capture();

        assert_eq!(capture.mime.as_deref(), Some("text/html"));
        assert_eq!(capture.status, Some(200));
        assert_eq!(capture.digest.as_deref(), Some("sha1:payload"));
        assert_eq!(
            capture.redirect.as_deref(),
            Some("https://example.com/next")
        );
        assert_eq!(capture.robot_flags.as_deref(), Some("A"));
        assert_eq!(capture.length, Some(1300));
        assert_eq!(capture.offset, Some(784));
        assert_eq!(capture.filename.as_deref(), Some("data.warc.gz"));
        assert_eq!(capture.record_digest.as_deref(), Some("sha1:record"));
        assert_eq!(
            capture.original,
            Some(Location {
                length: Some(1200),
                offset: Some(400),
                filename: Some(Cow::Borrowed("original.warc.gz")),
            })
        );
        assert_eq!(capture.extra["custom"], "value");
        assert!(!capture.extra.contains_key("absent"));
    }

    #[test]
    fn missing_and_invalid_required_fields_are_reported() {
        for (index, name) in ["urlkey", "timestamp", "url"].into_iter().enumerate() {
            let mut fields = required_fields();
            fields.remove(index);
            assert_eq!(from_fields(&fields), Err(Error::Missing(name)));
        }

        let mut fields = required_fields();
        fields[1].1 = Cow::Borrowed("not-a-timestamp");
        assert_eq!(
            from_fields(&fields),
            Err(Error::Invalid {
                field: "timestamp",
                value: "not-a-timestamp".to_owned(),
            })
        );
    }

    #[test]
    fn malformed_numeric_fields_are_reported() {
        for field in [
            Field::Status,
            Field::Length,
            Field::Offset,
            Field::OriginalLength,
            Field::OriginalOffset,
        ] {
            let mut fields = required_fields();
            fields.push((field.clone(), Cow::Borrowed("invalid")));
            assert!(matches!(
                from_fields(&fields),
                Err(Error::Invalid { value, .. }) if value == "invalid"
            ));
        }
    }

    #[test]
    fn negative_lengths_are_absent() {
        let mut fields = required_fields();
        fields.extend([
            (Field::Length, Cow::Borrowed("-1")),
            (Field::OriginalLength, Cow::Borrowed("-2")),
        ]);

        let capture = from_fields(&fields).unwrap();
        assert_eq!(capture.length, None);
        assert_eq!(capture.original, None);
    }

    #[test]
    fn ownership_conversion_detaches_all_text() {
        let capture = complete_capture().into_owned();

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

        let capture = complete_capture();
        assert_eq!(capture.to_static(), capture.clone().into_owned());
        assert_eq!(capture.clone().into_static(), capture.clone().into_owned());

        let location = capture.original.unwrap();
        assert_eq!(location.to_static(), location.clone().into_owned());
        assert_eq!(location.clone().into_static(), location.into_owned());
    }

    #[test_strategy::proptest]
    fn numeric_fields_round_trip(status: u16, length: u64, offset: u64) {
        let mut fields = required_fields();
        fields.extend([
            (Field::Status, Cow::Owned(status.to_string())),
            (Field::Length, Cow::Owned(length.to_string())),
            (Field::Offset, Cow::Owned(offset.to_string())),
        ]);

        let capture = from_fields(&fields).unwrap();
        prop_assert_eq!(capture.status, Some(status));
        prop_assert_eq!(capture.length, Some(length));
        prop_assert_eq!(capture.offset, Some(offset));
    }
}
