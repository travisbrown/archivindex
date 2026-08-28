//! Building a [`Capture`] from the text fields of an index record.

use std::borrow::Cow;

use crate::field::{Error, Field};
use crate::properties::ExtraProperties;
use crate::{Capture, Location};

fn present(value: &str) -> Option<&str> {
    (value != "-").then_some(value)
}

/// Text values are collected before parsing to preserve validation order. A stored `-` still
/// occupies its field, so later duplicate columns cannot replace the first occurrence.
#[derive(Default)]
struct TextFields<'a> {
    key: Option<Cow<'a, str>>,
    timestamp: Option<Cow<'a, str>>,
    url: Option<Cow<'a, str>>,
    mime: Option<Cow<'a, str>>,
    status: Option<Cow<'a, str>>,
    digest: Option<Cow<'a, str>>,
    redirect: Option<Cow<'a, str>>,
    robot_flags: Option<Cow<'a, str>>,
    length: Option<Cow<'a, str>>,
    offset: Option<Cow<'a, str>>,
    filename: Option<Cow<'a, str>>,
    record_digest: Option<Cow<'a, str>>,
    original_length: Option<Cow<'a, str>>,
    original_offset: Option<Cow<'a, str>>,
    original_filename: Option<Cow<'a, str>>,
    extra: ExtraProperties,
}

impl<'a, 'f> FromIterator<(Field<'f>, Cow<'a, str>)> for TextFields<'a> {
    fn from_iter<T: IntoIterator<Item = (Field<'f>, Cow<'a, str>)>>(fields: T) -> Self {
        let mut values = Self::default();
        for (field, value) in fields {
            let slot = match field {
                Field::UrlKey => &mut values.key,
                Field::Timestamp => &mut values.timestamp,
                Field::Url => &mut values.url,
                Field::Mime => &mut values.mime,
                Field::Status => &mut values.status,
                Field::Digest => &mut values.digest,
                Field::Redirect => &mut values.redirect,
                Field::RobotFlags => &mut values.robot_flags,
                Field::Length => &mut values.length,
                Field::Offset => &mut values.offset,
                Field::Filename => &mut values.filename,
                Field::RecordDigest => &mut values.record_digest,
                Field::OriginalLength => &mut values.original_length,
                Field::OriginalOffset => &mut values.original_offset,
                Field::OriginalFilename => &mut values.original_filename,
                Field::Other(name) => {
                    if present(&value).is_some() {
                        values.extra.insert(
                            name.into_owned(),
                            serde_json::Value::String(value.into_owned()),
                        );
                    }
                    continue;
                }
            };
            slot.get_or_insert(value);
        }
        values
    }
}

pub fn from_fields<'a, 'f>(
    fields: impl IntoIterator<Item = (Field<'f>, Cow<'a, str>)>,
) -> Result<Capture<'a>, Error> {
    let values: TextFields<'a> = fields.into_iter().collect();
    let key = required_value(values.key, "urlkey")?;
    let timestamp = required_value(values.timestamp, "timestamp")?;
    let url = required_value(values.url, "url")?;

    let timestamp = timestamp
        .parse()
        .map_err(|_| invalid("timestamp", &timestamp))?;
    let length = parse_length(values.original_length.as_deref(), "orig.length")?;
    let offset = parse_optional(values.original_offset.as_deref(), "orig.offset")?;
    let filename = optional_value(values.original_filename);
    let original =
        (length.is_some() || offset.is_some() || filename.is_some()).then_some(Location {
            length,
            offset,
            filename,
        });

    Ok(Capture {
        key,
        timestamp,
        url,
        mime: optional_value(values.mime),
        status: parse_optional(values.status.as_deref(), "status")?,
        digest: optional_value(values.digest),
        redirect: optional_value(values.redirect),
        robot_flags: optional_value(values.robot_flags),
        length: parse_length(values.length.as_deref(), "length")?,
        offset: parse_optional(values.offset.as_deref(), "offset")?,
        filename: optional_value(values.filename),
        record_digest: optional_value(values.record_digest),
        original,
        extra: values.extra,
    })
}

fn required_value<'a>(
    value: Option<Cow<'a, str>>,
    name: &'static str,
) -> Result<Cow<'a, str>, Error> {
    optional_value(value).ok_or(Error::Missing(name))
}

fn optional_value(value: Option<Cow<'_, str>>) -> Option<Cow<'_, str>> {
    value.filter(|value| present(value).is_some())
}

fn parse_optional<T: std::str::FromStr>(
    value: Option<&str>,
    name: &'static str,
) -> Result<Option<T>, Error> {
    value
        .and_then(present)
        .map(|value| value.parse().map_err(|_| invalid(name, value)))
        .transpose()
}

/// Parse a length, treating a negative value as absent.
fn parse_length(value: Option<&str>, name: &'static str) -> Result<Option<u64>, Error> {
    parse_optional(value.filter(|value| !value.starts_with('-')), name)
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
        from_fields(fields).unwrap()
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
            assert_eq!(from_fields(fields), Err(Error::Missing(name)));
        }

        let mut fields = required_fields();
        fields[1].1 = Cow::Borrowed("not-a-timestamp");
        assert_eq!(
            from_fields(fields),
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
                from_fields(fields),
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

        let capture = from_fields(fields).unwrap();
        assert_eq!(capture.length, None);
        assert_eq!(capture.original, None);
    }

    #[test]
    fn duplicate_fields_keep_their_existing_precedence() {
        let mut fields = required_fields();
        fields.extend([
            (Field::Url, Cow::Borrowed("ignored")),
            (Field::Status, Cow::Borrowed("-")),
            (Field::Status, Cow::Borrowed("invalid")),
            (Field::Other(Cow::Borrowed("first")), Cow::Borrowed("old")),
            (
                Field::Other(Cow::Borrowed("second")),
                Cow::Borrowed("other"),
            ),
            (Field::Other(Cow::Borrowed("first")), Cow::Borrowed("new")),
            (Field::Other(Cow::Borrowed("first")), Cow::Borrowed("-")),
        ]);
        let capture = from_fields(fields.clone()).unwrap();
        assert_eq!(capture.url, "https://example.com/");
        assert_eq!(capture.status, None);
        assert_eq!(capture.extra["first"], "new");
        assert_eq!(
            capture.extra.keys().map(String::as_str).collect::<Vec<_>>(),
            ["first", "second"]
        );

        fields.insert(0, (Field::UrlKey, Cow::Borrowed("-")));
        assert_eq!(from_fields(fields), Err(Error::Missing("urlkey")));
    }

    #[test]
    fn validation_order_does_not_depend_on_column_order() {
        let invalid_fields = [
            Field::Timestamp,
            Field::OriginalLength,
            Field::OriginalOffset,
            Field::Status,
            Field::Length,
            Field::Offset,
        ];
        for first_invalid in 0..invalid_fields.len() {
            let mut fields = required_fields();
            fields.splice(
                0..0,
                invalid_fields[first_invalid..]
                    .iter()
                    .rev()
                    .map(|field| (field.clone(), Cow::Borrowed("invalid"))),
            );
            assert_eq!(
                from_fields(fields),
                Err(Error::Invalid {
                    field: match first_invalid {
                        0 => "timestamp",
                        1 => "orig.length",
                        2 => "orig.offset",
                        3 => "status",
                        4 => "length",
                        _ => "offset",
                    },
                    value: "invalid".to_owned(),
                })
            );
        }

        // Missing required values are reported before even a malformed timestamp.
        let mut fields = required_fields();
        fields[1].1 = Cow::Borrowed("invalid");
        fields.pop();
        assert_eq!(from_fields(fields), Err(Error::Missing("url")));
    }

    #[proptest::property_test]
    fn numeric_fields_round_trip(status: u16, length: u64, offset: u64) {
        let mut fields = required_fields();
        fields.extend([
            (Field::Status, Cow::Owned(status.to_string())),
            (Field::Length, Cow::Owned(length.to_string())),
            (Field::Offset, Cow::Owned(offset.to_string())),
        ]);

        let capture = from_fields(fields).unwrap();
        prop_assert_eq!(capture.status, Some(status));
        prop_assert_eq!(capture.length, Some(length));
        prop_assert_eq!(capture.offset, Some(offset));
    }
}
