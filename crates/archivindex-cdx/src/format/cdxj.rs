//! CDXJ entries and their JSON field objects.

use std::borrow::Cow;
use std::str::FromStr;

use bounded_static::{IntoBoundedStatic, ToBoundedStatic};

use crate::properties::{self, ExtraProperties};
use crate::timestamp::{self, Timestamp};

/// A CDXJ line cannot be parsed or serialized.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The line does not contain a key, timestamp, and JSON object.
    #[error("truncated CDXJ line: {0}")]
    Truncated(String),
    /// The key is empty or contains whitespace.
    #[error("invalid CDXJ key: {0:?}")]
    InvalidKey(String),
    /// Custom fields did not serialize to a JSON object.
    #[error("CDXJ fields must serialize to a JSON object")]
    NonObjectFields,
    /// The timestamp is invalid.
    #[error(transparent)]
    InvalidTimestamp(#[from] timestamp::Error),
    /// The JSON field object is invalid.
    #[error("invalid CDXJ field block")]
    InvalidFields(#[source] serde_json::Error),
}

/// A CDXJ entry consisting of a key, timestamp, and JSON field object.
///
/// Convert to [`crate::Entry`] to work with the shared model across CDX formats.
///
/// By default, [`Entry`] uses [`Fields`], which allows standard fields to be missing.
/// Use [`ConformingEntry`] to require all seven mandatory CDXJ 0.1.0 fields. In both cases,
/// `recordDigest` is optional. Use [`Entry::to_line`] to serialize either form; it validates the
/// key and propagates serialization errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry<'a, F = Fields<'a>> {
    /// The searchable URL key.
    pub key: Cow<'a, str>,
    /// The capture timestamp.
    pub timestamp: Timestamp,
    /// The JSON field object.
    pub fields: F,
}

impl<'a> Entry<'a> {
    /// Check the key and the presence of all required CDXJ 0.1.0 fields without cloning the entry.
    ///
    /// Rejects empty keys, whitespace in keys, missing required fields, and extension properties
    /// with reserved names. Successful validation does not allocate. Use [`ConformingEntry::try_from`]
    /// to retain a conforming value, passing the entry by value to reuse its owned data.
    pub fn validate(&self) -> Result<(), ConformanceError> {
        validate_key(&self.key, ConformanceError::InvalidKey)?;
        self.fields.validate()
    }

    /// Parse a CDXJ line without its trailing newline.
    pub fn parse(line: &'a str) -> Result<Self, Error> {
        let (key, rest) = line
            .split_once(' ')
            .ok_or_else(|| Error::Truncated(line.to_owned()))?;
        let (timestamp, fields) = rest
            .split_once(' ')
            .ok_or_else(|| Error::Truncated(line.to_owned()))?;
        if key.is_empty() || timestamp.is_empty() || fields.is_empty() {
            return Err(Error::Truncated(line.to_owned()));
        }

        validate_key(key, Error::InvalidKey)?;
        Ok(Self {
            key: Cow::Borrowed(key),
            timestamp: timestamp.parse()?,
            fields: serde_json::from_str(fields).map_err(Error::InvalidFields)?,
        })
    }
}

/// A CDXJ entry with every field required by CDXJ 0.1.0.
///
/// The key remains editable and is checked by [`Entry::to_line`] when serializing.
pub type ConformingEntry<'a> = Entry<'a, ConformingFields<'a>>;

impl FromStr for Entry<'static> {
    type Err = Error;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let entry: Entry<'_> = Entry::parse(line)?;
        Ok(entry.into_static())
    }
}

impl<F: serde::Serialize> Entry<'_, F> {
    /// Serialize a CDXJ line without a trailing newline.
    ///
    /// Rejects empty keys, whitespace in keys, and fields that cannot be serialized as a JSON
    /// object. Standard field types also reject extension properties with reserved names.
    pub fn to_line(&self) -> Result<String, Error> {
        validate_key(&self.key, Error::InvalidKey)?;
        let fields = serde_json::to_string(&self.fields).map_err(Error::InvalidFields)?;
        if fields.starts_with('{') {
            Ok(format!("{} {} {fields}", self.key, self.timestamp))
        } else {
            Err(Error::NonObjectFields)
        }
    }
}

fn validate_key<E>(key: &str, invalid: impl FnOnce(String) -> E) -> Result<(), E> {
    if key.is_empty() || key.contains(char::is_whitespace) {
        Err(invalid(key.to_owned()))
    } else {
        Ok(())
    }
}

impl<'a> TryFrom<Entry<'a>> for ConformingEntry<'a> {
    type Error = ConformanceError;

    fn try_from(entry: Entry<'a>) -> Result<Self, Self::Error> {
        entry.validate()?;
        Ok(Self {
            key: entry.key,
            timestamp: entry.timestamp,
            fields: ConformingFields::from_checked(entry.fields),
        })
    }
}

impl<'a> TryFrom<&Entry<'a>> for ConformingEntry<'a> {
    type Error = ConformanceError;

    fn try_from(entry: &Entry<'a>) -> Result<Self, Self::Error> {
        entry.validate()?;
        Ok(Self {
            key: entry.key.clone(),
            timestamp: entry.timestamp,
            fields: ConformingFields::from_checked(entry.fields.clone()),
        })
    }
}

/// The JSON object in a CDXJ entry, allowing standard fields to be missing.
///
/// Numeric fields accept JSON numbers and decimal strings. They are written as strings to match
/// common CDXJ indexers. Unrecognized properties are preserved.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Fields<'a> {
    /// The original URL.
    #[serde(borrow)]
    pub url: Cow<'a, str>,
    /// The response payload digest.
    #[serde(
        default,
        with = "archivindex_serde::borrowable_str::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub digest: Option<Cow<'a, str>>,
    /// The response media type.
    #[serde(
        default,
        with = "archivindex_serde::borrowable_str::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub mime: Option<Cow<'a, str>>,
    /// The HTTP response status.
    #[serde(
        default,
        deserialize_with = "crate::format::optional_integer",
        serialize_with = "crate::format::optional_integer_str",
        skip_serializing_if = "Option::is_none"
    )]
    pub status: Option<u16>,
    /// The WARC record offset.
    #[serde(
        default,
        deserialize_with = "crate::format::optional_integer",
        serialize_with = "crate::format::optional_integer_str",
        skip_serializing_if = "Option::is_none"
    )]
    pub offset: Option<u64>,
    /// The WARC record length.
    #[serde(
        default,
        deserialize_with = "crate::format::optional_integer",
        serialize_with = "crate::format::optional_integer_str",
        skip_serializing_if = "Option::is_none"
    )]
    pub length: Option<u64>,
    /// The WARC filename.
    #[serde(
        default,
        with = "archivindex_serde::borrowable_str::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub filename: Option<Cow<'a, str>>,
    /// The digest of the complete stored WARC record.
    #[serde(
        rename = "recordDigest",
        default,
        with = "archivindex_serde::borrowable_str::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub record_digest: Option<Cow<'a, str>>,
    /// Additional JSON properties. Serialization rejects names reserved for standard fields.
    #[serde(flatten, serialize_with = "serialize_extra")]
    pub extra: ExtraProperties,
}

/// A CDXJ field object with all fields required by CDXJ 0.1.0.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct ConformingFields<'a> {
    /// The original URL.
    url: Cow<'a, str>,
    /// The response payload digest.
    digest: Cow<'a, str>,
    /// The response media type.
    mime: Cow<'a, str>,
    /// The HTTP response status.
    #[serde(serialize_with = "crate::format::integer_str")]
    status: u16,
    /// The WARC record offset.
    #[serde(serialize_with = "crate::format::integer_str")]
    offset: u64,
    /// The WARC record length.
    #[serde(serialize_with = "crate::format::integer_str")]
    length: u64,
    /// The WARC filename.
    filename: Cow<'a, str>,
    /// The digest of the complete stored WARC record.
    #[serde(rename = "recordDigest", skip_serializing_if = "Option::is_none")]
    record_digest: Option<Cow<'a, str>>,
    /// Additional JSON properties.
    #[serde(flatten)]
    extra: ExtraProperties,
}

impl<'a> ConformingFields<'a> {
    /// Create a field object with the seven required fields and no optional properties.
    #[must_use]
    pub fn new(
        url: impl Into<Cow<'a, str>>,
        digest: impl Into<Cow<'a, str>>,
        mime: impl Into<Cow<'a, str>>,
        status: u16,
        offset: u64,
        length: u64,
        filename: impl Into<Cow<'a, str>>,
    ) -> Self {
        Self {
            url: url.into(),
            digest: digest.into(),
            mime: mime.into(),
            status,
            offset,
            length,
            filename: filename.into(),
            record_digest: None,
            extra: ExtraProperties::default(),
        }
    }

    /// Set the digest of the complete stored WARC record.
    #[must_use]
    pub fn with_record_digest(mut self, value: impl Into<Cow<'a, str>>) -> Self {
        self.record_digest = Some(value.into());
        self
    }

    /// Set additional properties, rejecting any whose names duplicate standard fields.
    pub fn with_extra(mut self, value: ExtraProperties) -> Result<Self, properties::Error> {
        validate_extra(&value)?;
        self.extra = value;
        Ok(self)
    }

    /// The original URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The response payload digest.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// The response media type.
    #[must_use]
    pub fn mime(&self) -> &str {
        &self.mime
    }

    /// The HTTP response status.
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// The WARC record offset.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// The WARC record length.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }

    /// The WARC filename.
    #[must_use]
    pub fn filename(&self) -> &str {
        &self.filename
    }

    /// The digest of the complete stored WARC record.
    #[must_use]
    pub fn record_digest(&self) -> Option<&str> {
        self.record_digest.as_deref()
    }

    /// Additional JSON properties.
    #[must_use]
    pub const fn extra(&self) -> &ExtraProperties {
        &self.extra
    }
}

impl<'a> From<ConformingFields<'a>> for Fields<'a> {
    fn from(fields: ConformingFields<'a>) -> Self {
        Self {
            url: fields.url,
            digest: Some(fields.digest),
            mime: Some(fields.mime),
            status: Some(fields.status),
            offset: Some(fields.offset),
            length: Some(fields.length),
            filename: Some(fields.filename),
            record_digest: fields.record_digest,
            extra: fields.extra,
        }
    }
}

/// A CDXJ entry or field object does not conform to CDXJ 0.1.0.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConformanceError {
    /// The entry key is empty or contains whitespace.
    #[error("invalid CDXJ key: {0:?}")]
    InvalidKey(String),
    /// Required properties are absent.
    #[error("missing required CDXJ fields: {}", .0.join(", "))]
    Missing(Vec<&'static str>),
    /// An extension property duplicates a modeled property.
    #[error(transparent)]
    Extra(#[from] properties::Error),
}

impl Fields<'_> {
    /// Check that required CDXJ 0.1.0 fields are present and extension names are not reserved.
    ///
    /// URL and digest strings are not parsed. Successful validation does not allocate.
    /// Use [`Entry::validate`] to check the key as well, or [`ConformingFields::try_from`] to retain
    /// a conforming field object.
    pub fn validate(&self) -> Result<(), ConformanceError> {
        let mut missing = Vec::new();
        if self.digest.is_none() {
            missing.push("digest");
        }
        if self.mime.is_none() {
            missing.push("mime");
        }
        if self.status.is_none() {
            missing.push("status");
        }
        if self.offset.is_none() {
            missing.push("offset");
        }
        if self.length.is_none() {
            missing.push("length");
        }
        if self.filename.is_none() {
            missing.push("filename");
        }
        if !missing.is_empty() {
            return Err(ConformanceError::Missing(missing));
        }

        Ok(validate_extra(&self.extra)?)
    }
}

impl<'a> TryFrom<Fields<'a>> for ConformingFields<'a> {
    type Error = ConformanceError;

    fn try_from(fields: Fields<'a>) -> Result<Self, Self::Error> {
        fields.validate()?;
        Ok(Self::from_checked(fields))
    }
}

impl<'a> ConformingFields<'a> {
    /// Move fields whose conformance has already been checked.
    fn from_checked(fields: Fields<'a>) -> Self {
        Self {
            url: fields.url,
            digest: fields.digest.expect("conformance checked by caller"),
            mime: fields.mime.expect("conformance checked by caller"),
            status: fields.status.expect("conformance checked by caller"),
            offset: fields.offset.expect("conformance checked by caller"),
            length: fields.length.expect("conformance checked by caller"),
            filename: fields.filename.expect("conformance checked by caller"),
            record_digest: fields.record_digest,
            extra: fields.extra,
        }
    }
}

impl<'a> TryFrom<&Fields<'a>> for ConformingFields<'a> {
    type Error = ConformanceError;

    fn try_from(fields: &Fields<'a>) -> Result<Self, Self::Error> {
        fields.validate()?;
        Ok(Self::from_checked(fields.clone()))
    }
}

impl<'a> From<Entry<'a>> for crate::Entry<'a> {
    fn from(entry: Entry<'a>) -> Self {
        Self {
            key: entry.key,
            timestamp: entry.timestamp,
            url: entry.fields.url,
            mime: entry.fields.mime,
            status: entry.fields.status,
            digest: entry.fields.digest,
            redirect: None,
            robot_flags: None,
            length: entry.fields.length,
            offset: entry.fields.offset,
            filename: entry.fields.filename,
            record_digest: entry.fields.record_digest,
            original: None,
            extra: entry.fields.extra,
        }
    }
}

fn serialize_extra<S: serde::ser::Serializer>(
    extra: &ExtraProperties,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    validate_extra(extra).map_err(serde::ser::Error::custom)?;
    serde::ser::Serialize::serialize(extra, serializer)
}

fn validate_extra(extra: &ExtraProperties) -> Result<(), properties::Error> {
    extra.validate(
        "CDXJ fields",
        &[
            "url",
            "digest",
            "mime",
            "status",
            "offset",
            "length",
            "filename",
            "recordDigest",
        ],
    )
}

impl ToBoundedStatic for Fields<'_> {
    type Static = Fields<'static>;

    fn to_static(&self) -> Self::Static {
        Fields {
            url: self.url.to_static(),
            digest: self.digest.to_static(),
            mime: self.mime.to_static(),
            status: self.status,
            offset: self.offset,
            length: self.length,
            filename: self.filename.to_static(),
            record_digest: self.record_digest.to_static(),
            extra: self.extra.to_static(),
        }
    }
}

impl IntoBoundedStatic for Fields<'_> {
    type Static = Fields<'static>;

    fn into_static(self) -> Self::Static {
        Fields {
            url: self.url.into_static(),
            digest: self.digest.into_static(),
            mime: self.mime.into_static(),
            status: self.status,
            offset: self.offset,
            length: self.length,
            filename: self.filename.into_static(),
            record_digest: self.record_digest.into_static(),
            extra: self.extra.into_static(),
        }
    }
}

impl ToBoundedStatic for ConformingFields<'_> {
    type Static = ConformingFields<'static>;

    fn to_static(&self) -> Self::Static {
        ConformingFields {
            url: self.url.to_static(),
            digest: self.digest.to_static(),
            mime: self.mime.to_static(),
            status: self.status,
            offset: self.offset,
            length: self.length,
            filename: self.filename.to_static(),
            record_digest: self.record_digest.to_static(),
            extra: self.extra.to_static(),
        }
    }
}

impl IntoBoundedStatic for ConformingFields<'_> {
    type Static = ConformingFields<'static>;

    fn into_static(self) -> Self::Static {
        ConformingFields {
            url: self.url.into_static(),
            digest: self.digest.into_static(),
            mime: self.mime.into_static(),
            status: self.status,
            offset: self.offset,
            length: self.length,
            filename: self.filename.into_static(),
            record_digest: self.record_digest.into_static(),
            extra: self.extra.into_static(),
        }
    }
}

impl<F: ToBoundedStatic> ToBoundedStatic for Entry<'_, F> {
    type Static = Entry<'static, F::Static>;

    fn to_static(&self) -> Self::Static {
        Entry {
            key: self.key.to_static(),
            timestamp: self.timestamp,
            fields: self.fields.to_static(),
        }
    }
}

impl<F: IntoBoundedStatic> IntoBoundedStatic for Entry<'_, F> {
    type Static = Entry<'static, F::Static>;

    fn into_static(self) -> Self::Static {
        Entry {
            key: self.key.into_static(),
            timestamp: self.timestamp,
            fields: self.fields.into_static(),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::prop;

    const EXAMPLE: &str = concat!(
        "com,example)/ 20201007212236 {\"url\":\"https://example.com/\",",
        "\"digest\":\"sha256:test\",\"mime\":\"text/html\",\"status\":\"200\",",
        "\"offset\":\"784\",\"length\":\"1300\",\"filename\":\"data.warc.gz\"}",
    );

    fn complete_fields() -> ConformingFields<'static> {
        ConformingFields::new(
            "https://example.com/",
            "sha256:payload",
            "text/html",
            200,
            784,
            1300,
            "data.warc.gz",
        )
    }

    #[test]
    fn parses_spec_shape() -> Result<(), Box<dyn std::error::Error>> {
        let entry = Entry::parse(EXAMPLE)?;
        assert_eq!(entry.key, "com,example)/");
        assert_eq!(entry.fields.status, Some(200));
        assert_eq!(entry.fields.offset, Some(784));
        assert_eq!(entry.fields.length, Some(1300));
        Ok(())
    }

    #[test]
    fn accepts_numeric_fields_and_extensions() -> Result<(), Box<dyn std::error::Error>> {
        let entry = Entry::parse(
            "com,example)/ 20201007212236 {\"url\":\"https://example.com/\",\"offset\":784,\"custom\":true}",
        )?;
        assert_eq!(entry.fields.offset, Some(784));
        assert_eq!(entry.fields.extra["custom"], true);
        assert!(matches!(entry.fields.url, Cow::Borrowed(_)));
        Ok(())
    }

    #[test]
    fn serialized_line_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let entry = Entry::parse(EXAMPLE)?;
        assert_eq!(Entry::parse(&entry.to_line()?)?, entry);
        Ok(())
    }

    #[test]
    fn rejects_truncated_and_invalid_lines() {
        for line in [
            "",
            "key",
            "key timestamp",
            " timestamp {}",
            "key  {}",
            "key timestamp ",
        ] {
            assert!(matches!(Entry::parse(line), Err(Error::Truncated(value)) if value == line));
        }

        assert!(matches!(
            Entry::parse("key 20201007212236 {"),
            Err(Error::InvalidFields(_))
        ));
    }

    #[test]
    fn from_str_owns_the_entry() -> Result<(), Error> {
        let entry = EXAMPLE.parse::<Entry<'static>>()?;

        assert!(matches!(entry.key, Cow::Owned(_)));
        assert!(matches!(entry.fields.url, Cow::Owned(_)));
        Ok(())
    }

    #[test]
    fn conforming_accessors_and_builders_expose_every_value() {
        let mut extra = ExtraProperties::default();
        extra.insert("custom".to_owned(), true.into());
        let fields = complete_fields()
            .with_record_digest("sha256:record")
            .with_extra(extra)
            .unwrap();

        assert_eq!(fields.url(), "https://example.com/");
        assert_eq!(fields.digest(), "sha256:payload");
        assert_eq!(fields.mime(), "text/html");
        assert_eq!(fields.status(), 200);
        assert_eq!(fields.offset(), 784);
        assert_eq!(fields.length(), 1300);
        assert_eq!(fields.filename(), "data.warc.gz");
        assert_eq!(fields.record_digest(), Some("sha256:record"));
        assert_eq!(fields.extra()["custom"], true);

        let owned = fields.into_static();
        assert!(matches!(owned.url, Cow::Owned(_)));
        assert!(matches!(owned.digest, Cow::Owned(_)));
        assert!(matches!(owned.mime, Cow::Owned(_)));
        assert!(matches!(owned.filename, Cow::Owned(_)));
        assert!(matches!(owned.record_digest, Some(Cow::Owned(_))));
    }

    #[test]
    fn entries_convert_to_conforming_and_shared_entries() -> Result<(), Box<dyn std::error::Error>>
    {
        let entry = Entry::parse(EXAMPLE)?;
        let conforming = ConformingEntry::try_from(&entry)?;

        assert_eq!(conforming.key, entry.key);
        assert_eq!(conforming.fields.status(), 200);

        let entry = crate::Entry::from(entry);
        assert_eq!(entry.key, "com,example)/");
        assert_eq!(entry.url, "https://example.com/");
        assert_eq!(entry.status, Some(200));
        assert_eq!(entry.redirect, None);
        assert_eq!(entry.robot_flags, None);
        assert_eq!(entry.original, None);
        Ok(())
    }

    #[test]
    fn entry_validation_and_conversions_reject_invalid_keys() -> Result<(), Error> {
        for key in [
            "",
            "invalid key",
            "key\nnext",
            "key\rnext",
            "key\tnext",
            "key\u{a0}next",
        ] {
            let mut entry = Entry::parse(EXAMPLE)?;
            entry.key = key.into();
            assert_eq!(entry.fields.validate(), Ok(()));
            let expected = Err(ConformanceError::InvalidKey(key.to_owned()));
            assert_eq!(entry.validate(), expected);
            assert_eq!(ConformingEntry::try_from(&entry).map(|_| ()), expected);
            assert_eq!(ConformingEntry::try_from(entry).map(|_| ()), expected);
        }
        Ok(())
    }

    #[test]
    fn entry_validation_and_conversions_agree_on_field_errors() -> Result<(), Error> {
        let mut missing = Entry::parse(EXAMPLE)?;
        missing.fields.digest = None;
        missing.fields.mime = None;
        let mut collision = Entry::parse(EXAMPLE)?;
        collision
            .fields
            .extra
            .insert("url".to_owned(), "other".into());
        for (entry, expected) in [
            (Entry::parse(EXAMPLE)?, Ok(())),
            (
                missing,
                Err(ConformanceError::Missing(vec!["digest", "mime"])),
            ),
            (
                collision,
                Err(ConformanceError::Extra(properties::Error {
                    model: "CDXJ fields",
                    property: "url".to_owned(),
                })),
            ),
        ] {
            assert_eq!(entry.fields.validate(), expected);
            assert_eq!(
                ConformingFields::try_from(&entry.fields).map(|_| ()),
                expected
            );
            assert_eq!(entry.validate(), expected);
            assert_eq!(ConformingEntry::try_from(&entry).map(|_| ()), expected);
            assert_eq!(ConformingEntry::try_from(entry).map(|_| ()), expected);
        }
        Ok(())
    }

    #[test]
    fn consuming_entry_conversion_reuses_owned_buffers() -> Result<(), Box<dyn std::error::Error>> {
        let mut entry = Entry::parse(EXAMPLE)?.into_static();
        entry.fields.record_digest = Some("sha256:record".to_owned().into());
        entry
            .fields
            .extra
            .insert("custom".to_owned(), "extension value".into());
        let pointers = [
            entry.key.as_ptr(),
            entry.fields.url.as_ptr(),
            entry.fields.digest.as_ref().unwrap().as_ptr(),
            entry.fields.mime.as_ref().unwrap().as_ptr(),
            entry.fields.filename.as_ref().unwrap().as_ptr(),
            entry.fields.record_digest.as_ref().unwrap().as_ptr(),
            entry.fields.extra.keys().next().unwrap().as_ptr(),
            entry.fields.extra["custom"].as_str().unwrap().as_ptr(),
        ];
        let extra_value = std::ptr::from_ref(&entry.fields.extra["custom"]);
        let expected = ConformingEntry::try_from(&entry)?;
        entry.validate()?;
        let converted = ConformingEntry::try_from(entry)?;
        assert_eq!(converted, expected);
        assert_eq!(
            pointers,
            [
                converted.key.as_ptr(),
                converted.fields.url().as_ptr(),
                converted.fields.digest().as_ptr(),
                converted.fields.mime().as_ptr(),
                converted.fields.filename().as_ptr(),
                converted.fields.record_digest().unwrap().as_ptr(),
                converted.fields.extra().keys().next().unwrap().as_ptr(),
                converted.fields.extra()["custom"]
                    .as_str()
                    .unwrap()
                    .as_ptr(),
            ]
        );
        assert_eq!(
            extra_value,
            std::ptr::from_ref(&converted.fields.extra()["custom"])
        );
        Ok(())
    }

    #[test]
    fn consuming_entry_conversion_retains_borrowed_strings()
    -> Result<(), Box<dyn std::error::Error>> {
        let entry = Entry::parse(EXAMPLE)?;
        let converted = ConformingEntry::try_from(entry)?;
        for value in [
            &converted.key,
            &converted.fields.url,
            &converted.fields.digest,
            &converted.fields.mime,
            &converted.fields.filename,
        ] {
            assert!(matches!(value, Cow::Borrowed(_)));
        }
        assert_eq!(converted.to_line()?, EXAMPLE);
        Ok(())
    }

    #[test]
    fn bounded_static_conversions_detach_every_model() -> Result<(), Box<dyn std::error::Error>> {
        let input = EXAMPLE.to_owned();
        let entry = Entry::parse(&input)?;
        let conforming = ConformingEntry::try_from(&entry)?;
        for fields in [entry.fields.to_static(), entry.fields.clone().into_static()] {
            assert_eq!(fields, entry.fields);
            assert!(matches!(fields.url, Cow::Owned(_)));
        }
        for fields in [
            conforming.fields.to_static(),
            conforming.fields.clone().into_static(),
        ] {
            assert_eq!(fields, conforming.fields);
            assert!(matches!(fields.url, Cow::Owned(_)));
            assert!(matches!(fields.digest, Cow::Owned(_)));
            assert!(matches!(fields.mime, Cow::Owned(_)));
            assert!(matches!(fields.filename, Cow::Owned(_)));
        }
        let entries = [entry.to_static(), entry.into_static()];
        let conforming_entries = [conforming.to_static(), conforming.into_static()];
        drop(input);

        for entry in entries {
            assert_eq!(entry.to_line()?, EXAMPLE);
            assert!(matches!(entry.key, Cow::Owned(_)));
            assert!(matches!(entry.fields.url, Cow::Owned(_)));
        }
        for entry in conforming_entries {
            assert_eq!(entry.key, "com,example)/");
            assert_eq!(entry.fields.url(), "https://example.com/");
            assert!(matches!(entry.key, Cow::Owned(_)));
            assert!(matches!(entry.fields.url, Cow::Owned(_)));
        }
        Ok(())
    }

    #[test]
    fn required_fields_report_all_missing_properties() -> Result<(), Error> {
        let entry =
            Entry::parse("com,example)/ 20201007212236 {\"url\":\"https://example.com/\"}")?;
        assert_eq!(
            ConformingFields::try_from(&entry.fields),
            Err(ConformanceError::Missing(vec![
                "digest", "mime", "status", "offset", "length", "filename"
            ]))
        );
        Ok(())
    }

    #[test]
    fn required_fields_reject_extension_collisions() {
        let mut fields = Fields {
            url: "https://example.com/".into(),
            digest: Some("sha1:test".into()),
            mime: Some("text/html".into()),
            status: Some(200),
            offset: Some(0),
            length: Some(1),
            filename: Some("data.warc.gz".into()),
            record_digest: None,
            extra: ExtraProperties::default(),
        };
        fields.extra.insert("offset".to_owned(), 1.into());

        assert!(matches!(
            ConformingFields::try_from(&fields),
            Err(ConformanceError::Extra(properties::Error { property, .. }))
                if property == "offset"
        ));
    }

    #[test]
    fn serialization_rejects_all_reserved_extension_names() -> Result<(), Error> {
        for name in [
            "url",
            "digest",
            "mime",
            "status",
            "offset",
            "length",
            "filename",
            "recordDigest",
        ] {
            let mut entry = Entry::parse(EXAMPLE)?;
            entry
                .fields
                .extra
                .insert(name.to_owned(), "duplicate".into());
            assert!(serde_json::to_string(&entry.fields).is_err());
            assert!(matches!(entry.to_line(), Err(Error::InvalidFields(_))));
        }
        Ok(())
    }

    #[test]
    fn serialization_checks_keys_even_after_conformance_conversion()
    -> Result<(), Box<dyn std::error::Error>> {
        for key in ["", "invalid key", "key\nnext", "key\rnext", "key\tnext"] {
            let mut entry = Entry::parse(EXAMPLE)?;
            let mut conforming = ConformingEntry::try_from(&entry)?;
            entry.key = key.into();
            conforming.key = key.into();
            assert!(matches!(entry.to_line(), Err(Error::InvalidKey(_))));
            assert!(matches!(conforming.to_line(), Err(Error::InvalidKey(_))));
            if !key.is_empty() {
                assert!(Entry::parse(&format!("{key} 20201007212236 {{\"url\":\"u\"}}")).is_err());
            }
        }
        Ok(())
    }

    #[test]
    fn serialization_preserves_custom_errors() -> Result<(), Error> {
        struct BrokenFields;

        impl serde::ser::Serialize for BrokenFields {
            fn serialize<S: serde::ser::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("custom field failure"))
            }
        }

        let entry = Entry {
            key: "com,example)/".into(),
            timestamp: Entry::parse(EXAMPLE)?.timestamp,
            fields: BrokenFields,
        };
        let error = entry.to_line().unwrap_err();
        assert!(
            matches!(error, Error::InvalidFields(ref source) if source.to_string() == "custom field failure")
        );
        assert!(matches!(
            Entry {
                key: entry.key,
                timestamp: entry.timestamp,
                fields: 42,
            }
            .to_line(),
            Err(Error::NonObjectFields)
        ));
        Ok(())
    }

    #[proptest::property_test]
    fn text_round_trips(#[strategy = prop::entry()] entry: Entry<'static>) {
        let line = entry.to_line().unwrap();

        prop_assert_eq!(
            Entry::parse(&line).map(Entry::into_static).ok(),
            Some(entry)
        );
    }

    /// The lenient field object accepts a JSON number wherever it accepts a numeric string.
    #[proptest::property_test]
    fn numbers_and_numeric_strings_agree(
        status: u16,
        offset: u64,
        length: u64,
        #[strategy = prop::json_text()] filename: String,
    ) {
        let filename = serde_json::Value::String(filename);
        let quoted = format!(
            "{{\"url\":\"u\",\"status\":\"{status}\",\"offset\":\"{offset}\",\
             \"length\":\"{length}\",\"filename\":{filename}}}"
        );
        let bare = format!(
            "{{\"url\":\"u\",\"status\":{status},\"offset\":{offset},\
             \"length\":{length},\"filename\":{filename}}}"
        );

        let parsed = serde_json::from_str::<Fields<'_>>(&quoted).ok();

        prop_assert!(parsed.is_some());
        prop_assert_eq!(parsed, serde_json::from_str::<Fields<'_>>(&bare).ok());
    }

    #[proptest::property_test]
    fn conforming_fields_survive_the_lenient_form(
        #[strategy = prop::conforming_fields()] fields: ConformingFields<'static>,
    ) {
        let lenient = Fields::from(fields.clone());

        prop_assert_eq!(
            ConformingFields::try_from(&lenient).ok(),
            Some(fields.clone())
        );
        prop_assert_eq!(ConformingFields::try_from(lenient).ok(), Some(fields));
    }
}
