//! JSON output from the Internet Archive CDX server API.
//!
//! Each response is a JSON array. Its first row names the fields, and the remaining rows contain
//! their values as strings. If more results are available, the response may end with an empty row
//! followed by a row containing only a resume key. Use that key to request the next page.

use std::borrow::Cow;
use std::marker::PhantomData;

use bounded_static::{IntoBoundedStatic, ToBoundedStatic};
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;

use crate::Entry;
use crate::field::{self, Field};

/// A CDX server JSON document has an invalid structure or field value.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// Entries or a resume key were provided without a header.
    #[error("CDX JSON entries require a nonempty header")]
    MissingHeader,
    /// An entry has a different number of values from the header.
    #[error("CDX JSON entry {entry} has {actual} fields, expected {expected}")]
    FieldCount {
        /// Zero-based entry index.
        entry: usize,
        /// Number of fields named by the header.
        expected: usize,
        /// Number of values in the entry.
        actual: usize,
    },
    /// The resume trailer has an invalid shape.
    #[error("invalid CDX JSON resume trailer")]
    InvalidResumeTrailer,

    /// A row cannot be converted to the common entry model.
    #[error("invalid CDX JSON entry {entry}")]
    Field {
        /// Zero-based entry index.
        entry: usize,
        /// The invalid field.
        #[source]
        source: field::Error,
    },
}

/// A CDX server JSON response, including its header, entries, and optional resume key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document<'a> {
    fields: Vec<Cow<'a, str>>,
    entries: Vec<Vec<Cow<'a, str>>>,
    resume_key: Option<Cow<'a, str>>,
}

impl<'a> Document<'a> {
    /// Create a document, checking that every entry has the number of fields given in the header.
    pub fn new(
        fields: Vec<Cow<'a, str>>,
        entries: Vec<Vec<Cow<'a, str>>>,
        resume_key: Option<Cow<'a, str>>,
    ) -> Result<Self, Error> {
        let document = Self {
            fields,
            entries,
            resume_key,
        };
        document.validate()?;
        Ok(document)
    }

    /// The names in the header row.
    #[must_use]
    pub fn fields(&self) -> &[Cow<'a, str>] {
        &self.fields
    }

    /// The entry rows.
    #[must_use]
    pub fn entries(&self) -> &[Vec<Cow<'a, str>>] {
        &self.entries
    }

    /// The key used to request the next page, when present.
    #[must_use]
    pub fn resume_key(&self) -> Option<&str> {
        self.resume_key.as_deref()
    }

    /// Convert all entry rows to the shared [`Entry`] model.
    pub fn into_entries(self) -> Result<EntryList<'a>, Error> {
        let Self {
            fields,
            entries,
            resume_key,
        } = self;
        let semantic_fields = fields
            .iter()
            .map(|name| Field::named(name))
            .collect::<Vec<_>>();
        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(entry, values)| {
                crate::entry::from_fields(semantic_fields.iter().cloned().zip(values))
                    .map_err(|source| Error::Field { entry, source })
            })
            .collect::<Result<_, _>>()?;
        Ok(EntryList {
            entries,
            resume_key,
        })
    }

    fn validate(&self) -> Result<(), Error> {
        if self.fields.is_empty() && (!self.entries.is_empty() || self.resume_key.is_some()) {
            return Err(Error::MissingHeader);
        }
        for (entry, values) in self.entries.iter().enumerate() {
            if values.len() != self.fields.len() {
                return Err(Error::FieldCount {
                    entry,
                    expected: self.fields.len(),
                    actual: values.len(),
                });
            }
        }
        Ok(())
    }
}

impl serde::Serialize for Document<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.fields.is_empty() {
            return serializer.serialize_seq(Some(0))?.end();
        }
        let trailer_length = usize::from(self.resume_key.is_some()) * 2;
        let mut sequence =
            serializer.serialize_seq(Some(1 + self.entries.len() + trailer_length))?;
        sequence.serialize_element(&self.fields)?;
        for entry in &self.entries {
            sequence.serialize_element(entry)?;
        }
        if let Some(resume_key) = &self.resume_key {
            sequence.serialize_element(&Vec::<String>::new())?;
            sequence.serialize_element(&[resume_key])?;
        }
        sequence.end()
    }
}

impl<'a, 'de: 'a> serde::Deserialize<'de> for Document<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DocumentVisitor<'a>(PhantomData<&'a ()>);

        impl<'a, 'de: 'a> Visitor<'de> for DocumentVisitor<'a> {
            type Value = Document<'a>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a CDX JSON array with a header row")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let Some(Row(fields)) = sequence.next_element::<Row<'a>>()? else {
                    return Ok(Document {
                        fields: Vec::new(),
                        entries: Vec::new(),
                        resume_key: None,
                    });
                };
                if fields.is_empty() {
                    return Err(serde::de::Error::custom(Error::MissingHeader));
                }

                let mut entries = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(10_000));
                let mut resume_key = None;
                while let Some(Row(row)) = sequence.next_element::<Row<'a>>()? {
                    if row.is_empty() {
                        let Some(Row(key)) = sequence.next_element::<Row<'a>>()? else {
                            return Err(serde::de::Error::custom(Error::InvalidResumeTrailer));
                        };
                        if key.len() != 1 || sequence.next_element::<Row<'a>>()?.is_some() {
                            return Err(serde::de::Error::custom(Error::InvalidResumeTrailer));
                        }
                        resume_key = key.into_iter().next();
                        break;
                    }
                    let entry = entries.len();
                    if row.len() != fields.len() {
                        return Err(serde::de::Error::custom(Error::FieldCount {
                            entry,
                            expected: fields.len(),
                            actual: row.len(),
                        }));
                    }
                    entries.push(row);
                }

                Ok(Document {
                    fields,
                    entries,
                    resume_key,
                })
            }
        }

        deserializer.deserialize_seq(DocumentVisitor(PhantomData))
    }
}

/// Entries decoded from a CDX server JSON document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryList<'a> {
    /// The decoded entries.
    pub entries: Vec<Entry<'a>>,
    /// The key used to request the next page.
    pub resume_key: Option<Cow<'a, str>>,
}

impl<'a, 'de: 'a> serde::Deserialize<'de> for EntryList<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Document::deserialize(deserializer)?
            .into_entries()
            .map_err(|error| match std::error::Error::source(&error) {
                // A deserializer error keeps only a message, so the message must include the source.
                Some(source) => serde::de::Error::custom(format_args!("{error}: {source}")),
                None => serde::de::Error::custom(error),
            })
    }
}

#[derive(serde::Deserialize)]
struct Row<'a>(#[serde(borrow, with = "archivindex_serde::borrowable_str::seq")] Vec<Cow<'a, str>>);

impl ToBoundedStatic for Document<'_> {
    type Static = Document<'static>;

    fn to_static(&self) -> Self::Static {
        Document {
            fields: self.fields.to_static(),
            entries: self.entries.to_static(),
            resume_key: self.resume_key.to_static(),
        }
    }
}

impl IntoBoundedStatic for Document<'_> {
    type Static = Document<'static>;

    fn into_static(self) -> Self::Static {
        Document {
            fields: self.fields.into_static(),
            entries: self.entries.into_static(),
            resume_key: self.resume_key.into_static(),
        }
    }
}

impl ToBoundedStatic for EntryList<'_> {
    type Static = EntryList<'static>;

    fn to_static(&self) -> Self::Static {
        EntryList {
            entries: self.entries.to_static(),
            resume_key: self.resume_key.to_static(),
        }
    }
}

impl IntoBoundedStatic for EntryList<'_> {
    type Static = EntryList<'static>;

    fn into_static(self) -> Self::Static {
        EntryList {
            entries: self.entries.into_static(),
            resume_key: self.resume_key.into_static(),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::format::classic::Header;
    use crate::prop;

    const EXAMPLE: &str = concat!(
        "[[\"urlkey\",\"timestamp\",\"url\",\"mime\",\"status\",\"digest\",\"length\"],",
        "[\"com,example)/\",\"20201007212236\",\"https://example.com/\",",
        "\"text/html\",\"200\",\"sha1:TEST\",\"1300\"],[],[\"next-key\"]]"
    );

    #[test]
    fn bounded_static_conversions_detach_documents_and_lists()
    -> Result<(), Box<dyn std::error::Error>> {
        let input = EXAMPLE.to_owned();
        let document: Document<'_> = serde_json::from_str(&input)?;
        let list = document.clone().into_entries()?;
        let documents = [document.to_static(), document.into_static()];
        let lists = [list.to_static(), list.into_static()];
        drop(input);

        for document in documents {
            assert_eq!(serde_json::to_string(&document)?, EXAMPLE);
            assert!(
                document
                    .fields
                    .iter()
                    .all(|value| matches!(value, Cow::Owned(_)))
            );
            assert!(
                document
                    .entries
                    .iter()
                    .flatten()
                    .all(|value| matches!(value, Cow::Owned(_)))
            );
            assert!(matches!(document.resume_key, Some(Cow::Owned(_))));
        }
        for list in lists {
            assert_eq!(list.entries[0].key, "com,example)/");
            assert_eq!(list.entries[0].url, "https://example.com/");
            assert_eq!(list.resume_key.as_deref(), Some("next-key"));
            assert!(matches!(list.entries[0].key, Cow::Owned(_)));
            assert!(matches!(list.entries[0].url, Cow::Owned(_)));
            assert!(matches!(list.resume_key, Some(Cow::Owned(_))));
        }
        Ok(())
    }

    #[test]
    fn decodes_header_rows_and_resume_key() -> Result<(), Box<dyn std::error::Error>> {
        let list: EntryList<'_> = serde_json::from_str(EXAMPLE)?;
        assert_eq!(list.entries.len(), 1);
        assert_eq!(list.entries[0].status, Some(200));
        assert_eq!(list.entries[0].length, Some(1300));
        assert_eq!(list.resume_key.as_deref(), Some("next-key"));
        Ok(())
    }

    #[test]
    fn decodes_wayback_aliases_and_unusual_values() -> Result<(), Box<dyn std::error::Error>> {
        let input = concat!(
            "[[\"urlkey\",\"timestamp\",\"original\",\"mimetype\",\"statuscode\",",
            "\"digest\",\"length\"],[\"com,example)/\",\"20201007212236\",",
            "\"https://example.com/\",\"application/problem+json\",\"530\",",
            "\"INVALID-DIGEST\",\"-1\"]]"
        );
        let document: Document<'_> = serde_json::from_str(input)?;
        assert!(matches!(document.fields[0], Cow::Borrowed(_)));
        assert!(matches!(document.entries[0][0], Cow::Borrowed(_)));

        let list = document.into_entries()?;
        assert!(matches!(list.entries[0].key, Cow::Borrowed(_)));
        assert!(matches!(list.entries[0].url, Cow::Borrowed(_)));
        assert!(matches!(list.entries[0].mime, Some(Cow::Borrowed(_))));
        assert_eq!(
            list.entries[0].mime.as_deref(),
            Some("application/problem+json")
        );
        assert_eq!(list.entries[0].status, Some(530));
        assert_eq!(list.entries[0].digest.as_deref(), Some("INVALID-DIGEST"));
        assert_eq!(list.entries[0].length, None);
        Ok(())
    }

    #[test]
    fn consuming_owned_rows_reuses_retained_string_buffers() {
        let input = concat!(
            "[[\"urlkey\",\"timestamp\",\"url\",\"mime\",\"digest\",\"redirect\",",
            "\"robotflags\",\"filename\",\"recordDigest\",\"orig.filename\",\"custom\"],",
            "[\"com,example)/\",\"20201007212236\",\"https://example.com/\",",
            "\"text/html\",\"payload\",\"https://example.com/next\",\"A\",",
            "\"capture.warc.gz\",\"record\",\"original.warc.gz\",\"extra value\"]]"
        );
        let document = serde_json::from_str::<Document<'_>>(input)
            .unwrap()
            .into_static();
        let pointers: Vec<_> = document.entries()[0]
            .iter()
            .map(|value| value.as_ptr())
            .collect();
        let list = document.into_entries().unwrap();
        let entry = &list.entries[0];
        let retained = [
            (0, &entry.key),
            (2, &entry.url),
            (3, entry.mime.as_ref().unwrap()),
            (4, entry.digest.as_ref().unwrap()),
            (5, entry.redirect.as_ref().unwrap()),
            (6, entry.robot_flags.as_ref().unwrap()),
            (7, entry.filename.as_ref().unwrap()),
            (8, entry.record_digest.as_ref().unwrap()),
            (
                9,
                entry.original.as_ref().unwrap().filename.as_ref().unwrap(),
            ),
        ];
        for (index, value) in retained {
            assert!(matches!(value, Cow::Owned(_)));
            assert_eq!(value.as_ptr(), pointers[index]);
        }
        assert_eq!(
            entry.extra["custom"].as_str().unwrap().as_ptr(),
            pointers[10]
        );
    }

    #[test]
    fn raw_document_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let document: Document<'_> = serde_json::from_str(EXAMPLE)?;
        let serialized = serde_json::to_string(&document)?;
        assert_eq!(serde_json::from_str::<Document<'_>>(&serialized)?, document);
        Ok(())
    }

    #[test]
    fn empty_document_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let document: Document<'_> = serde_json::from_str("[]")?;
        assert_eq!(serde_json::to_string(&document)?, "[]");
        Ok(())
    }

    #[test]
    fn field_errors_retain_row_field_and_value() -> Result<(), serde_json::Error> {
        let input = r#"[
            ["urlkey", "timestamp", "url", "offset"],
            ["com,example)/", "20201007212236", "https://example.com/", "0"],
            ["com,example)/", "20201007212236", "https://example.com/", "oops"]
        ]"#;
        let error = serde_json::from_str::<EntryList<'_>>(input).unwrap_err();
        let message = error.to_string();
        for detail in ["entry 1", "offset", "oops"] {
            assert!(message.contains(detail), "{message}");
        }

        let document: Document<'_> = serde_json::from_str(input)?;
        let error = document.into_entries().unwrap_err();
        let source = std::error::Error::source(&error).expect("field errors retain their source");
        assert_eq!(source.to_string(), "invalid CDX field `offset`: `oops`");
        assert!(matches!(error, Error::Field {
            entry: 1,
            source: field::Error::Invalid { field: "offset", ref value },
        } if value == "oops"));
        Ok(())
    }

    #[test]
    fn rejects_short_rows() {
        let result = serde_json::from_str::<Document<'_>>(
            "[[\"urlkey\",\"timestamp\"],[\"com,example)/\"]]",
        );
        assert!(result.is_err());
    }

    /// The names the CDX server uses for the fields of a standard classic legend.
    const NAMES: [&str; 11] = [
        "urlkey",
        "timestamp",
        "original",
        "mimetype",
        "statuscode",
        "digest",
        "redirect",
        "robotflags",
        "length",
        "offset",
        "filename",
    ];

    #[proptest::property_test]
    fn document_round_trips(
        #[strategy = proptest::collection::vec(prop::entry_parts(), 0..=3)] rows: Vec<
            prop::EntryParts,
        >,
        #[strategy = proptest::option::of(prop::json_text())] resume_key: Option<String>,
    ) {
        let document = Document::new(
            NAMES.iter().copied().map(Cow::Borrowed).collect(),
            rows.iter().map(prop::EntryParts::values).collect(),
            resume_key.map(Cow::Owned),
        )
        .unwrap();
        let text = serde_json::to_string(&document).unwrap();

        prop_assert_eq!(
            serde_json::from_str::<Document<'_>>(&text)
                .map(Document::into_static)
                .ok(),
            Some(document)
        );
    }

    #[proptest::property_test]
    fn entries_recover_every_value(#[strategy = prop::entry_parts()] parts: prop::EntryParts) {
        let document = Document::new(
            NAMES.iter().copied().map(Cow::Borrowed).collect(),
            vec![parts.values()],
            None,
        )
        .unwrap();
        let list = document.into_entries().unwrap();
        let expected = Header::standard_11().decode(&parts.entry()).unwrap();

        prop_assert_eq!(list.entries, vec![expected]);
        prop_assert_eq!(list.resume_key, None);
    }
}
