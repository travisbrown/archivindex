//! A sequence of strings, each borrowed from the input when possible.
//!
//! # Examples
//!
//! ```
//! use std::borrow::Cow;
//!
//! #[derive(serde::Deserialize)]
//! struct Record<'a> {
//!     #[serde(borrow, with = "archivindex_serde::borrowable_str::seq")]
//!     tags: Vec<Cow<'a, str>>,
//! }
//!
//! let record = serde_json::from_str::<Record<'_>>(r#"{"tags":["a","b"]}"#)?;
//! assert!(matches!(record.tags[0], Cow::Borrowed("a")));
//! # Ok::<(), serde_json::Error>(())
//! ```

use std::borrow::Cow;

use serde::de::{Deserializer, SeqAccess, Visitor};

/// Serialize a sequence of strings.
///
/// # Errors
///
/// Returns the serializer's own error if writing fails.
pub fn serialize<S: serde::ser::Serializer>(
    values: &[Cow<'_, str>],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serde::ser::Serialize::serialize(values, serializer)
}

/// Deserialize a sequence of strings, borrowing each from the input when possible.
///
/// Strings are returned in input order.
///
/// # Errors
///
/// Returns the deserializer's own error if the value is not a sequence, or if any element of it is
/// not a string.
pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<Cow<'de, str>>, D::Error> {
    struct SeqVisitor;

    impl<'de> Visitor<'de> for SeqVisitor {
        type Value = Vec<Cow<'de, str>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a sequence of strings")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default());

            // Unwrap elements directly into the output vector, without an intermediate collection.
            while let Some(crate::BorrowableStr(value)) = sequence.next_element()? {
                values.push(value);
            }

            Ok(values)
        }
    }

    deserializer.deserialize_seq(SeqVisitor)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    #[derive(serde::Deserialize, serde::Serialize)]
    struct Record<'a> {
        #[serde(borrow, with = "crate::borrowable_str::seq")]
        tags: Vec<Cow<'a, str>>,
    }

    #[test]
    fn a_sequence_borrows_each_element_it_can() {
        // The second element carries an escape, so only the others can be borrowed.
        let input = r#"{"tags":["first","sec\"ond","third"]}"#;

        let record = serde_json::from_str::<Record<'_>>(input).unwrap();

        assert_eq!(record.tags.len(), 3);
        assert!(matches!(record.tags[0], Cow::Borrowed("first")));
        assert!(matches!(record.tags[1], Cow::Owned(_)));
        assert!(matches!(record.tags[2], Cow::Borrowed("third")));
    }

    #[test]
    fn a_record_round_trips() {
        // The serializing half must match what the derive would emit on its own.
        let input = r#"{"tags":["first","second"]}"#;

        let record = serde_json::from_str::<Record<'_>>(input).unwrap();
        let output = serde_json::to_string(&record).unwrap();

        assert_eq!(output, input);
    }
}
