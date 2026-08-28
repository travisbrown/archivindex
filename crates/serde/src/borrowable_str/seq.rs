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

/// The largest element count a size hint may reserve for on its own.
///
/// A self-describing format's length prefix is part of its input, so a hostile document can claim
/// any length it likes. Beyond this bound the vector grows as elements actually arrive.
const CAPACITY_HINT_LIMIT: usize = 10_000;

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
            let hint = sequence.size_hint().unwrap_or_default();
            let mut values = Vec::with_capacity(hint.min(CAPACITY_HINT_LIMIT));

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

    use serde::de::value::Error;
    use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};

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

    /// An empty sequence that claims to hold `hint` elements, standing in for a format whose
    /// length prefix comes from the input.
    struct Claiming {
        hint: usize,
    }

    impl<'de> SeqAccess<'de> for Claiming {
        type Error = Error;

        fn next_element_seed<T: DeserializeSeed<'de>>(
            &mut self,
            _seed: T,
        ) -> Result<Option<T::Value>, Self::Error> {
            Ok(None)
        }

        fn size_hint(&self) -> Option<usize> {
            Some(self.hint)
        }
    }

    impl<'de> Deserializer<'de> for Claiming {
        type Error = Error;

        fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            visitor.visit_seq(self)
        }

        fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
            Err(serde::de::Error::custom("only a sequence"))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
            option unit unit_struct newtype_struct tuple tuple_struct map struct enum identifier
            ignored_any
        }
    }

    /// A hint is a suggestion, so a document claiming an implausible length must not reserve for
    /// it: `Vec::with_capacity(usize::MAX)` would abort the process before an element arrived.
    #[test]
    fn an_implausible_size_hint_is_not_trusted() {
        let values = super::deserialize(Claiming { hint: usize::MAX }).unwrap();

        assert!(values.is_empty());
        assert!(values.capacity() <= super::CAPACITY_HINT_LIMIT);
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
