//! `serde` helpers for Archivindex projects.
//!
//! [`BorrowableStr`] deserializes strings by borrowing from the input when the deserializer
//! supports it. The [`borrowable_str`] modules apply this behavior to optional strings and string
//! sequences. [`from_str`] deserializes a string through a type's [`FromStr`] implementation.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::{Deserializer, Unexpected, Visitor};

pub mod borrowable_str;

/// A `Cow<str>` that borrows from the deserializer input when possible.
///
/// Serde's stock `Cow` deserialization always produces `Cow::Owned`, even when the input is a slice
/// the value could have borrowed. This wrapper takes the zero-copy path whenever the format offers
/// one. For JSON parsed from a string or byte slice, strings without escape sequences can borrow;
/// reader-based deserialization cannot borrow from its input.
///
/// Use it directly as a field or element type, or through [`borrowable_str::option`] and
/// [`borrowable_str::seq`] for fields that `#[serde(borrow)]` cannot reach into.
///
/// # Examples
///
/// ```
/// use std::borrow::Cow;
/// use archivindex_serde::BorrowableStr;
///
/// let BorrowableStr(value) = serde_json::from_str::<BorrowableStr<'_>>("\"plain\"")?;
/// assert!(matches!(value, Cow::Borrowed("plain")));
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BorrowableStr<'a>(
    /// The deserialized string, borrowed from the input where the format allowed it.
    pub Cow<'a, str>,
);

/// Deserialize a value from a string using its [`FromStr`] implementation.
///
/// `expecting` describes the expected value in errors, for example `"a digest algorithm label"`.
///
/// # Errors
///
/// Returns the deserializer's own error if the input is not a string, or an
/// [`invalid_value`](serde::de::Error::invalid_value) error if [`FromStr`] rejects it. The parse
/// error itself is discarded in favour of `serde`'s message, which names both the offending value
/// and `expecting`.
///
/// # Examples
///
/// ```
/// use std::net::Ipv4Addr;
///
/// fn address<'de, D: serde::de::Deserializer<'de>>(
///     deserializer: D,
/// ) -> Result<Ipv4Addr, D::Error> {
///     archivindex_serde::from_str(deserializer, "an IPv4 address")
/// }
///
/// #[derive(serde::Deserialize)]
/// struct Record {
///     #[serde(deserialize_with = "address")]
///     host: Ipv4Addr,
/// }
///
/// let record = serde_json::from_str::<Record>(r#"{"host":"127.0.0.1"}"#)?;
/// assert_eq!(record.host, Ipv4Addr::LOCALHOST);
/// # Ok::<(), serde_json::Error>(())
/// ```
pub fn from_str<'de, T: FromStr, D: Deserializer<'de>>(
    deserializer: D,
    expecting: &'static str,
) -> Result<T, D::Error> {
    deserializer.deserialize_str(FromStrVisitor {
        expecting,
        value: PhantomData,
    })
}

/// Visits a string and parses it with [`FromStr`], discarding the parse error in favour of
/// `serde`'s own "invalid value" message.
struct FromStrVisitor<T> {
    expecting: &'static str,
    /// The type this visitor parses.
    value: PhantomData<T>,
}

impl<T: FromStr> Visitor<'_> for FromStrVisitor<T> {
    type Value = T;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.expecting)
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        value
            .parse()
            .map_err(|_| serde::de::Error::invalid_value(Unexpected::Str(value), &self))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::num::NonZeroU8;

    use super::BorrowableStr;

    fn count<'de, D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<NonZeroU8, D::Error> {
        super::from_str(deserializer, "a non-zero count")
    }

    #[derive(Debug, serde::Deserialize)]
    struct Counted {
        #[serde(deserialize_with = "count")]
        count: NonZeroU8,
    }

    #[test]
    fn a_string_without_escapes_is_borrowed_from_the_input() {
        let input = "\"borrowed\"";

        let BorrowableStr(value) = serde_json::from_str::<BorrowableStr<'_>>(input).unwrap();

        assert!(matches!(value, Cow::Borrowed("borrowed")));
    }

    #[test]
    fn a_string_with_an_escape_is_owned() {
        // An escape forces the parser to build a new string, so it cannot lend a slice.
        let input = "\"an \\\"escape\\\"\"";

        let BorrowableStr(value) = serde_json::from_str::<BorrowableStr<'_>>(input).unwrap();

        assert_eq!(value, Cow::<'_, str>::Owned("an \"escape\"".to_owned()));
        assert!(matches!(value, Cow::Owned(_)));
    }

    #[test]
    fn a_parsed_value_uses_its_from_str_implementation() {
        let input = r#"{"count":"7"}"#;

        let counted = serde_json::from_str::<Counted>(input).unwrap();

        assert_eq!(counted.count.get(), 7);
    }

    #[test]
    fn a_value_from_str_rejects_names_the_expectation() {
        let input = r#"{"count":"0"}"#;

        let error = serde_json::from_str::<Counted>(input).unwrap_err();

        assert!(
            error.to_string().contains("expected a non-zero count"),
            "unexpected message: {error}"
        );
    }
}
