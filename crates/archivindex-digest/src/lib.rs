//! Encode and decode fixed-size digests as text.
//!
//! Archive formats use different text representations for digests. For example, a CDX index may
//! use Base32 for a SHA-1 digest, while a WACZ manifest uses hexadecimal with a `sha256:` prefix.
//!
//! Implement [`Format`] to choose an encoding and optional prefix, then use [`encode`] and
//! [`decode`] to write and read digest text. Decoding checks the prefix, encoded length, and number
//! of decoded bytes. A format can accept input that differs from its output, such as uppercase
//! letters in a digest that is written as lowercase hexadecimal.
//!
//! Callers are responsible for choosing the hash algorithm and computing the digest bytes.
//!
//! ```
//! use std::str::FromStr;
//!
//! struct CdxDigest;
//!
//! impl archivindex_digest::Format for CdxDigest {
//!     const PREFIX: &'static str = "";
//!     const ENCODING: data_encoding::Encoding = data_encoding::BASE32;
//! }
//!
//! let bytes = archivindex_digest::decode::<CdxDigest, 20>(
//!     "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ",
//! )?;
//!
//! let mut text = String::new();
//! archivindex_digest::encode::<CdxDigest, _>(&bytes, &mut text)?;
//! assert_eq!(text, "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

/// How one kind of digest is written as text.
///
/// A format defines the encoding and prefix. The caller chooses the hash algorithm separately.
pub trait Format {
    /// A label written before the encoded digest, or an empty string if no label is needed.
    ///
    /// [`encode`] writes this prefix and [`decode`] requires it. For example, a WACZ digest with
    /// the prefix `sha256:` must include that label when it is read.
    const PREFIX: &'static str;

    /// The encoding used to write digests.
    ///
    /// [`decode`] also uses this encoding to calculate the expected text length, including padding.
    const ENCODING: data_encoding::Encoding;

    /// The encoding used to read digests. Defaults to [`Self::ENCODING`].
    ///
    /// Override this to accept more input forms. For example, use
    /// [`data_encoding::HEXLOWER_PERMISSIVE`] to accept uppercase letters while writing lowercase
    /// hexadecimal.
    const DECODING: data_encoding::Encoding = Self::ENCODING;
}

/// A digest string does not match the expected format.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// The value does not begin with the format's prefix.
    #[error("missing `{prefix}` digest prefix: {value}")]
    MissingPrefix {
        /// The prefix the format requires.
        prefix: &'static str,
        /// The original input, which lacks the required prefix.
        value: String,
    },
    /// The encoded digest has the wrong length for this format.
    #[error("invalid digest string length: expected {expected}, found `{value}`")]
    InvalidLength {
        /// The expected encoded length in bytes, excluding the prefix.
        expected: usize,
        /// The input without the prefix.
        value: String,
    },
    /// The value is the right length but does not decode to the expected digest bytes.
    #[error("invalid digest encoding: {0}")]
    InvalidEncoding(String),
}

/// Encode `bytes` as text using format `F`, including its prefix.
///
/// The digest is encoded directly into `writer`, without an intermediate `String`.
///
/// # Errors
///
/// Returns whatever error `writer` returns.
pub fn encode<F: Format + ?Sized, W: std::fmt::Write>(
    bytes: &[u8],
    writer: &mut W,
) -> std::fmt::Result {
    writer.write_str(F::PREFIX)?;
    F::ENCODING.encode_write(bytes, writer)
}

/// Decode `input` as an `N`-byte digest using format `F`.
///
/// # Errors
///
/// Returns [`Error::MissingPrefix`] if `input` does not begin with `F`'s prefix,
/// [`Error::InvalidLength`] if the remaining text has the wrong length for an `N`-byte digest, and
/// [`Error::InvalidEncoding`] if it does not decode to exactly `N` bytes.
pub fn decode<F: Format + ?Sized, const N: usize>(input: &str) -> Result<[u8; N], Error> {
    let encoded = input
        .strip_prefix(F::PREFIX)
        .ok_or_else(|| Error::MissingPrefix {
            prefix: F::PREFIX,
            value: input.to_owned(),
        })?;

    let expected = F::ENCODING.encode_len(N);
    if encoded.len() != expected {
        return Err(Error::InvalidLength {
            expected,
            value: encoded.to_owned(),
        });
    }

    let mut bytes = [0; N];
    let decoded = F::DECODING
        .decode_mut(encoded.as_bytes(), &mut bytes)
        .map_err(|_| Error::InvalidEncoding(encoded.to_owned()))?;

    // A padded value can have the expected encoded length but decode to fewer than N bytes.
    if decoded == N {
        Ok(bytes)
    } else {
        Err(Error::InvalidEncoding(encoded.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{Error, Format, decode, encode};

    /// The unpadded uppercase Base32 used for SHA-1 digests in the Wayback Machine's CDX index.
    struct Base32Sha1;

    impl Format for Base32Sha1 {
        const PREFIX: &'static str = "";
        const ENCODING: data_encoding::Encoding = data_encoding::BASE32;
    }

    /// The lowercase hexadecimal with a `sha256:` prefix used in WACZ manifests.
    struct PrefixedSha256;

    impl Format for PrefixedSha256 {
        const PREFIX: &'static str = "sha256:";
        const ENCODING: data_encoding::Encoding = data_encoding::HEXLOWER;
        const DECODING: data_encoding::Encoding = data_encoding::HEXLOWER_PERMISSIVE;
    }

    fn encoded<F: Format>(bytes: &[u8]) -> String {
        let mut text = String::new();
        encode::<F, _>(bytes, &mut text).expect("a String accepts any write");
        text
    }

    #[test]
    fn round_trips_a_known_value() -> Result<(), Box<dyn std::error::Error>> {
        // The Base32 SHA-1 digest of the empty input, as the CDX index writes it.
        const EMPTY: &str = "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ";

        let bytes = decode::<Base32Sha1, 20>(EMPTY)?;

        assert_eq!(encoded::<Base32Sha1>(&bytes), EMPTY);

        Ok(())
    }

    #[test]
    fn a_prefix_is_written_and_required() {
        let text = encoded::<PrefixedSha256>(&[0; 32]);

        assert!(text.starts_with("sha256:"));
        assert_eq!(decode::<PrefixedSha256, 32>(&text), Ok([0; 32]));
        assert_eq!(
            decode::<PrefixedSha256, 32>(text.trim_start_matches("sha256:")),
            Err(Error::MissingPrefix {
                prefix: "sha256:",
                value: text[7..].to_owned(),
            })
        );
    }

    #[test]
    fn permissive_decoding_accepts_input_encoding_never_writes() {
        let text = encoded::<PrefixedSha256>(&[0xab; 32]);

        assert_eq!(
            decode::<PrefixedSha256, 32>(&text.to_uppercase().replace("SHA256", "sha256")),
            Ok([0xab; 32])
        );
    }

    #[test]
    fn the_wrong_length_is_rejected_before_decoding() {
        assert_eq!(
            decode::<Base32Sha1, 20>("AAAA"),
            Err(Error::InvalidLength {
                expected: 32,
                value: "AAAA".to_owned(),
            })
        );
    }

    #[test]
    fn padding_that_shortens_the_digest_is_rejected() {
        // Twenty-four Base32 characters carry fifteen bytes; the padding makes up the length.
        let padded = format!("{}========", "A".repeat(24));

        assert_eq!(
            decode::<Base32Sha1, 20>(&padded),
            Err(Error::InvalidEncoding(padded))
        );
    }

    #[test]
    fn characters_outside_the_alphabet_are_rejected() {
        let outside = "1".repeat(32);

        assert_eq!(
            decode::<Base32Sha1, 20>(&outside),
            Err(Error::InvalidEncoding(outside))
        );
    }

    /// Every digest survives a round trip under both formats, and neither accepts the other's
    /// output.
    #[proptest::property_test]
    fn digests_round_trip_under_their_own_format(bytes: [u8; 20]) {
        let base32 = encoded::<Base32Sha1>(&bytes);
        let hex = encoded::<PrefixedSha256>(&bytes);

        prop_assert_eq!(decode::<Base32Sha1, 20>(&base32), Ok(bytes));
        prop_assert_eq!(decode::<PrefixedSha256, 20>(&hex), Ok(bytes));
        prop_assert!(decode::<PrefixedSha256, 20>(&base32).is_err());
        prop_assert!(decode::<Base32Sha1, 20>(&hex).is_err());
    }
}
