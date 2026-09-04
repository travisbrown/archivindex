//! Text encodings shared by fixed-size digest newtypes.
//!
//! Implement [`Format`] to choose an encoding and optional prefix, then use [`encode`] and
//! [`decode`] to write and read digest text. Decoding checks the prefix, encoded length, and
//! decoded byte count. A format can accept a broader alphabet than it writes, such as uppercase
//! input for lowercase hexadecimal output.
//!
//! Callers choose the hash algorithm and compute the digest bytes.
//!
//! ```
//! use std::str::FromStr;
//!
//! struct CdxDigest;
//!
//! impl archivindex_digest::Format for CdxDigest {
//!     const PREFIX: &'static str = "";
//!
//!     fn encoding() -> data_encoding::Encoding {
//!         data_encoding::BASE32
//!     }
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

use std::fmt;

/// How one kind of digest is written as text.
///
/// A format defines the encoding and prefix, independently of the hash algorithm. Implementations
/// typically use a zero-sized marker type.
pub trait Format {
    /// A fixed label written before the encoded digest, empty when the format has none.
    ///
    /// [`decode`] requires the prefix and [`encode`] writes it, so a format that labels its
    /// algorithm (as WACZ does with `sha256:`) never silently accepts an unlabelled value.
    const PREFIX: &'static str;

    /// The encoding digests are written in.
    ///
    /// This also fixes the encoded length [`decode`] accepts, so it must be the exact encoding of
    /// the written form, including whether it pads.
    #[must_use]
    fn encoding() -> data_encoding::Encoding;

    /// The encoding digests are read in, which defaults to the one they are written in.
    ///
    /// Override this to accept input the written form never produces, such as
    /// [`data_encoding::HEXLOWER_PERMISSIVE`] for a format written as lowercase hexadecimal.
    #[must_use]
    fn decoding() -> data_encoding::Encoding {
        Self::encoding()
    }
}

/// A digest string does not match the format it was read under.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ParseError {
    /// The value does not begin with the format's prefix.
    #[error("missing `{prefix}` digest prefix: {value}")]
    MissingPrefix {
        /// The prefix the format requires.
        prefix: &'static str,
        /// The original input, which lacks the required prefix.
        value: String,
    },
    /// The encoded digest is not the length the format writes.
    #[error("invalid digest string length: expected {expected}, found `{value}`")]
    InvalidLength {
        /// The encoded byte length, excluding the prefix.
        expected: usize,
        /// The value as it was read, without the prefix.
        value: String,
    },
    /// The value is the right length but does not decode to the expected digest bytes.
    #[error("invalid digest encoding: {0}")]
    InvalidEncoding(String),
}

/// Write `bytes` in `F`'s encoding, behind `F`'s prefix.
///
/// The digest is encoded directly into `writer`, without an intermediate `String`.
///
/// # Errors
///
/// Returns whatever error `writer` returns.
pub fn encode<F: Format + ?Sized, W: fmt::Write>(bytes: &[u8], writer: &mut W) -> fmt::Result {
    writer.write_str(F::PREFIX)?;
    F::encoding().encode_write(bytes, writer)
}

/// Read the `N` digest bytes `input` encodes under `F`.
///
/// # Errors
///
/// Returns [`ParseError::MissingPrefix`] if `input` does not begin with `F`'s prefix,
/// [`ParseError::InvalidLength`] if the rest is not as long as `F` writes an `N`-byte digest, and
/// [`ParseError::InvalidEncoding`] if it does not decode to exactly `N` bytes.
pub fn decode<F: Format + ?Sized, const N: usize>(input: &str) -> Result<[u8; N], ParseError> {
    let encoded = input
        .strip_prefix(F::PREFIX)
        .ok_or_else(|| ParseError::MissingPrefix {
            prefix: F::PREFIX,
            value: input.to_owned(),
        })?;

    let expected = F::encoding().encode_len(N);
    if encoded.len() != expected {
        return Err(ParseError::InvalidLength {
            expected,
            value: encoded.to_owned(),
        });
    }

    let mut bytes = [0; N];
    let decoded = F::decoding()
        .decode_mut(encoded.as_bytes(), &mut bytes)
        .map_err(|_| ParseError::InvalidEncoding(encoded.to_owned()))?;

    // A padded value can have the expected encoded length but decode to fewer than N bytes.
    if decoded == N {
        Ok(bytes)
    } else {
        Err(ParseError::InvalidEncoding(encoded.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{Format, ParseError, decode, encode};

    /// The unpadded uppercase Base32 the Wayback Machine's CDX index writes SHA-1 digests in.
    struct Base32Sha1;

    impl Format for Base32Sha1 {
        const PREFIX: &'static str = "";

        fn encoding() -> data_encoding::Encoding {
            data_encoding::BASE32
        }
    }

    /// The labelled lowercase hexadecimal WACZ manifests write SHA-256 digests in.
    struct PrefixedSha256;

    impl Format for PrefixedSha256 {
        const PREFIX: &'static str = "sha256:";

        fn encoding() -> data_encoding::Encoding {
            data_encoding::HEXLOWER
        }

        fn decoding() -> data_encoding::Encoding {
            data_encoding::HEXLOWER_PERMISSIVE
        }
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
            Err(ParseError::MissingPrefix {
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
            Err(ParseError::InvalidLength {
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
            Err(ParseError::InvalidEncoding(padded))
        );
    }

    #[test]
    fn characters_outside_the_alphabet_are_rejected() {
        let outside = "1".repeat(32);

        assert_eq!(
            decode::<Base32Sha1, 20>(&outside),
            Err(ParseError::InvalidEncoding(outside))
        );
    }

    /// Every digest survives a round trip under both formats, and neither accepts the other's
    /// output.
    #[test_strategy::proptest]
    fn digests_round_trip_under_their_own_format(bytes: [u8; 20]) {
        let base32 = encoded::<Base32Sha1>(&bytes);
        let hex = encoded::<PrefixedSha256>(&bytes);

        prop_assert_eq!(decode::<Base32Sha1, 20>(&base32), Ok(bytes));
        prop_assert_eq!(decode::<PrefixedSha256, 20>(&hex), Ok(bytes));
        prop_assert!(decode::<PrefixedSha256, 20>(&base32).is_err());
        prop_assert!(decode::<Base32Sha1, 20>(&hex).is_err());
    }
}
