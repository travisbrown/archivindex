//! Shared header boundaries and the distinct validation policies of metadata and body extraction.

use archivindex_http::body::{self, Error, Framing};
use archivindex_http::message::{RequestMetadata, ResponseMetadata};

/// Metadata and body extraction agree on boundaries even with mixed line endings, folded and
/// repeated fields, and binary values. Header-like bytes in the body are not scanned as fields.
#[test]
fn metadata_and_bodies_share_head_boundaries() {
    let fields = b"X-Binary: \xff\nTransfer-Encoding: identity,\r\n\tchunked\n\
                   Transfer-Encoding: identity\r\n\r\n";
    let encoded = b"4\r\nX: y\r\n0\r\n\r\n";

    for start in [b"GET / HTTP/1.1\n".as_slice(), b"HTTP/1.1 200 OK\r\n"] {
        let head = [start, fields].concat();
        let message = [head.as_slice(), encoded].concat();
        let offset = if start.starts_with(b"GET") {
            let metadata = RequestMetadata::parse(&message).unwrap();
            assert_eq!(metadata.header("x-binary"), Some(b"\xff".as_slice()));
            assert_eq!(
                metadata
                    .combined_header("transfer-encoding")
                    .unwrap()
                    .as_deref(),
                Some("identity, chunked, identity")
            );
            metadata.body_offset
        } else {
            let metadata = ResponseMetadata::parse(&message).unwrap();
            assert_eq!(metadata.header("x-binary"), Some(b"\xff".as_slice()));
            assert_eq!(
                metadata
                    .combined_header("transfer-encoding")
                    .unwrap()
                    .as_deref(),
                Some("identity, chunked, identity")
            );
            metadata.body_offset
        };
        assert_eq!(offset, head.len());
        assert_eq!(body::message_body(&message).unwrap(), &message[offset..]);
        for policy in [Framing::Declared, Framing::Inferred] {
            assert_eq!(
                body::entity_body_with_policy(&message, policy)
                    .unwrap()
                    .as_ref(),
                b"X: y"
            );
        }

        // No prefix ending before the header terminator provides a body boundary.
        for end in 0..head.len() {
            let prefix = &head[..end];
            assert!(RequestMetadata::parse(prefix).is_none(), "{prefix:?}");
            assert!(ResponseMetadata::parse(prefix).is_none(), "{prefix:?}");
            assert_eq!(body::message_body(prefix), Err(Error::UnterminatedHeaders));
            assert_eq!(body::entity_body(prefix), Err(Error::UnterminatedHeaders));
        }
    }
}

/// Body extraction tolerates malformed fields and whitespace before colons, while metadata
/// rejects them. An orphan continuation must not become a Transfer-Encoding field.
#[test]
fn metadata_and_bodies_keep_their_validation_policies() {
    for fields in [
        b" Transfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n".as_slice(),
        b"Bad Name: gzip\r\nTransfer-Encoding: chunked\r\n",
        b"missing-colon\r\nTransfer-Encoding: chunked\r\n",
        b"Transfer-Encoding \t: chunked\r\n",
    ] {
        for start in [b"GET / HTTP/1.1\r\n".as_slice(), b"HTTP/1.1 200 OK\r\n"] {
            let encoded = b"3\r\nabc\r\n0\r\n\r\n";
            let message = [start, fields, b"\r\n", encoded].concat();
            assert!(RequestMetadata::parse(&message).is_none());
            assert!(ResponseMetadata::parse(&message).is_none());
            assert_eq!(body::message_body(&message).unwrap(), encoded);
            assert_eq!(body::entity_body(&message).unwrap().as_ref(), b"abc");
        }
    }
}

/// Continuations belong only to the immediately preceding field, even when that line is ignored.
#[test]
fn unrelated_or_invalid_fields_end_transfer_encoding_continuations() {
    for line in [
        "X-Test: ignored",
        "no colon",
        "Transfer-Encoding\u{7f}: gzip",
    ] {
        let message = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n{line}\r\n\
             \tgzip\r\n\r\n3\r\nabc\r\n0\r\n\r\n"
        );
        assert_eq!(
            body::entity_body(message.as_bytes()).unwrap().as_ref(),
            b"abc"
        );
    }
}

/// Extraction reports unsupported transfer codings with their original internal whitespace.
#[test]
fn transfer_encoding_errors_preserve_folded_value_bytes() {
    let message = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip,\r\n\
                    \tchunked\r\nTransfer-Encoding: identity\r\n\r\n";
    assert_eq!(
        body::entity_body(message),
        Err(Error::UnsupportedTransferEncoding(
            "gzip, \tchunked, identity".to_owned()
        ))
    );
}
