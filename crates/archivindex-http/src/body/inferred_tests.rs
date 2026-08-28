use super::{Error, Framing, entity_body_with_policy};

fn entity_body(message: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>, Error> {
    entity_body_with_policy(message, Framing::Inferred)
}

/// Without transfer-coding, the bytes after the header section are the entity-body.
#[test]
fn body_of_an_unencoded_message() {
    let message = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";

    assert_eq!(entity_body(message).unwrap().as_ref(), b"hello");
}

/// A message ending after its headers has an empty entity-body.
#[test]
fn body_of_a_message_without_one() {
    let message = b"HTTP/1.1 204 No Content\r\n\r\n";

    assert_eq!(entity_body(message).unwrap().as_ref(), b"");
}

/// HTTP messages accept bare `LF` line endings; WARC headers require `CRLF`.
#[test]
fn body_of_a_message_written_with_bare_line_feeds() {
    let message = b"HTTP/1.1 200 OK\nContent-Length: 5\n\nhello";

    assert_eq!(entity_body(message).unwrap().as_ref(), b"hello");
}

/// A header section without a closing empty line has no identifiable body.
#[test]
fn unterminated_headers() {
    for message in [
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\n".as_slice(),
        b"".as_slice(),
    ] {
        assert_eq!(
            entity_body(message).unwrap_err(),
            Error::UnterminatedHeaders,
            "{message:?}"
        );
    }
}

/// Chunk sizes, extensions, and trailers are framing and are removed.
#[test]
fn chunked_body_is_joined() {
    let message = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Transfer-Encoding: chunked\r\n",
        "\r\n",
        "5;name=value\r\n",
        "hello\r\n",
        "2\r\n",
        " w\r\n",
        "5\r\n",
        "orld!\r\n",
        "0\r\n",
        "Expires: Wed, 21 Oct 2026 07:28:00 GMT\r\n",
        "\r\n",
    );

    assert_eq!(
        entity_body(message.as_bytes()).unwrap().as_ref(),
        b"hello world!"
    );
}

/// A chunked body may end immediately after its zero-length chunk or its trailer fields, and
/// may be followed by extra line endings.
#[test]
fn chunked_body_with_missing_or_padded_trailers() {
    let prefix = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n";

    for trailers in [
        "",
        "X: y\r\n",
        "X: y\nZ : w\n",
        "\r\n\r\n",
        "X: y\r\n\r\n\n",
    ] {
        let message = [prefix.as_bytes(), trailers.as_bytes()].concat();

        assert_eq!(
            entity_body(&message).unwrap().as_ref(),
            b"abc",
            "{trailers:?}"
        );
    }
}

/// The `identity` coding leaves the body unchanged.
#[test]
fn identity_coding_is_dropped() {
    let message = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: Identity\r\n\r\nhello";

    assert_eq!(entity_body(message).unwrap().as_ref(), b"hello");
}

/// Repeated and folded `Transfer-Encoding` fields form one comma-separated list.
#[test]
fn transfer_encoding_is_read_as_one_list() {
    let message = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Transfer-Encoding: identity,\r\n",
        "\tchunked\r\n",
        "Transfer-Encoding: identity\r\n",
        "\r\n",
        "3\r\nabc\r\n0\r\n\r\n",
    );

    assert_eq!(entity_body(message.as_bytes()).unwrap().as_ref(), b"abc");
}

/// An unsupported coding is reported with the complete value that named it.
#[test]
fn unsupported_transfer_encoding() {
    let message = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\nhello";

    assert_eq!(
        entity_body(message).unwrap_err(),
        Error::UnsupportedTransferEncoding("gzip, chunked".to_owned())
    );
}

/// Content-coding is part of the entity-body and remains unchanged.
#[test]
fn content_coding_is_left_in_place() {
    let message = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n\x1f\x8b\x08";

    assert_eq!(entity_body(message).unwrap().as_ref(), b"\x1f\x8b\x08");
}

/// Archive records can contain dechunked bodies with the original `Transfer-Encoding` header.
/// Inferred framing preserves any body that is not entirely chunk framing. Text whose first line
/// happens to be hexadecimal must not be decoded, and must not be truncated at a line reading `0`.
/// The cost is that malformed or truncated chunk framing is also returned unchanged.
#[test]
fn body_declared_chunked_that_is_not_chunk_framing_is_returned_unchanged() {
    let prefix = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: -1\r\n\r\n";

    for body in [
        "<!doctype html>\r\n<title>a</title>",
        "",
        // Text that opens like chunk framing.
        "0\nzero is the first line of this text\n",
        "0\r\n\r\nThe first line is zero.",
        "2024\nreport",
        "cafe\nlatte and more",
        // A chunk-size line that is not one.
        "zz\r\nabc\r\n0\r\n\r\n",
        "3\r\nabc\r\nzz\r\ndef\r\n0\r\n\r\n",
        // The body ends inside the data of a chunk.
        "5\r\nabc",
        // The data of a chunk is not closed by a line ending.
        "3\r\nabcdef\r\n0\r\n\r\n",
        // No chunk closes the body.
        "3\r\nabc\r\n",
        // A trailer line is not a field, or is incomplete.
        "3\r\nabc\r\n0\r\nnot a field\r\n\r\n",
        "3\r\nabc\r\n0\r\nX: y",
    ] {
        let message = [prefix.as_bytes(), body.as_bytes()].concat();

        assert_eq!(
            entity_body(&message).unwrap().as_ref(),
            body.as_bytes(),
            "{body:?}"
        );
    }
}
