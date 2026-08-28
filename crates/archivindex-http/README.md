# archivindex-http

HTTP/1 message parsing and body extraction, without an HTTP client or async runtime.

`message::RequestMetadata` and `message::ResponseMetadata` parse start lines and headers, locate
body offsets, and expose repeated or combined header values. They leave the input bytes unchanged
and borrow from them, copying a header value only to unfold its continuation lines.
Combined values return `Ok(None)` for an absent field and a UTF-8 error for an unreadable value.

`body::message_body` returns the bytes after the head unchanged, including any chunk framing and
trailers. The caller supplies one message; the function does not enforce `Content-Length` or check
whether a body is permitted. `body::entity_body` removes chunk framing and trailers while preserving
content coding. It uses `Framing::Declared`, requiring complete chunk framing when chunked encoding
is declared. `body::entity_body_with_policy` also supports `Framing::Inferred`. For a body declared
chunked, it decodes chunks if the body opens with a complete chunk-size line; otherwise it assumes
the body has already been dechunked and returns it unchanged. It also accepts a final zero-size
chunk without trailers. Callers must account for request methods and response statuses that forbid a
body.

Only chunked transfer decoding is supported. `Transfer-Encoding: gzip, chunked` is valid HTTP but
requires gzip decompression after dechunking, so both policies reject it. `Content-Encoding: gzip`
with `Transfer-Encoding: chunked` is supported and leaves the gzip content coding intact.

Inferred framing accommodates records such as older Common Crawl ARC and WARC captures, which
retained `Transfer-Encoding: chunked` after removing chunk framing. Common Crawl documents this in
its [archive conversion notes][common-crawl-headers].

[common-crawl-headers]: https://github.com/commoncrawl/arc2warc-conversion#required-rewriting-of-http-headers

```rust
use archivindex_http::{body, message::ResponseMetadata};

let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
let metadata = ResponseMetadata::parse(response).unwrap();
assert_eq!(metadata.status, 200);
assert_eq!(body::entity_body(response).unwrap().as_ref(), b"hello");
```

## License

Licensed under either the [MIT License](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option.
