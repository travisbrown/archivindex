# archivindex-http

Parse stored HTTP/1 messages and extract their bodies without an HTTP client or async runtime.
The parser reads request and response metadata without changing the original bytes, including
repeated and folded headers found in archived captures.

Body extraction removes chunk framing and trailers while preserving content coding such as gzip.
Strict framing detects incomplete chunked bodies. An optional inference policy also handles
archives whose chunk framing was removed without updating the headers, as in older Common Crawl
captures. Inference can leave incomplete framing untouched or mistake decoded content for chunks.

This crate works on one message at a time; it does not find message boundaries in a network stream
or decompress content. For live capture, use
[`archivindex-http-client`](../archivindex-http-client/), which applies transport limits and records
complete or truncated exchanges.

See the [Rustdoc reference](../../README.md#documentation) for parsing examples and framing
policies.

## License

Licensed under either the [MIT License](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option.
