# archivindex-cdx

Data models for [classic CDX][cdx], [CDXJ][cdxj], and the [Wayback Machine's CDX JSON][wayback-cdx].

Web archive indexes describe when a URL was captured and where the archived record can be found.
This crate preserves each format's structure and provides a common entry model for tools that
consume more than one format.

It supports custom classic CDX legends, CDX server pagination, timestamps with second or millisecond
precision, and additional fields. CDXJ entries can accommodate incomplete records or require the
mandatory fields of the CDXJ specification.

[`archivindex-surt`](../archivindex-surt/) provides URL keys and canonicalization;
[`archivindex-lines`](../archivindex-lines/) provides bounded line reading and error locations.
This crate supplies the record models rather than file storage or index searching.

See the [Rustdoc reference](../../README.md#documentation) for parsing, validation, and conversion
examples.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[cdx]: https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
[cdxj]: https://specs.webrecorder.net/cdxj/0.1.0/
[mit]: https://opensource.org/license/mit
[wayback-cdx]: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
