# archivindex-lines

Read lines with size limits and source locations for errors.

Intended for index files and other line-oriented archive data, this crate pairs each UTF-8 line
with its source name and line number. Parsers can attach a short excerpt to an error without
copying every successful line. Configurable size limits bound memory use for oversized records.

It adds source-aware diagnostics, blank-line handling, and size limits to a buffered reader.
The caller supplies the record parser, for example the models in
[`archivindex-cdx`](../archivindex-cdx/).

See the [Rustdoc reference](../../README.md#documentation) for line-reading and error-reporting
examples.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
