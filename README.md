# archivindex

![GitHub last commit][last-commit-badge]
[![build][build-badge]][build]
[![codecov][codecov-badge]][codecov]
[![License: MIT OR Apache-2.0][license-badge]](#license)

This repository contains data models and support crates shared by Archivindex's web archiving and
indexing tools.

## Crates

| Crate                                                                            | Description                                                                                         |
| -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| [`archivindex-cdx`](crates/archivindex-cdx/)                                     | Data models for [classic CDX][cdx], [CDXJ][cdxj], and the [Wayback Machine's CDX JSON][wayback-cdx] |
| [`archivindex-cli-support`](crates/archivindex-cli-support/)                     | Command-line options, configuration, logging, progress, and exit statuses                           |
| [`archivindex-digest`](crates/archivindex-digest/)                               | Encode and decode fixed-size digests as text                                                        |
| [`archivindex-http`](crates/archivindex-http/)                                   | HTTP/1 message parsing and body extraction                                                          |
| [`archivindex-http-client`](crates/archivindex-http-client/)                     | HTTP clients that capture request and response messages                                             |
| [`archivindex-http-client-challenge`](crates/archivindex-http-client-challenge/) | Recognition and bounded solutions for HTTP challenges                                               |
| [`archivindex-lines`](crates/archivindex-lines/)                                 | Read lines with size limits and source locations for errors                                         |
| [`archivindex-publication`](crates/archivindex-publication/)                     | Publish completed files with control over overwriting and partial output                            |
| [`archivindex-serde`](crates/archivindex-serde/)                                 | String deserialization, borrowing, and `FromStr` helpers                                            |
| [`archivindex-surt`](crates/archivindex-surt/)                                   | [SURT][surt] keys and URL canonicalization for web archives                                         |
| [`archivindex-test-support`](crates/archivindex-test-support/)                   | Test fixtures, property-testing strategies, and [`wiremock`][wiremock] utilities                    |
| [`cargo-archivindex-build`](crates/cargo-archivindex-build/)                     | Workspace policy checks and configuration synchronization                                           |

## Ecosystem

Other Archivindex repositories use these crates to work with specific archive formats and services.
Each repository is a separate Cargo workspace.

| Repository                                       | Responsibility                                                        |
| ------------------------------------------------ | --------------------------------------------------------------------- |
| [`archivindex-wacz`][archivindex-wacz]           | WACZ representations and packaging                                    |
| [`archivindex-warc`][archivindex-warc]           | WARC reading, writing, capture, transformations, and revisit indexing |
| [`archivindex-wbm`][archivindex-wbm]             | Wayback Machine queries, downloads, storage, and snapshot processing  |
| [`archivindex-wordpress`][archivindex-wordpress] | WordPress API models and archive capture                              |

## Development

The workspace requires Rust 1.98 or later. The HTTP client's optional `wreq` feature also requires
C and C++ compilers, CMake, and libclang. Run all tests and build the documentation with:

```console
cargo test --locked --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

## License

The HTTP client crates are licensed under the [GNU General Public License, version 3][gpl-3.0]; see
their local `LICENSE` files. All other crates are licensed under either the [MIT License][mit] or
the [Apache License, Version 2.0][apache-2.0], at your option; see [LICENSE-MIT][license-mit] and
[LICENSE-APACHE][license-apache] for the full texts.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[archivindex]: https://github.com/travisbrown/archivindex
[archivindex-wacz]: https://github.com/travisbrown/archivindex-wacz
[archivindex-warc]: https://github.com/travisbrown/archivindex-warc
[archivindex-wbm]: https://github.com/travisbrown/archivindex-wbm
[archivindex-wordpress]: https://github.com/travisbrown/archivindex-wordpress
[build]: https://github.com/travisbrown/archivindex/actions/workflows/ci.yml
[build-badge]: https://github.com/travisbrown/archivindex/actions/workflows/ci.yml/badge.svg
[cdx]: https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
[cdxj]: https://specs.webrecorder.net/cdxj/0.1.0/
[codecov]: https://codecov.io/gh/travisbrown/archivindex
[codecov-badge]: https://codecov.io/gh/travisbrown/archivindex/branch/main/graph/badge.svg
[gpl-3.0]: https://www.gnu.org/licenses/gpl-3.0.html
[last-commit-badge]: https://img.shields.io/github/last-commit/travisbrown/archivindex
[license-apache]: LICENSE-APACHE
[license-badge]: https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue
[license-mit]: LICENSE-MIT
[mit]: https://opensource.org/license/mit
[surt]: http://crawler.archive.org/articles/user_manual/glossary.html#surt
[wayback-cdx]: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
[wiremock]: https://docs.rs/wiremock/latest/wiremock/
