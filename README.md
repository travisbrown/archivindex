# archivindex

![GitHub last commit][last-commit-badge]
[![build][build-badge]][build]
[![codecov][codecov-badge]][codecov]
[![License: MIT OR Apache-2.0][license-badge]](#license)

Core data models and support crates for Archivindex web archiving and indexing tools.

## Crates

| Crate                                                        | Description                                                                                         |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| [`archivindex-surt`](crates/surt/)                           | [SURT][surt] keys and URL canonicalization for web archives                                         |
| [`archivindex-cdx`](crates/cdx/)                             | Data models for [classic CDX][cdx], [CDXJ][cdxj], and the [Wayback Machine's CDX JSON][wayback-cdx] |
| [`archivindex-digest`](crates/digest/)                       | Fixed-size digest text encoding and decoding                                                        |
| [`archivindex-lines`](crates/lines/)                         | Bounded line-oriented reading with source diagnostics                                               |
| [`archivindex-publication`](crates/publication/)             | File publication with explicit overwrite policies and durability checks                             |
| [`archivindex-serde`](crates/serde/)                         | String deserialization, borrowing, and `FromStr` helpers                                            |
| [`archivindex-cli-support`](crates/cli-support/)             | Command-line options, configuration, logging, progress, and exit statuses                           |
| [`archivindex-test-support`](crates/test-support/)           | Test fixtures, property-testing strategies, and scripted loopback HTTP servers                      |
| [`cargo-archivindex-build`](crates/cargo-archivindex-build/) | Workspace policy checks and configuration synchronization                                           |

## Ecosystem

| Repository                                       | Responsibility                                                               |
| ------------------------------------------------ | ---------------------------------------------------------------------------- |
| `archivindex` (this repository)                  | Shared archive data models and development support                           |
| [`archivindex-warc`][archivindex-warc]           | WARC reading, writing, capture, transformations, and revisit indexing        |
| [`archivindex-wacz`][archivindex-wacz]           | WACZ collections and WARC-to-WACZ packaging                                  |
| [`archivindex-wbm`][archivindex-wbm]             | Wayback Machine queries, downloads, content storage, and snapshot processing |
| [`archivindex-wordpress`][archivindex-wordpress] | WordPress API models and archive capture                                     |

Each repository is a separate Cargo workspace.

## Development

The workspace requires Rust 1.88 or later. Run its tests and build its documentation with:

```console
cargo test --locked --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

## License

Every crate here is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option; see [LICENSE-MIT][license-mit] and
[LICENSE-APACHE][license-apache] for the full texts.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
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
[last-commit-badge]: https://img.shields.io/github/last-commit/travisbrown/archivindex
[license-apache]: LICENSE-APACHE
[license-badge]: https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue
[license-mit]: LICENSE-MIT
[mit]: https://opensource.org/license/mit
[surt]: http://crawler.archive.org/articles/user_manual/glossary.html#surt
[wayback-cdx]: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
