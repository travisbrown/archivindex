# archivindex

![GitHub last commit][last-commit-badge]
[![build][build-badge]][build]
[![codecov][codecov-badge]][codecov]

This repository contains data models and support crates shared by Archivindex's web archiving and
indexing tools.

The crates are pre-1.0, and APIs may change between releases. Check each crate's README for its
scope and license before integrating it.

## Crates

These libraries are published on crates.io:

| Crate                                                        | Description                                                                 | API docs                                           |
| ------------------------------------------------------------ | --------------------------------------------------------------------------- | -------------------------------------------------- |
| [`archivindex-cdx`](crates/archivindex-cdx/)                 | Data models for [CDX][cdx], [CDXJ][cdxj], and [other variants][wayback-cdx] | [docs.rs](https://docs.rs/archivindex-cdx)         |
| [`archivindex-digest`](crates/archivindex-digest/)           | Encode and decode fixed-size digests as text                                | [docs.rs](https://docs.rs/archivindex-digest)      |
| [`archivindex-http`](crates/archivindex-http/)               | HTTP/1 message parsing and body extraction                                  | [docs.rs](https://docs.rs/archivindex-http)        |
| [`archivindex-lines`](crates/archivindex-lines/)             | Read lines with size limits and source locations for errors                 | [docs.rs](https://docs.rs/archivindex-lines)       |
| [`archivindex-publication`](crates/archivindex-publication/) | Publish files with control over overwriting and partial output              | [docs.rs](https://docs.rs/archivindex-publication) |
| [`archivindex-serde`](crates/archivindex-serde/)             | String deserialization, borrowing, and `FromStr` helpers                    | [docs.rs](https://docs.rs/archivindex-serde)       |
| [`archivindex-surt`](crates/archivindex-surt/)               | [SURT][surt] keys and URL canonicalization for web archives                 | [docs.rs](https://docs.rs/archivindex-surt)        |

The remaining crates are not published. They support development of this workspace and the other
Archivindex repositories.

| Crate                                                                            | Description                                                                      |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| [`archivindex-cli-support`](crates/archivindex-cli-support/)                     | Command-line options, configuration, logging, progress, etc.                     |
| [`archivindex-http-client`](crates/archivindex-http-client/)                     | HTTP clients that capture request and response messages                          |
| [`archivindex-http-client-challenge`](crates/archivindex-http-client-challenge/) | Support for HTTP challenges                                                      |
| [`archivindex-test-support`](crates/archivindex-test-support/)                   | Test fixtures, property-testing strategies, and [`wiremock`][wiremock] utilities |
| [`cargo-archivindex-build`](crates/cargo-archivindex-build/)                     | Workspace policy checks and configuration synchronization                        |

## Ecosystem

Other Archivindex repositories use these crates to work with specific archive formats and services.
Each repository is a separate Cargo workspace.

| Repository                                       | Responsibility                                                        |
| ------------------------------------------------ | --------------------------------------------------------------------- |
| [`archivindex-wacz`][archivindex-wacz]           | WACZ data model and packaging tools                                   |
| [`archivindex-warc`][archivindex-warc]           | WARC reading, writing, capture, transformations, and revisit indexing |
| [`archivindex-wbm`][archivindex-wbm]             | Wayback Machine queries, downloads, storage, and snapshot processing  |
| [`archivindex-wordpress`][archivindex-wordpress] | WordPress API data model and archive capture                          |

## Development

The published libraries require Rust 1.88 or later. The HTTP client crates and the build tool
require Rust 1.100 or later, and the HTTP client's optional `wreq` feature also requires C and C++
compilers, CMake, and libclang. See the HTTP client's
[build guide](crates/archivindex-http-client/BUILDING.md) for dependency patches required by
consuming workspaces.

From the repository root, build the workspace with its default features:

```console
cargo build --locked --workspace
```

To run the tests with all optional features enabled:

```console
cargo test --locked --workspace --all-features
```

## Documentation

Each crate's Rustdoc contains API usage, examples, and behavior guarantees. The published crates'
documentation is on docs.rs, linked from the table above. Build the complete reference, including
the unpublished crates, locally with:

```console
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

Open `target/doc/<crate_name>/index.html`, using underscores in place of hyphens in the crate name.
The [build tool usage guide](crates/cargo-archivindex-build/USAGE.md) covers its command-line
interface.

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
[license-mit]: LICENSE-MIT
[mit]: https://opensource.org/license/mit
[surt]: http://crawler.archive.org/articles/user_manual/glossary.html#surt
[wayback-cdx]: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
[wiremock]: https://docs.rs/wiremock/latest/wiremock/
