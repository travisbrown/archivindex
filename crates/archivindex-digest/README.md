# archivindex-digest

Encode and decode fixed-size digests as text for archive formats.

A CDX index may store a SHA-1 digest as Base32, while a WACZ manifest uses hexadecimal with a
`sha256:` prefix. This crate handles those format choices independently of the hash algorithm.
It checks prefixes and lengths when decoding and can accept more input forms than it writes,
such as uppercase hexadecimal input with lowercase output.

Applications define their own formats and compute digest bytes with their chosen hashing library.
The encoding layer uses `data-encoding` and can write directly to a text formatter.

See the [Rustdoc reference](https://docs.rs/archivindex-digest) for defining a format and encoding
or decoding digests.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
