# archivindex-serde

String deserialization helpers for [Serde][serde], shared by Archivindex data models.

The borrowing helpers retain references to input strings where the format permits, including
strings inside optional fields and sequences. Serialization remains ordinary string serialization.
This is useful for parsing many records without allocating every text field.

A separate helper connects string deserialization to a type's `FromStr` implementation, with
errors that describe the expected value.

See the [Rustdoc reference](https://docs.rs/archivindex-serde) for field attributes and examples.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
[serde]: https://serde.rs
