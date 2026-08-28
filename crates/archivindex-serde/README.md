# archivindex-serde

String deserialization, borrowing, and `FromStr` helpers for [Serde][serde].

When deserializing strings, we can often avoid allocating by borrowing from the input. Serde's
default `Cow<str>` deserialization produces an owned string even when borrowing is possible.
`BorrowableStr` borrows when the deserializer supports it and serializes as a plain string.

```rust
use std::borrow::Cow;
use archivindex_serde::BorrowableStr;

#[derive(serde::Deserialize, serde::Serialize)]
struct Record<'a> {
    #[serde(borrow)]
    name: BorrowableStr<'a>,
}

let record = serde_json::from_str::<Record<'_>>(r#"{"name":"plain"}"#).unwrap();

assert!(matches!(record.name.0, Cow::Borrowed("plain")));
```

The `borrowable_str::option` and `borrowable_str::seq` modules also let you borrow strings inside an
`Option` or a `Vec`, where `#[serde(borrow)]` alone is not enough. The `from_str` helper
deserializes a string using a type's `FromStr` implementation.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
[serde]: https://serde.rs
