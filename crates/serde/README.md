# archivindex-serde

String deserialization, borrowing, and `FromStr` helpers for [Serde][serde].

Serde's own `Cow<str>` deserialization always allocates, even when the input is a slice the value
could have borrowed. `BorrowableStr` takes the zero-copy path whenever the format offers one, and
serializes as the string itself.

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

The `borrowable_str::option` and `borrowable_str::seq` modules apply the same behavior to strings
behind an `Option` or a `Vec`, which `#[serde(borrow)]` cannot reach into on its own. `from_str`
deserializes a string through a type's `FromStr` implementation.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
[serde]: https://serde.rs
