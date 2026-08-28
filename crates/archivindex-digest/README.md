# archivindex-digest

Encode and decode fixed-size digests as text.

Archive formats use different text representations for digests. For example, a CDX index may use
Base32 for a SHA-1 digest, while a WACZ manifest uses hexadecimal with a `sha256:` prefix.

Implement `Format` to choose an encoding and an optional prefix, then use `encode` and `decode` to
write and read digest text. Decoding checks the prefix, the encoded length, and the number of
decoded bytes. A format can accept input that differs from its output, such as uppercase letters
in a digest that is written as lowercase hexadecimal.

Callers are responsible for choosing the hash algorithm and computing the digest bytes.

```rust
struct CdxDigest;

impl archivindex_digest::Format for CdxDigest {
    const PREFIX: &'static str = "";
    const ENCODING: data_encoding::Encoding = data_encoding::BASE32;
}

let bytes = archivindex_digest::decode::<CdxDigest, 20>(
    "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ",
).unwrap();

let mut text = String::new();
archivindex_digest::encode::<CdxDigest, _>(&bytes, &mut text).unwrap();
assert_eq!(text, "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ");
```

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
