# archivindex-cdx

Data models for [classic CDX][cdx], [CDXJ][cdxj], and the [Wayback Machine's CDX JSON][wayback-cdx].

Each format has its own model under `format`, and all three convert to the
representation-neutral `Capture`. Parsing borrows from the input, and `into_owned` detaches the
result.

```rust
use archivindex_cdx::format::cdxj::Item;

let item = Item::parse(
    r#"com,example)/ 20201007212236 {"url":"https://example.com/","status":"200"}"#,
).unwrap();

assert_eq!(item.key, "com,example)/");
assert_eq!(item.timestamp.to_string(), "20201007212236");
assert_eq!(item.fields.status, Some(200));
```

Reading files, sorting indexes, looking up captures, and resolving WARC byte ranges belong to
higher-level crates.

## Features

| Feature          | Default | Description                                           |
| ---------------- | ------- | ----------------------------------------------------- |
| `bounded-static` | no      | `ToBoundedStatic` and `IntoBoundedStatic` conversions |

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[cdx]: https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
[cdxj]: https://specs.webrecorder.net/cdxj/0.1.0/
[mit]: https://opensource.org/license/mit
[wayback-cdx]: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
