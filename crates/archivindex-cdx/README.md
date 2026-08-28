# archivindex-cdx

Data models for [classic CDX][cdx], [CDXJ][cdxj], and the [Wayback Machine's CDX JSON][wayback-cdx].

Web archive indexes describe when a URL was captured and where the archived record can be found.
This crate provides a model for each format under `format`, along with a shared `Entry` model
that all three formats can convert to. Format-specific entries preserve each format's structure;
the shared entry exposes common fields independently of the source format.

Parsed values borrow strings from the input where possible. Import
`bounded_static::IntoBoundedStatic` and use `into_static` to keep a value after the input is
dropped. Use `bounded_static::ToBoundedStatic::to_static` to obtain an owned copy without consuming
the value.

```rust
use archivindex_cdx::format::cdxj::Entry;

let entry = Entry::parse(
    r#"com,example)/ 20201007212236 {"url":"https://example.com/","status":"200"}"#,
).unwrap();

assert_eq!(entry.key, "com,example)/");
assert_eq!(entry.timestamp.to_string(), "20201007212236");
assert_eq!(entry.fields.status, Some(200));
```

Use `entry.validate()?` to check the key and mandatory CDXJ fields without cloning.
For a field object alone, use `fields.validate()?`. Successful checks do not allocate.
To retain a conforming entry, `ConformingEntry::try_from(entry)?` moves its owned strings and
extension properties. Passing `&entry` instead validates first, then clones the data.

Serialize a CDXJ entry with `entry.to_line()?`. This checks the key, rejects extension properties
that duplicate standard fields, and returns any serialization error. `ConformingEntry` requires
all mandatory JSON fields, but its editable key is still checked when writing the line.

Other crates handle reading index files, sorting them, looking up entries, and using WARC record
offsets and lengths to read archived data.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[cdx]: https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
[cdxj]: https://specs.webrecorder.net/cdxj/0.1.0/
[mit]: https://opensource.org/license/mit
[wayback-cdx]: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
