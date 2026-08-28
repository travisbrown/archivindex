# archivindex-lines

Line-oriented reading with bounded lines and source diagnostics.

`Lines` reads UTF-8 lines with a configurable size limit, strips trailing CR and LF bytes, and
tracks line numbers. It skips blank lines by default and can instead reject them. Successful reads
return borrowed context; `LineContextRef::into_owned` creates an owned location with a bounded
excerpt.

`Error` distinguishes a failed read from content the reader rejected, and converts into an
`std::io::Error` for callers that expose only those.

```rust
let mut lines =
    archivindex_lines::Lines::with_source(&b"first\r\n\nsecond\n"[..], "test.jsonl");

let context = lines.next_content().unwrap().expect("the first line");
assert_eq!((context.line, context.content), (1, "first"));

// The blank line is skipped but still counted.
let context = lines.next_content().unwrap().expect("the third line");
assert_eq!((context.line, context.content), (3, "second"));
assert_eq!(lines.next_content().unwrap(), None);
```

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
