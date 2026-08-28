# archivindex-lines

Read lines with size limits and source locations for errors.

When parsing an index file, we often need to report which file and line caused an error. `Lines`
keeps track of this information while reading UTF-8 lines. It limits the size of each line and
removes trailing CR and LF bytes. Blank lines are skipped by default, or you can reject them with
`reject_blank_lines`. Lines containing other whitespace are not considered blank.

Each successful read returns the line's content, source name, and line number, borrowing from the
reader. If parsing fails, use `LineContextRef::into_owned` to keep the location and a short excerpt
for an error message.

`Error` distinguishes I/O failures from invalid content. You can convert it to `std::io::Error`
when your API uses that error type.

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
