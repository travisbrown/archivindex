# archivindex-publication

Publish complete files with explicit overwrite and durability guarantees.

Writing an archive can take a long time, and the process may fail before the file is complete. This
crate wraps `tempfile` to keep partial output separate from completed files. You can choose whether
to preserve unfinished output and whether publication may replace an existing destination.

A `Publication` owns a temporary file in the destination directory. By default, dropping it before
publication tries to remove that file and leaves the destination alone. When the output is ready,
finish any encoders, flush your buffers, and call `publish`. This synchronizes the file, moves it to
the destination under the chosen `Policy`, and synchronizes the parent directory on Unix. Other
platforms get the file synchronization and move, but no directory synchronization guarantee.

```rust
use std::io::Write;
use archivindex_publication::{Policy, Publication};

let directory = tempfile::tempdir().unwrap();
let output = directory.path().join("result");

let mut pending = Publication::new(&output, Policy::CreateNew).unwrap();
pending.write_all(b"complete contents").unwrap();
pending.publish().unwrap();
```

If `publish` returns `ErrorKind::DirectorySync`, the completed file is already at the destination,
but synchronizing its directory failed. The file stays in place, though its survival after a system
crash is not guaranteed. Errors before the file is moved leave any existing destination untouched.

To keep unfinished output after a parsing error or cancellation, call `retain_partial` before
writing:

```rust
let mut pending = Publication::with_partial_path(
    "foo-12739137.warc",
    "foo-12739137.warc.partial",
    Policy::CreateNew,
)?
.retain_partial();
```

Dropping this writer leaves the partial file in place, including when you return early after an
error or unwind after a panic. The file also stays at its temporary path if synchronization or the
move fails. A successful move puts it at the destination as usual. If you try to create another
partial file at the same path, creation fails without overwriting the retained output.

To handle Ctrl-C gracefully, stop writing and drop the publication without calling `publish`.
Abrupt process termination also leaves the partial file behind. Keeping the file does not flush
external buffers or finish encoders, so it may contain an incomplete record. It also does not
guarantee that the data will survive a system crash or power loss.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
