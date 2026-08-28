# archivindex-publication

Publish complete files with explicit overwrite and durability guarantees.

A `Publication` owns a temporary file in the destination directory. Dropping it before publication
removes that file on a best-effort basis and leaves the destination alone. Callers finish their
encoders and flush their buffers, then `publish` synchronizes the file, persists it under the
chosen `Policy`, and synchronizes the parent directory on Unix.

```rust
use std::io::Write;
use archivindex_publication::{Policy, Publication};

let directory = tempfile::tempdir().unwrap();
let output = directory.path().join("result");

let mut pending = Publication::new(&output, Policy::CreateNew).unwrap();
pending.write_all(b"complete contents").unwrap();
pending.publish().unwrap();
```

`Error::DirectorySync` means publication succeeded but durability could not be confirmed, and the
published file is retained. Every failure before publication leaves an existing destination
untouched.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
