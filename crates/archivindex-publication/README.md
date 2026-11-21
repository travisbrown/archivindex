# archivindex-publication

Publish completed files with control over overwriting, unfinished output, and durability.

Designed for archive writers and other long-running exports, this crate keeps partial output
separate from the destination. Publication can refuse to overwrite existing output or replace it
atomically. Interrupted work can be retained at a predictable path for inspection or recovery.

Files are synchronized before publication, and the parent directory is synchronized on Unix.
Errors distinguish failures before publication from failures after the completed file is already
visible. These guarantees complement the temporary-file handling supplied by `tempfile`.

The caller remains responsible for finishing encoders, flushing external buffers, and handling
interrupts. Retaining a partial file alone does not make it durable across a crash.

See the [Rustdoc reference](https://docs.rs/archivindex-publication) for examples, overwrite
policies, and failure handling.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: https://opensource.org/license/mit
