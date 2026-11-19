# cargo-archivindex-build

Check and synchronize Cargo workspace policy across Archivindex projects.

The tool keeps package metadata, dependency inheritance, lints, formatter settings, and dependency
policy consistent across separate repositories. `check` reports violations; `sync` applies supported
fixes and reports what still needs attention. Package-specific exemptions cover intentional
differences in authorship, licensing, lints, and dependencies.

This encodes Archivindex's conventions rather than providing a general policy language. It
configures rustfmt, Clippy, Taplo, rumdl, and cargo-deny; those tools still perform the actual
checks.

## Try it

From the repository root, check this workspace without changing files:

```console
cargo run --locked -p cargo-archivindex-build -- check
```

Add `--manifest-path /path/to/Cargo.toml` after `check` to inspect another workspace.
Replace `check` with `sync` to apply automatic fixes. See the [usage guide](USAGE.md) for
installation as a Cargo subcommand, enforced rules, and exemptions.

The tool also has a Rust library interface, described in [Rustdoc](../../README.md#documentation).

## License

Licensed under either the [MIT License][mit] or the [Apache License, Version 2.0][apache-2.0], at
your option; see [LICENSE-MIT][license-mit] and [LICENSE-APACHE][license-apache] for the full
texts.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[license-apache]: LICENSE-APACHE
[license-mit]: LICENSE-MIT
[mit]: https://opensource.org/license/mit
