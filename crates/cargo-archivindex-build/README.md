# cargo-archivindex-build

`cargo-archivindex-build` keeps Cargo workspace policy and formatter configuration consistent
across Archivindex projects.

Install it from a clone of this repository, then run it from any workspace:

```console
cargo install --locked --path crates/cargo-archivindex-build
cargo archivindex-build check
cargo archivindex-build sync
```

Both commands accept `--manifest-path <PATH>`. Use `check` to report policy violations; it exits
with a failure status if it finds any. Use `sync` to apply automatic fixes and then run the same
checks. You must supply project-specific values, such as the repository URL, in
`[workspace.package]`.

The enforced policy includes the following requirements:

- Cargo resolver version 3;
- shared workspace package metadata (`authors`, `repository`, `edition`, `rust-version`,
  `license`, and `version`);
- the Archivindex Rust and Clippy workspace lint configuration;
- workspace lint and package metadata inheritance in all member packages, including root packages;
- a sorted `[workspace.dependencies]` table with no unused entries, and members inheriting any
  dependencies declared there;
- a description, a package-specific `readme` path, and docs.rs metadata for packages that can be
  published to a registry;
- the shared `rustfmt.toml`, `.taplo.toml`, and `.rumdl.toml` settings;
- shared `deny.toml` settings for dependency checks.

The tool checks how `rustfmt`, Clippy, Taplo, rumdl, and `cargo deny` are configured. You still need
to run those tools to check formatting, code, documentation, and dependencies.

The rumdl configuration enables MD013 with a 100-character line limit and paragraph reflow.
Code blocks and tables are exempt from that limit. MD060 aligns table columns in the Markdown
source, including tables wider than 100 characters. Run `rumdl check .` to check Markdown
formatting, or `rumdl check --fix .` to apply fixes.

## Exemptions

Declare package-specific exemptions in the root manifest:

```toml
[[workspace.metadata.archivindex-build.exemptions]]
package = "archivindex-surt"
rule = "dependencies.serde"
reason = "The shared declaration enables `derive`, which this crate implements by hand."
```

Exemptions support three categories:

| Rule                                 | Waives                                                                              |
| ------------------------------------ | ----------------------------------------------------------------------------------- |
| `package.authors`, `package.license` | Inheritance of the named field, for a crate carrying its own authorship or license. |
| `lints.workspace`                    | Inheritance of the workspace lints, for a package that must relax one.              |
| `dependencies.<name>`                | Workspace inheritance for a dependency that must be configured per package.         |

Every entry must name a package and give a non-empty `reason`. `sync` skips the named rule for that
package and applies the others as usual. `check` reports exemptions that are no longer needed so
you can remove them.

## License

Licensed under either the [MIT License][mit] or the [Apache License, Version 2.0][apache-2.0], at
your option; see [LICENSE-MIT][license-mit] and [LICENSE-APACHE][license-apache] for the full
texts.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[license-apache]: LICENSE-APACHE
[license-mit]: LICENSE-MIT
[mit]: https://opensource.org/license/mit
