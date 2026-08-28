# cargo-archivindex-build

`cargo-archivindex-build` keeps Cargo workspace policy and formatter configuration consistent
across Archivindex projects.

Install it from a clone of this repository, then run it from any workspace:

```console
cargo install --locked --path crates/cargo-archivindex-build
cargo archivindex-build check
cargo archivindex-build sync
```

Both commands accept `--manifest-path <PATH>`. `check` reports policy violations and exits
unsuccessfully when it finds one. `sync` applies fixes that can be determined mechanically, then
runs the same checks. Values that are specific to a project, such as its repository URL, must
already be present in `[workspace.package]`.

The enforced policy includes the following requirements:

- Cargo resolver version 3;
- shared workspace package metadata (`authors`, `repository`, `edition`, `rust-version`,
  `readme`, `license`, and `version`);
- the Archivindex Rust and Clippy workspace lint configuration;
- workspace lint and package metadata inheritance in all member packages, including root packages;
- a sorted `[workspace.dependencies]` table, with no entry that no member uses, and no member
  restating a dependency the table already declares;
- `description`, `readme`, and docs.rs metadata on packages that allow publication to a registry;
- the shared `rustfmt.toml` and `.taplo.toml` settings;
- the `deny.toml` settings that decide how strict a `cargo deny` run is.

It checks the configuration of `rustfmt`, Clippy, Taplo, and `cargo deny`; run those tools
separately to check source formatting, code, and dependencies.

## Exemptions

Declare package-specific exemptions in the root manifest:

```toml
[[workspace.metadata.archivindex-build.exemptions]]
package = "archivindex-surt"
rule = "dependencies.serde"
reason = "The shared declaration enables `derive`, which this crate implements by hand."
```

Exemptions support three categories:

| Rule | Waives |
| --- | --- |
| `package.authors`, `package.license` | Inheritance of the named field, for a crate carrying its own authorship or license. |
| `lints.workspace` | Inheritance of the workspace lints, for a package that must relax one. |
| `dependencies.<name>` | Workspace inheritance for a dependency that must be configured per package. |

Every entry must name a package and give a non-empty `reason`. `sync` skips only the exempted
rule for that package. `check` reports unused exemptions so they can be removed.

## License

Licensed under either the [MIT License][mit] or the [Apache License, Version 2.0][apache-2.0], at
your option; see [LICENSE-MIT][license-mit] and [LICENSE-APACHE][license-apache] for the full
texts.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[license-apache]: LICENSE-APACHE
[license-mit]: LICENSE-MIT
[mit]: https://opensource.org/license/mit
