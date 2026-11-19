# Using cargo-archivindex-build

Install it from a clone of this repository, then run it from any workspace:

```console
cargo install --locked --path crates/cargo-archivindex-build
cargo archivindex-build check
cargo archivindex-build sync
```

`check` reports policy violations and exits with a failure status if it finds any. `sync` applies
automatic fixes, then reports anything that still needs attention. Both accept
`--manifest-path <PATH>`.

Supply project-specific values, such as the repository URL, in `[workspace.package]`. If a member
has its own lint settings, `sync` preserves them and reports the missing workspace inheritance.
Remove those settings to inherit the workspace lints, or keep them with a `lints.workspace`
exemption.

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

Markdown paragraphs wrap at 100 characters. Code blocks and tables have no width limit, and table
columns align in the source. Run `rumdl check .` to check formatting or `rumdl check --fix .` to
apply fixes.

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
