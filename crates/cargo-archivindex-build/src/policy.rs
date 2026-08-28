/// Fields `[workspace.package]` must define so that members have something to inherit.
///
/// `readme` is absent because an inherited path resolves against the workspace root, which would
/// give every member the same file; see [`PUBLISHED_FIELDS`].
pub const WORKSPACE_PACKAGE_FIELDS: [&str; 6] = [
    "authors",
    "repository",
    "edition",
    "rust-version",
    "license",
    "version",
];

/// Fields every member package must inherit from `[workspace.package]`.
///
/// Publishable packages must declare `readme` as well, but name their own file rather than inherit
/// one; see [`PUBLISHED_FIELDS`].
pub const INHERITED_PACKAGE_FIELDS: [&str; 6] = [
    "authors",
    "repository",
    "edition",
    "rust-version",
    "license",
    "version",
];

/// Metadata fields required for packages that allow publication to any registry.
///
/// A package names its own `readme`, since Cargo resolves an inherited one against the workspace
/// root rather than the package directory.
pub const PUBLISHED_FIELDS: [&str; 2] = ["description", "readme"];

/// The `rustdoc` arguments that let a published crate document its feature-gated items.
pub const DOCS_RS_RUSTDOC_ARGS: [&str; 2] = ["--cfg", "docsrs"];

/// The docs.rs setting that documents every feature of a published crate.
pub const DOCS_RS_ALL_FEATURES: [&str; 5] = ["package", "metadata", "docs", "rs", "all-features"];

/// The docs.rs setting that carries [`DOCS_RS_RUSTDOC_ARGS`].
pub const DOCS_RS_ARGUMENTS: [&str; 5] = ["package", "metadata", "docs", "rs", "rustdoc-args"];

/// Workspace resolver shared by checking and repair.
pub const WORKSPACE_RESOLVER: &str = "3";

/// Required lint paths, levels, and optional group priorities.
pub const LINTS: [(&[&str], &str, Option<i64>); 7] = [
    (
        &["workspace", "lints", "rust", "missing_docs"],
        "deny",
        None,
    ),
    (
        &["workspace", "lints", "rust", "rust_2018_idioms"],
        "warn",
        Some(-1),
    ),
    (
        &["workspace", "lints", "rust", "unsafe_code"],
        "forbid",
        None,
    ),
    (&["workspace", "lints", "clippy", "all"], "warn", Some(-1)),
    (
        &["workspace", "lints", "clippy", "pedantic"],
        "warn",
        Some(-1),
    ),
    (
        &["workspace", "lints", "clippy", "nursery"],
        "warn",
        Some(-1),
    ),
    (
        &["workspace", "lints", "clippy", "missing_errors_doc"],
        "allow",
        None,
    ),
];

/// Shared Rust import formatting settings.
pub const RUSTFMT_STRING_SETTINGS: [(&[&str], &str); 2] = [
    (&["group_imports"], "StdExternalCrate"),
    (&["imports_granularity"], "Module"),
];

/// Shared TOML formatting settings.
pub const TAPLO_INTEGER_SETTINGS: [(&[&str], i64); 1] = [(&["formatting", "column_width"], 100)];

/// Manifest sections that can name a dependency.
pub const DEPENDENCY_SECTIONS: [&str; 3] =
    ["dependencies", "dev-dependencies", "build-dependencies"];

/// Shared `cargo deny` rules for duplicate versions, wildcard dependencies, and unknown sources.
/// Project-specific license allowances, source allowances, and advisory ignores are unchanged.
pub const DENY_STRING_SETTINGS: [(&[&str], &str); 4] = [
    (&["bans", "multiple-versions"], "warn"),
    (&["bans", "wildcards"], "deny"),
    (&["sources", "unknown-registry"], "deny"),
    (&["sources", "unknown-git"], "deny"),
];

/// Required configuration versions for the `deny.toml` advisory and license sections.
pub const DENY_INTEGER_SETTINGS: [(&[&str], i64); 2] = [
    (&["advisories", "version"], 2),
    (&["licenses", "version"], 2),
];

/// The `deny.toml` setting that makes the checks cover optional dependencies.
pub const DENY_ALL_FEATURES: [&str; 2] = ["graph", "all-features"];

/// Markdown rules enabled across repositories.
pub const RUMDL_ENABLED_RULES: [&str; 2] = ["MD013", "MD060"];

/// Shared Markdown prose and table formatting settings.
pub const RUMDL_BOOLEAN_SETTINGS: [(&[&str], bool); 6] = [
    (&["MD013", "code-blocks"], false),
    (&["MD013", "tables"], false),
    (&["MD013", "stern"], true),
    (&["MD013", "ignore-link-urls"], false),
    (&["MD013", "reflow"], true),
    (&["MD060", "enabled"], true),
];

/// Tables have no width limit because MD013 excludes them and MD060 inherits that setting.
pub const RUMDL_INTEGER_SETTINGS: [(&[&str], i64); 2] = [
    (&["MD013", "line-length"], 100),
    (&["MD060", "max-width"], 0),
];

/// Pad Markdown table columns so their separators align in the source.
pub const RUMDL_STRING_SETTINGS: [(&[&str], &str); 1] = [(&["MD060", "style"], "aligned")];
