//! Checks and synchronizes the Cargo workspace conventions used by Archivindex projects.

use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};

mod check;
mod dependencies;
mod document;
mod exemption;
mod policy;
mod project;
mod sync;

use project::Project;

/// The reason a project could not be loaded or updated.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The workspace metadata could not be read.
    #[error("failed to read the workspace metadata: {0}")]
    Metadata(#[from] cargo_metadata::Error),
    /// A project file could not be read or written.
    #[error("failed to access {}: {source}", path.display())]
    Io {
        /// The file being accessed.
        path: PathBuf,
        /// The reason the file could not be read or written.
        source: std::io::Error,
    },
    /// A project file is not valid TOML.
    #[error("failed to parse {}: {source}", path.display())]
    Parse {
        /// The file that could not be parsed.
        path: PathBuf,
        /// The reason the file could not be parsed.
        source: toml_edit::TomlError,
    },
}

/// One policy violation in a project file.
#[derive(Debug, Eq, PartialEq)]
pub struct Violation {
    /// File containing the violation.
    pub path: PathBuf,
    /// Human-readable description of the required change.
    pub message: String,
}

impl Display for Violation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.path.display(), self.message)
    }
}

/// The result of synchronizing a project.
#[derive(Debug)]
pub struct SyncReport {
    /// Files whose contents were changed.
    pub changed_files: Vec<PathBuf>,
    /// Violations that could not be fixed mechanically.
    pub violations: Vec<Violation>,
}

/// Checks the workspace containing `manifest_path`, or the current workspace when it is `None`.
pub fn check_project(manifest_path: Option<&Path>) -> Result<Vec<Violation>, Error> {
    check::project(&Project::discover(manifest_path)?)
}

/// Synchronizes the workspace containing `manifest_path`, or the current workspace when it is
/// `None`, and checks the resulting files.
pub fn sync_project(manifest_path: Option<&Path>) -> Result<SyncReport, Error> {
    // Discovery costs a `cargo metadata` subprocess, so the project is loaded once. Synchronizing
    // rewrites the contents of manifests the project already names, never the set of members, and
    // checking re-reads those files from disk.
    let project = Project::discover(manifest_path)?;
    let changed_files = sync::project(&project)?;
    let violations = check::project(&project)?;

    Ok(SyncReport {
        changed_files,
        violations,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::{Violation, check_project, sync_project};

    const ROOT: &str = r#"[workspace]
members = ["helper", "member"]
resolver = "3"

[workspace.package]
authors = ["Archivindex developers"]
repository = "https://example.com/project"
edition = "2024"
rust-version = "1.88"
readme = "README.md"
license = "MIT OR Apache-2.0"
version = "0.1.0"

[workspace.dependencies]
fixture-helper = { path = "helper" }

[workspace.lints.rust]
missing_docs = "deny"
rust_2018_idioms = { level = "warn", priority = -1 }
unsafe_code = "forbid"

[workspace.lints.clippy]
all = { level = "warn", priority = -1 }
pedantic = { level = "warn", priority = -1 }
nursery = { level = "warn", priority = -1 }
missing_errors_doc = "allow"
"#;

    const HELPER: &str = r#"[package]
name = "fixture-helper"
authors.workspace = true
repository.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
version.workspace = true
publish = false

[lints]
workspace = true
"#;

    const MEMBER: &str = r#"[package]
name = "fixture-member"
description = "A fixture"
authors.workspace = true
repository.workspace = true
edition.workspace = true
rust-version.workspace = true
readme.workspace = true
license.workspace = true
version.workspace = true

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]

[lints]
workspace = true

[dependencies]
fixture-helper.workspace = true
"#;

    const DENY: &str = r#"[graph]
all-features = true

[advisories]
version = 2

[bans]
multiple-versions = "warn"
wildcards = "deny"

[licenses]
version = 2
allow = ["MIT"]

[sources]
unknown-registry = "deny"
unknown-git = "deny"
"#;

    /// A workspace on disk that starts out conforming and can then be broken in one way.
    struct Fixture {
        directory: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let fixture = Self {
                directory: tempfile::tempdir().expect("the fixture needs a temporary directory"),
            };

            fixture.write("Cargo.toml", ROOT);
            fixture.write("README.md", "# Fixture\n");
            fixture.write("helper/Cargo.toml", HELPER);
            fixture.write("helper/src/lib.rs", "");
            fixture.write("member/Cargo.toml", MEMBER);
            fixture.write("member/src/lib.rs", "");
            fixture.write("deny.toml", DENY);
            fixture.write(
                "rustfmt.toml",
                "group_imports = \"StdExternalCrate\"\nimports_granularity = \"Module\"\n",
            );
            fixture.write(
                ".taplo.toml",
                "exclude = [\"target/**\"]\n\n[formatting]\ncolumn_width = 100\n",
            );
            fixture
        }

        fn path(&self) -> &Path {
            self.directory.path()
        }

        fn manifest(&self) -> PathBuf {
            self.path().join("Cargo.toml")
        }

        fn write(&self, relative: &str, contents: &str) {
            let path = self.path().join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("the fixture needs its directories");
            }
            fs::write(path, contents).expect("the fixture needs its files");
        }

        fn read(&self, relative: &str) -> String {
            fs::read_to_string(self.path().join(relative)).expect("the fixture file must exist")
        }

        /// Replace the first occurrence of `original` in a fixture file.
        fn replace(&self, relative: &str, original: &str, replacement: &str) {
            let contents = self.read(relative);
            assert!(
                contents.contains(original),
                "{relative} has no {original:?}"
            );
            self.write(relative, &contents.replacen(original, replacement, 1));
        }

        fn check(&self) -> Vec<Violation> {
            check_project(Some(&self.manifest())).expect("the fixture must be readable")
        }

        /// The messages reported for one fixture file.
        fn messages(&self, relative: &str) -> Vec<String> {
            let path = self.path().join(relative);
            self.check()
                .into_iter()
                .filter(|violation| violation.path == path)
                .map(|violation| violation.message)
                .collect()
        }
    }

    /// Whether any message contains `text`.
    fn mentions(messages: &[String], text: &str) -> bool {
        messages.iter().any(|message| message.contains(text))
    }

    #[test]
    fn accepts_a_conforming_project() {
        let fixture = Fixture::new();
        let violations = fixture.check();
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn sync_does_not_rewrite_a_conforming_project() {
        let fixture = Fixture::new();
        let files = [
            "Cargo.toml",
            "helper/Cargo.toml",
            "member/Cargo.toml",
            "deny.toml",
            "rustfmt.toml",
            ".taplo.toml",
        ];
        let before: Vec<_> = files.iter().map(|file| fixture.read(file)).collect();

        let report = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");
        let after: Vec<_> = files.iter().map(|file| fixture.read(file)).collect();

        assert!(report.changed_files.is_empty());
        assert!(report.violations.is_empty());
        assert_eq!(before, after);
    }

    #[test]
    fn reports_workspace_metadata_and_lints() {
        let fixture = Fixture::new();
        // The members inherit from `[workspace.package]`, so a root stripped of it needs none.
        fixture.write("Cargo.toml", "[workspace]\nmembers = []\n");
        let messages = fixture.messages("Cargo.toml");

        assert!(mentions(&messages, "workspace.resolver"));
        assert!(mentions(
            &messages,
            "[workspace.package] must define `license`"
        ));
        assert!(mentions(
            &messages,
            "[workspace.package] must define `readme`"
        ));
        assert!(mentions(&messages, "workspace.lints.rust.unsafe_code"));
    }

    #[test]
    fn reports_a_missing_configuration_file() {
        let fixture = Fixture::new();
        for file in ["deny.toml", "rustfmt.toml", ".taplo.toml"] {
            fs::remove_file(fixture.path().join(file)).expect("the fixture file must exist");
            assert_eq!(fixture.messages(file), ["file is missing"]);
        }
    }

    #[test]
    fn reports_a_permissive_deny_configuration() {
        let fixture = Fixture::new();
        fixture.replace("deny.toml", "wildcards = \"deny\"", "wildcards = \"allow\"");
        fixture.replace("deny.toml", "all-features = true", "all-features = false");
        let messages = fixture.messages("deny.toml");

        assert!(mentions(&messages, "`bans.wildcards` must be \"deny\""));
        assert!(mentions(&messages, "`graph.all-features` must be true"));
    }

    #[test]
    fn sync_repairs_the_deny_configuration_but_never_writes_one() {
        let fixture = Fixture::new();
        fixture.replace("deny.toml", "wildcards = \"deny\"", "wildcards = \"allow\"");

        let report = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");
        assert!(report.violations.is_empty(), "{:#?}", report.violations);
        assert!(fixture.read("deny.toml").contains("wildcards = \"deny\""));

        fs::remove_file(fixture.path().join("deny.toml")).expect("the fixture file must exist");
        let report = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");

        assert!(!fixture.path().join("deny.toml").exists());
        assert_eq!(report.violations.len(), 1);
    }

    #[test]
    fn reports_and_sorts_an_unsorted_dependency_table() {
        let fixture = Fixture::new();
        fixture.replace(
            "Cargo.toml",
            "fixture-helper = { path = \"helper\" }",
            "zzz = { path = \"helper\", package = \"fixture-helper\" }\nfixture-helper = { path = \"helper\" }",
        );

        assert!(mentions(
            &fixture.messages("Cargo.toml"),
            "`fixture-helper` must precede `zzz`"
        ));

        let report = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");
        let sorted = fixture.read("Cargo.toml");
        let helper = sorted
            .find("fixture-helper =")
            .expect("the entry must survive");
        let other = sorted.find("zzz =").expect("the entry must survive");

        assert!(helper < other);
        assert!(report.changed_files.contains(&fixture.manifest()));
    }

    #[test]
    fn reports_an_unused_workspace_dependency() {
        let fixture = Fixture::new();
        fixture.replace("member/Cargo.toml", "fixture-helper.workspace = true", "");

        assert!(mentions(
            &fixture.messages("Cargo.toml"),
            "`workspace.dependencies.fixture-helper` is not used by any member"
        ));
    }

    #[test]
    fn reports_a_dependency_that_does_not_inherit() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "fixture-helper.workspace = true",
            "fixture-helper = { path = \"../helper\" }",
        );

        assert!(mentions(
            &fixture.messages("member/Cargo.toml"),
            "`fixture-helper` has a workspace entry"
        ));
    }

    #[test]
    fn reports_incomplete_metadata_only_on_publishable_packages() {
        let fixture = Fixture::new();
        fixture.replace("member/Cargo.toml", "description = \"A fixture\"\n", "");
        fixture.replace("member/Cargo.toml", "readme.workspace = true\n", "");
        fixture.replace(
            "member/Cargo.toml",
            "all-features = true",
            "all-features = false",
        );
        let messages = fixture.messages("member/Cargo.toml");

        assert!(mentions(&messages, "[package] must define `description`"));
        assert!(mentions(&messages, "[package] must define `readme`"));
        assert!(mentions(&messages, "docs.rs.all-features"));
        // The helper is `publish = false`, and declares none of this.
        assert!(fixture.messages("helper/Cargo.toml").is_empty());
    }

    #[test]
    fn sync_writes_the_docs_rs_metadata() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "[package.metadata.docs.rs]\nall-features = true\nrustdoc-args = [\"--cfg\", \"docsrs\"]\n",
            "",
        );

        let report = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");
        let member = fixture.read("member/Cargo.toml");

        assert!(report.violations.is_empty(), "{:#?}", report.violations);
        assert!(member.contains("all-features = true"));
        assert!(member.contains("rustdoc-args = [\"--cfg\", \"docsrs\"]"));
        assert!(!fixture.read("helper/Cargo.toml").contains("docs.rs"));
    }

    #[test]
    fn sync_repairs_inheritance_and_is_idempotent() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "license.workspace = true",
            "license = \"MIT\"",
        );
        fixture.replace("member/Cargo.toml", "[lints]\nworkspace = true\n", "");

        let report = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");
        assert!(report.violations.is_empty(), "{:#?}", report.violations);
        assert_eq!(
            report.changed_files,
            vec![fixture.path().join("member/Cargo.toml")]
        );
        // The dotted form, which is what every manifest in these projects already uses.
        assert!(
            fixture
                .read("member/Cargo.toml")
                .contains("license.workspace = true")
        );

        let second = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");
        assert!(second.changed_files.is_empty());
        assert!(second.violations.is_empty());
    }

    #[test]
    fn an_exemption_excuses_the_rule_it_names() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "license.workspace = true",
            "license = \"MIT\"",
        );
        assert!(mentions(
            &fixture.messages("member/Cargo.toml"),
            "package.license.workspace"
        ));

        exempt(&fixture, "fixture-member", "package.license");
        assert!(fixture.check().is_empty(), "{:#?}", fixture.check());
    }

    #[test]
    fn sync_leaves_an_exempted_package_alone() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "[lints]\nworkspace = true\n",
            "[lints.rust]\nunsafe_code = \"deny\"\n",
        );
        exempt(&fixture, "fixture-member", "lints.workspace");

        let report = sync_project(Some(&fixture.manifest())).expect("the fixture must be readable");

        assert!(report.violations.is_empty(), "{:#?}", report.violations);
        let member = fixture.read("member/Cargo.toml");
        assert!(member.contains("unsafe_code = \"deny\""));
        assert!(!member.contains("[lints]"));
    }

    #[test]
    fn a_dependency_exemption_names_the_dependency() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "fixture-helper.workspace = true",
            "fixture-helper = { path = \"../helper\" }",
        );

        exempt(&fixture, "fixture-member", "dependencies.other");
        assert!(mentions(
            &fixture.messages("member/Cargo.toml"),
            "has a workspace entry"
        ));

        fixture.replace(
            "Cargo.toml",
            "dependencies.other",
            "dependencies.fixture-helper",
        );
        assert!(fixture.check().is_empty(), "{:#?}", fixture.check());
    }

    #[test]
    fn reports_an_exemption_that_excuses_nothing() {
        let fixture = Fixture::new();
        exempt(&fixture, "fixture-member", "package.license");

        assert!(mentions(
            &fixture.messages("Cargo.toml"),
            "the exemption of `fixture-member` from `package.license` is no longer needed"
        ));
    }

    #[test]
    fn an_exemption_covers_only_the_fields_a_fork_carries() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "\nversion.workspace = true",
            "\nversion = \"0.1.0\"",
        );
        exempt(&fixture, "fixture-member", "package.version");

        assert!(mentions(
            &fixture.messages("Cargo.toml"),
            "names `package.version`, which is not an exemptable rule"
        ));
        assert!(mentions(
            &fixture.messages("member/Cargo.toml"),
            "`package.version.workspace` must be true"
        ));
    }

    #[test]
    fn reports_an_unusable_exemption() {
        let fixture = Fixture::new();
        exempt(&fixture, "fixture-member", "workspace.resolver");
        let messages = fixture.messages("Cargo.toml");

        assert!(mentions(
            &messages,
            "names `workspace.resolver`, which is not an exemptable rule"
        ));

        fixture.replace("Cargo.toml", ", reason = \"Because.\"", "");
        assert!(mentions(
            &fixture.messages("Cargo.toml"),
            "must set `reason` to a non-empty string"
        ));
    }

    #[test]
    fn an_exemption_can_be_a_table() {
        let fixture = Fixture::new();
        fixture.replace(
            "member/Cargo.toml",
            "license.workspace = true",
            "license = \"MIT\"",
        );

        let root = fixture.read("Cargo.toml");
        fixture.write(
            "Cargo.toml",
            &format!(
                "{root}\n[[workspace.metadata.archivindex-build.exemptions]]\n\
                 package = \"fixture-member\"\nrule = \"package.license\"\n\
                 reason = \"Because.\"\n"
            ),
        );

        assert!(fixture.check().is_empty(), "{:#?}", fixture.check());
    }

    /// Declare an exemption of `package` from `rule` in the fixture's root manifest.
    fn exempt(fixture: &Fixture, package: &str, rule: &str) {
        let root = fixture.read("Cargo.toml");
        fixture.write(
            "Cargo.toml",
            &format!(
                "{root}\n[workspace.metadata.archivindex-build]\nexemptions = [\
                 {{ package = \"{package}\", rule = \"{rule}\", reason = \"Because.\" }}]\n"
            ),
        );
    }
}
