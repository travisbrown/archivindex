use std::fmt::{self, Display, Formatter};
use std::path::Path;

use toml_edit::{DocumentMut, Item, TableLike, Value};

use crate::Violation;
use crate::document::get;

/// The root manifest table that holds the exemption list.
const TABLE_PATH: [&str; 4] = ["workspace", "metadata", "archivindex-build", "exemptions"];

/// The prefix of the rule name that excuses one dependency from workspace inheritance.
const DEPENDENCY_PREFIX: &str = "dependencies.";

/// The prefix of the rule name that excuses one package field from workspace inheritance.
const PACKAGE_PREFIX: &str = "package.";

/// Package metadata fields that may be exempted from workspace inheritance.
const PACKAGE_FIELDS: [&str; 2] = ["authors", "license"];

/// A rule that a package may be exempted from: metadata inheritance, lint inheritance, or
/// dependency inheritance. Other policy rules cannot be exempted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Rule {
    /// The package states the named field for itself instead of inheriting the workspace value.
    Package(&'static str),
    /// The package configures its own lints instead of inheriting the workspace lints.
    Lints,
    /// The package depends on the named crate without workspace inheritance.
    Dependency(String),
}

impl Rule {
    /// The rule an exemption entry names, or `None` if no exemptable rule has that name.
    fn parse(name: &str) -> Option<Self> {
        if name == "lints.workspace" {
            return Some(Self::Lints);
        }
        if let Some(field) = name.strip_prefix(PACKAGE_PREFIX) {
            return PACKAGE_FIELDS
                .into_iter()
                .find(|exemptable| *exemptable == field)
                .map(Self::Package);
        }

        name.strip_prefix(DEPENDENCY_PREFIX)
            .filter(|dependency| !dependency.is_empty())
            .map(|dependency| Self::Dependency(dependency.to_owned()))
    }
}

impl Display for Rule {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Package(field) => write!(formatter, "{PACKAGE_PREFIX}{field}"),
            Self::Lints => formatter.write_str("lints.workspace"),
            Self::Dependency(dependency) => write!(formatter, "{DEPENDENCY_PREFIX}{dependency}"),
        }
    }
}

/// One declared exemption, and whether anything has needed it.
#[derive(Debug)]
struct Entry {
    package: String,
    rule: Rule,
    used: bool,
}

/// The exemptions a project declares in its root manifest.
///
/// Matching an exemption marks it as used. [`Exemptions::report_unused`] reports entries that did
/// not waive a violation.
#[derive(Debug)]
pub struct Exemptions {
    entries: Vec<Entry>,
}

impl Exemptions {
    /// Read the exemptions declared by a root manifest.
    ///
    /// Accepts arrays of tables and arrays of inline tables. Invalid entries are reported as
    /// violations of `path` and ignored.
    pub fn read(document: &DocumentMut, path: &Path, violations: &mut Vec<Violation>) -> Self {
        let mut entries = Vec::new();
        let Some(item) = get(document, &TABLE_PATH) else {
            return Self { entries };
        };

        let tables: Vec<_> = match item {
            Item::ArrayOfTables(tables) => tables
                .iter()
                .map(|table| Some(table as &dyn TableLike))
                .collect(),
            Item::Value(Value::Array(array)) => array
                .iter()
                .map(|value| value.as_inline_table().map(|table| table as &dyn TableLike))
                .collect(),
            Item::None | Item::Value(_) | Item::Table(_) => {
                push(
                    violations,
                    path,
                    format!("`{}` must be an array of tables", TABLE_PATH.join(".")),
                );
                return Self { entries };
            }
        };

        for (index, table) in tables.into_iter().enumerate() {
            match Self::entry(table) {
                Ok(entry) => entries.push(entry),
                Err(message) => push(
                    violations,
                    path,
                    format!("exemption {index} in `{}` {message}", TABLE_PATH.join(".")),
                ),
            }
        }

        Self { entries }
    }

    /// Parse one exemption entry, or describe why it is not usable.
    fn entry(table: Option<&dyn TableLike>) -> Result<Entry, String> {
        let table = table.ok_or_else(|| "must be a table".to_owned())?;
        let field = |name: &str| {
            table
                .get(name)
                .and_then(Item::as_str)
                .filter(|text| !text.is_empty())
                .ok_or_else(|| format!("must set `{name}` to a non-empty string"))
        };

        let package = field("package")?.to_owned();
        let rule = field("rule")?;
        // Never read, but requiring it keeps the justification next to the entry.
        field("reason")?;

        Rule::parse(rule)
            .map(|rule| Entry {
                package,
                rule,
                used: false,
            })
            .ok_or_else(|| format!("names `{rule}`, which is not an exemptable rule"))
    }

    /// Whether `package` is excused from inheriting the workspace lints.
    pub fn allows_lints(&mut self, package: &str) -> bool {
        self.mark(|entry| entry.package == package && entry.rule == Rule::Lints)
    }

    /// Whether `package` is excused from inheriting `field` from `[workspace.package]`.
    pub fn allows_package_field(&mut self, package: &str, field: &str) -> bool {
        self.mark(|entry| {
            entry.package == package
                && matches!(entry.rule, Rule::Package(exempted) if exempted == field)
        })
    }

    /// Whether `package` is excused from inheriting `dependency` from the workspace.
    pub fn allows_dependency(&mut self, package: &str, dependency: &str) -> bool {
        self.mark(|entry| {
            entry.package == package
                && matches!(&entry.rule, Rule::Dependency(name) if name == dependency)
        })
    }

    /// Mark the first matching entry as used, and report whether one matched.
    fn mark(&mut self, matches: impl Fn(&Entry) -> bool) -> bool {
        self.entries
            .iter_mut()
            .find(|entry| matches(entry))
            .is_some_and(|entry| {
                entry.used = true;
                true
            })
    }

    /// Report every exemption that excused nothing.
    pub fn report_unused(&self, path: &Path, violations: &mut Vec<Violation>) {
        for entry in self.entries.iter().filter(|entry| !entry.used) {
            push(
                violations,
                path,
                format!(
                    "the exemption of `{}` from `{}` is no longer needed",
                    entry.package, entry.rule
                ),
            );
        }
    }
}

/// Record a violation of the root manifest.
fn push(violations: &mut Vec<Violation>, path: &Path, message: String) {
    violations.push(Violation {
        path: path.to_path_buf(),
        message,
    });
}
