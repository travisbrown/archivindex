use std::collections::BTreeSet;
use std::path::Path;

use toml_edit::{DocumentMut, Item, Value};

use crate::document::{get, read, read_optional};
use crate::exemption::Exemptions;
use crate::policy::{
    DENY_ALL_FEATURES, DENY_INTEGER_SETTINGS, DENY_STRING_SETTINGS, DOCS_RS_ALL_FEATURES,
    DOCS_RS_ARGUMENTS, DOCS_RS_RUSTDOC_ARGS, INHERITED_PACKAGE_FIELDS, PRIORITY_LINTS,
    PUBLISHED_FIELDS, STRING_LINTS, WORKSPACE_PACKAGE_FIELDS,
};
use crate::project::{Member, Project};
use crate::{Error, Violation, dependencies};

pub fn project(project: &Project) -> Result<Vec<Violation>, Error> {
    let mut violations = Vec::new();
    let root_document = read(&project.root_manifest)?;
    let mut exemptions = Exemptions::read(&root_document, &project.root_manifest, &mut violations);
    let workspace_dependencies = dependencies::workspace_names(&root_document);

    root(project, &root_document, &mut violations);

    // Every workspace dependency has to be used by someone, so the members are visited once and
    // their dependency names accumulated rather than the root manifest being revisited per name.
    let mut used = BTreeSet::new();
    for entry in &project.members {
        let document = read(&entry.manifest_path)?;
        dependencies::for_each(&document, |name, _| {
            used.insert(name.to_owned());
        });
        member(
            &document,
            entry,
            &workspace_dependencies,
            &mut exemptions,
            &mut violations,
        );
    }

    for name in workspace_dependencies.difference(&used) {
        push(
            &mut violations,
            &project.root_manifest,
            format!("`workspace.dependencies.{name}` is not used by any member"),
        );
    }

    exemptions.report_unused(&project.root_manifest, &mut violations);
    rustfmt(project, &mut violations)?;
    taplo(project, &mut violations)?;
    deny(project, &mut violations)?;
    Ok(violations)
}

fn root(project: &Project, document: &DocumentMut, violations: &mut Vec<Violation>) {
    check_string(
        document,
        &["workspace", "resolver"],
        "3",
        &project.root_manifest,
        violations,
    );

    for field in WORKSPACE_PACKAGE_FIELDS {
        if get(document, &["workspace", "package", field]).is_none() {
            push(
                violations,
                &project.root_manifest,
                format!("[workspace.package] must define `{field}`"),
            );
        }
    }

    sorted_dependencies(document, &project.root_manifest, violations);
    lints(document, &project.root_manifest, violations);
}

/// Report the first pair of workspace dependencies that is out of order. `sync` sorts the table.
fn sorted_dependencies(document: &DocumentMut, path: &Path, violations: &mut Vec<Violation>) {
    let Some(table) = get(document, &["workspace", "dependencies"]).and_then(Item::as_table_like)
    else {
        return;
    };

    let names: Vec<_> = table.iter().map(|(name, _)| name).collect();
    let unsorted = names
        .iter()
        .zip(names.iter().skip(1))
        .find(|(previous, name)| previous > name);

    if let Some((previous, name)) = unsorted {
        push(
            violations,
            path,
            format!("[workspace.dependencies] must be sorted: `{name}` must precede `{previous}`"),
        );
    }
}

fn member(
    document: &DocumentMut,
    member: &Member,
    workspace_dependencies: &BTreeSet<String>,
    exemptions: &mut Exemptions,
    violations: &mut Vec<Violation>,
) {
    let path = member.manifest_path.as_path();

    // An exemption is only consulted once a rule has actually been broken, so that an entry which
    // excuses nothing stays visibly unused.
    if get(document, &["lints", "workspace"]).and_then(Item::as_bool) != Some(true)
        && !exemptions.allows_lints(&member.name)
    {
        push(violations, path, "`lints.workspace` must be true");
    }

    for field in INHERITED_PACKAGE_FIELDS {
        if get(document, &["package", field, "workspace"]).and_then(Item::as_bool) != Some(true)
            && !exemptions.allows_package_field(&member.name, field)
        {
            push(
                violations,
                path,
                format!("`package.{field}.workspace` must be true"),
            );
        }
    }

    if member.publishable {
        published(document, path, violations);
    }

    dependencies::for_each(document, |name, specification| {
        if workspace_dependencies.contains(name)
            && specification.get("workspace").and_then(Item::as_bool) != Some(true)
            && !exemptions.allows_dependency(&member.name, name)
        {
            push(
                violations,
                path,
                format!("`{name}` has a workspace entry, so it must be `workspace = true`"),
            );
        }
    });
}

/// Check the metadata required for packages that allow publication.
fn published(document: &DocumentMut, path: &Path, violations: &mut Vec<Violation>) {
    for field in PUBLISHED_FIELDS {
        if get(document, &["package", field]).is_none() {
            push(violations, path, format!("[package] must define `{field}`"));
        }
    }

    check_true(document, &DOCS_RS_ALL_FEATURES, path, violations);

    let arguments = get(document, &DOCS_RS_ARGUMENTS)
        .and_then(Item::as_array)
        .is_some_and(|array| {
            array
                .iter()
                .map(Value::as_str)
                .eq(DOCS_RS_RUSTDOC_ARGS.map(Some))
        });
    if !arguments {
        push(
            violations,
            path,
            format!("`package.metadata.docs.rs.rustdoc-args` must be {DOCS_RS_RUSTDOC_ARGS:?}"),
        );
    }
}

fn lints(document: &DocumentMut, path: &Path, violations: &mut Vec<Violation>) {
    for (item_path, expected) in STRING_LINTS {
        check_string(document, item_path, expected, path, violations);
    }

    for item_path in PRIORITY_LINTS {
        check_integer(document, item_path, -1, path, violations);
    }
}

fn rustfmt(project: &Project, violations: &mut Vec<Violation>) -> Result<(), Error> {
    let path = project.root.join("rustfmt.toml");
    let Some(document) = read_optional(&path)? else {
        push(violations, &path, "file is missing");
        return Ok(());
    };

    check_string(
        &document,
        &["group_imports"],
        "StdExternalCrate",
        &path,
        violations,
    );
    check_string(
        &document,
        &["imports_granularity"],
        "Module",
        &path,
        violations,
    );
    Ok(())
}

fn taplo(project: &Project, violations: &mut Vec<Violation>) -> Result<(), Error> {
    let path = project.root.join(".taplo.toml");
    let Some(document) = read_optional(&path)? else {
        push(violations, &path, "file is missing");
        return Ok(());
    };

    let excludes_target = get(&document, &["exclude"])
        .and_then(Item::as_array)
        .is_some_and(|array| {
            array
                .iter()
                .any(|entry| entry.as_str() == Some("target/**"))
        });
    if !excludes_target {
        push(violations, &path, "`exclude` must contain \"target/**\"");
    }
    check_integer(
        &document,
        &["formatting", "column_width"],
        100,
        &path,
        violations,
    );
    Ok(())
}

/// Check the shared `cargo deny` settings, leaving project-specific allowances unchanged.
fn deny(project: &Project, violations: &mut Vec<Violation>) -> Result<(), Error> {
    let path = project.root.join("deny.toml");
    let Some(document) = read_optional(&path)? else {
        push(violations, &path, "file is missing");
        return Ok(());
    };

    for (item_path, expected) in DENY_STRING_SETTINGS {
        check_string(&document, item_path, expected, &path, violations);
    }
    for (item_path, expected) in DENY_INTEGER_SETTINGS {
        check_integer(&document, item_path, expected, &path, violations);
    }
    check_true(&document, &DENY_ALL_FEATURES, &path, violations);
    Ok(())
}

fn check_string(
    document: &DocumentMut,
    path: &[&str],
    expected: &str,
    file: &Path,
    violations: &mut Vec<Violation>,
) {
    if get(document, path).and_then(Item::as_str) != Some(expected) {
        push(
            violations,
            file,
            format!("`{}` must be \"{expected}\"", path.join(".")),
        );
    }
}

fn check_true(document: &DocumentMut, path: &[&str], file: &Path, violations: &mut Vec<Violation>) {
    if get(document, path).and_then(Item::as_bool) != Some(true) {
        push(
            violations,
            file,
            format!("`{}` must be true", path.join(".")),
        );
    }
}

fn check_integer(
    document: &DocumentMut,
    path: &[&str],
    expected: i64,
    file: &Path,
    violations: &mut Vec<Violation>,
) {
    if get(document, path).and_then(Item::as_integer) != Some(expected) {
        push(
            violations,
            file,
            format!("`{}` must be {expected}", path.join(".")),
        );
    }
}

fn push(violations: &mut Vec<Violation>, path: &Path, message: impl Into<String>) {
    violations.push(Violation {
        path: path.to_path_buf(),
        message: message.into(),
    });
}
