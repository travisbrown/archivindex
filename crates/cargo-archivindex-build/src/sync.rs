use std::path::PathBuf;

use toml_edit::{Array, DocumentMut, InlineTable, Item, Table, Value, value};

use crate::Error;
use crate::document::{get, read, read_optional, update};
use crate::exemption::Exemptions;
use crate::policy::{
    DENY_ALL_FEATURES, DENY_INTEGER_SETTINGS, DENY_STRING_SETTINGS, DOCS_RS_ALL_FEATURES,
    DOCS_RS_ARGUMENTS, DOCS_RS_RUSTDOC_ARGS, INHERITED_PACKAGE_FIELDS,
};
use crate::project::{Member, Project};

pub fn project(project: &Project) -> Result<Vec<PathBuf>, Error> {
    let mut changed = Vec::new();
    let root_document = read(&project.root_manifest)?;
    // Respect exemptions during repair. The subsequent check reports invalid exemption entries.
    let mut exemptions = Exemptions::read(&root_document, &project.root_manifest, &mut Vec::new());
    let inheritable: Vec<_> = INHERITED_PACKAGE_FIELDS
        .into_iter()
        .filter(|field| get(&root_document, &["workspace", "package", field]).is_some())
        .collect();

    // A workspace root can be a package too, in which case one file holds both the workspace and
    // the member tables and has to be rewritten in a single pass.
    let root_member = project
        .members
        .iter()
        .find(|member| member.manifest_path == project.root_manifest);

    update(&project.root_manifest, &mut changed, |document| {
        if get(document, &["workspace", "resolver"]).and_then(Item::as_str) != Some("3") {
            document["workspace"]["resolver"] = value("3");
        }
        set_lints(document);
        sort_dependencies(document);

        if let Some(member) = root_member {
            set_member(document, member, &inheritable, &mut exemptions);
        }
    })?;

    for member in &project.members {
        if member.manifest_path == project.root_manifest {
            continue;
        }
        update(&member.manifest_path, &mut changed, |document| {
            set_member(document, member, &inheritable, &mut exemptions);
        })?;
    }

    let rustfmt_path = project.root.join("rustfmt.toml");
    update(&rustfmt_path, &mut changed, |document| {
        if get(document, &["group_imports"]).and_then(Item::as_str) != Some("StdExternalCrate") {
            document["group_imports"] = value("StdExternalCrate");
        }
        if get(document, &["imports_granularity"]).and_then(Item::as_str) != Some("Module") {
            document["imports_granularity"] = value("Module");
        }
    })?;

    let taplo_path = project.root.join(".taplo.toml");
    update(&taplo_path, &mut changed, |document| {
        let contains_target = document
            .get("exclude")
            .and_then(Item::as_array)
            .is_some_and(|array| {
                array
                    .iter()
                    .any(|entry| entry.as_str() == Some("target/**"))
            });
        if !contains_target {
            if let Some(excludes) = document["exclude"].as_array_mut() {
                excludes.push("target/**");
            } else {
                let mut excludes = Array::new();
                excludes.push("target/**");
                document["exclude"] = value(excludes);
            }
        }
        if get(document, &["formatting", "column_width"]).and_then(Item::as_integer) != Some(100) {
            document["formatting"]["column_width"] = value(100);
        }
    })?;

    // Do not create deny.toml: its license allowances require a project-specific decision.
    let deny_path = project.root.join("deny.toml");
    if read_optional(&deny_path)?.is_some() {
        update(&deny_path, &mut changed, |document| {
            for (path, expected) in DENY_STRING_SETTINGS {
                if get(document, path).and_then(Item::as_str) != Some(expected) {
                    set(document, path, value(expected));
                }
            }
            for (path, expected) in DENY_INTEGER_SETTINGS {
                if get(document, path).and_then(Item::as_integer) != Some(expected) {
                    set(document, path, value(expected));
                }
            }
            if get(document, &DENY_ALL_FEATURES).and_then(Item::as_bool) != Some(true) {
                set(document, &DENY_ALL_FEATURES, value(true));
            }
        })?;
    }

    Ok(changed)
}

/// Apply the member policy to one package manifest.
fn set_member(
    document: &mut DocumentMut,
    member: &Member,
    inheritable: &[&str],
    exemptions: &mut Exemptions,
) {
    if get(document, &["lints", "workspace"]).and_then(Item::as_bool) != Some(true)
        && !exemptions.allows_lints(&member.name)
    {
        document["lints"]["workspace"] = value(true);
    }

    for &field in inheritable {
        if get(document, &["package", field, "workspace"]).and_then(Item::as_bool) != Some(true)
            && !exemptions.allows_package_field(&member.name, field)
        {
            document["package"][field] = workspace_inheritance();
        }
    }

    // A description cannot be invented and a readme has to name a file that exists, so only the
    // docs.rs settings, which are the same for every published crate, are written here.
    if member.publishable {
        if get(document, &DOCS_RS_ALL_FEATURES).and_then(Item::as_bool) != Some(true) {
            set(document, &DOCS_RS_ALL_FEATURES, value(true));
        }
        let arguments_match = get(document, &DOCS_RS_ARGUMENTS)
            .and_then(Item::as_array)
            .is_some_and(|array| {
                array
                    .iter()
                    .map(Value::as_str)
                    .eq(DOCS_RS_RUSTDOC_ARGS.map(Some))
            });
        if !arguments_match {
            let mut arguments = Array::new();
            arguments.extend(DOCS_RS_RUSTDOC_ARGS);
            set(document, &DOCS_RS_ARGUMENTS, value(arguments));
        }
    }
}

/// Sort `[workspace.dependencies]` by name.
///
/// `toml_edit` keeps the trivia around each entry, so comments explaining a dependency move with it
/// rather than being stranded next to whatever ends up in its place.
fn sort_dependencies(document: &mut DocumentMut) {
    if let Some(table) = document["workspace"]["dependencies"].as_table_like_mut() {
        table.sort_values();
    }
}

fn set_lints(document: &mut DocumentMut) {
    if get(document, &["workspace", "lints", "rust", "missing_docs"]).and_then(Item::as_str)
        != Some("deny")
    {
        document["workspace"]["lints"]["rust"]["missing_docs"] = value("deny");
    }
    if !level_and_priority_match(
        document,
        &["workspace", "lints", "rust", "rust_2018_idioms"],
    ) {
        document["workspace"]["lints"]["rust"]["rust_2018_idioms"] = warn_with_priority();
    }
    if get(document, &["workspace", "lints", "rust", "unsafe_code"]).and_then(Item::as_str)
        != Some("forbid")
    {
        document["workspace"]["lints"]["rust"]["unsafe_code"] = value("forbid");
    }
    for lint in ["all", "pedantic", "nursery"] {
        if !level_and_priority_match(document, &["workspace", "lints", "clippy", lint]) {
            document["workspace"]["lints"]["clippy"][lint] = warn_with_priority();
        }
    }
    if get(
        document,
        &["workspace", "lints", "clippy", "missing_errors_doc"],
    )
    .and_then(Item::as_str)
        != Some("allow")
    {
        document["workspace"]["lints"]["clippy"]["missing_errors_doc"] = value("allow");
    }
}

fn level_and_priority_match(document: &DocumentMut, path: &[&str]) -> bool {
    let mut level_path = path.to_vec();
    level_path.push("level");
    let mut priority_path = path.to_vec();
    priority_path.push("priority");

    get(document, &level_path).and_then(Item::as_str) == Some("warn")
        && get(document, &priority_path).and_then(Item::as_integer) == Some(-1)
}

/// Write `item` at a dotted path, creating the tables it passes through.
fn set(document: &mut DocumentMut, path: &[&str], item: Item) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };

    let mut target = document.as_item_mut();
    for part in parents {
        target = &mut target[*part];
    }
    target[*last] = item;
}

fn warn_with_priority() -> Item {
    let mut table = InlineTable::new();
    table.insert("level", Value::from("warn"));
    table.insert("priority", Value::from(-1));
    value(table)
}

/// The dotted `field.workspace = true` that every manifest here uses to inherit a field.
fn workspace_inheritance() -> Item {
    let mut table = Table::new();
    table.set_dotted(true);
    table.insert("workspace", value(true));
    Item::Table(table)
}
