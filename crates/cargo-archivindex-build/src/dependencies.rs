use std::collections::BTreeSet;

use toml_edit::{DocumentMut, Item};

use crate::document::get;
use crate::policy::DEPENDENCY_SECTIONS;

/// The names declared in `[workspace.dependencies]`.
pub fn workspace_names(document: &DocumentMut) -> BTreeSet<String> {
    get(document, &["workspace", "dependencies"])
        .and_then(Item::as_table_like)
        .map(|table| table.iter().map(|(name, _)| name.to_owned()).collect())
        .unwrap_or_default()
}

/// Apply `action` to every dependency a manifest declares, with its specification.
///
/// Normal, development, and build dependencies are all visited, both the unconditional ones and
/// those under a `[target.'cfg(...)']` table, because a workspace entry applies to all of them.
pub fn for_each(document: &DocumentMut, mut action: impl FnMut(&str, &Item)) {
    let mut visit = |item: &Item| {
        for section in DEPENDENCY_SECTIONS {
            if let Some(table) = item.get(section).and_then(Item::as_table_like) {
                for (name, specification) in table.iter() {
                    action(name, specification);
                }
            }
        }
    };

    visit(document.as_item());

    if let Some(targets) = get(document, &["target"]).and_then(Item::as_table_like) {
        for (_, target) in targets.iter() {
            visit(target);
        }
    }
}
