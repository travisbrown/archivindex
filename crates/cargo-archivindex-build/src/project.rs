use std::path::{Path, PathBuf};

use cargo_metadata::MetadataCommand;

use crate::Error;

/// A package that belongs to the workspace being checked.
#[derive(Debug)]
pub struct Member {
    /// The package name, which is how an exemption refers to it.
    pub name: String,
    /// The package manifest.
    pub manifest_path: PathBuf,
    /// Whether the package can be published to a registry.
    ///
    /// The checker requires publication metadata only for these packages.
    pub publishable: bool,
}

/// A Cargo workspace and the packages it contains.
#[derive(Debug)]
pub struct Project {
    pub root: PathBuf,
    pub root_manifest: PathBuf,
    pub members: Vec<Member>,
}

impl Project {
    pub fn discover(manifest_path: Option<&Path>) -> Result<Self, Error> {
        let mut command = MetadataCommand::new();
        command.no_deps();

        if let Some(path) = manifest_path {
            command.manifest_path(path);
        }

        let metadata = command.exec()?;
        let root = metadata.workspace_root.into_std_path_buf();
        let root_manifest = root.join("Cargo.toml");
        let mut members: Vec<_> = metadata
            .packages
            .iter()
            .filter(|package| metadata.workspace_members.contains(&package.id))
            .map(|package| Member {
                name: package.name.to_string(),
                manifest_path: package.manifest_path.clone().into_std_path_buf(),
                // Cargo represents `publish = false` as an empty registry list.
                publishable: package
                    .publish
                    .as_ref()
                    .is_none_or(|registries| !registries.is_empty()),
            })
            .collect();
        members.sort_by(|first, second| first.manifest_path.cmp(&second.manifest_path));

        Ok(Self {
            root,
            root_manifest,
            members,
        })
    }
}
