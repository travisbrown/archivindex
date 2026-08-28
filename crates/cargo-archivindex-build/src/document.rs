use std::fs;
use std::path::{Path, PathBuf};

use toml_edit::{DocumentMut, Item};

use crate::Error;

pub fn read(path: &Path) -> Result<DocumentMut, Error> {
    let source = fs::read_to_string(path).map_err(|source| io(path, source))?;
    parse(path, &source)
}

pub fn read_optional(path: &Path) -> Result<Option<DocumentMut>, Error> {
    match fs::read_to_string(path) {
        Ok(source) => parse(path, &source).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(path, error)),
    }
}

pub fn update(
    path: &Path,
    changed: &mut Vec<PathBuf>,
    update: impl FnOnce(&mut DocumentMut),
) -> Result<(), Error> {
    let original = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(io(path, error)),
    };
    let mut document = parse(path, &original)?;
    update(&mut document);
    let updated = document.to_string();

    if updated != original {
        fs::write(path, updated).map_err(|source| io(path, source))?;
        changed.push(path.to_path_buf());
    }
    Ok(())
}

pub fn get<'a>(document: &'a DocumentMut, path: &[&str]) -> Option<&'a Item> {
    let mut item = document.as_item();
    for part in path {
        item = item.get(*part)?;
    }
    Some(item)
}

fn parse(path: &Path, source: &str) -> Result<DocumentMut, Error> {
    source
        .parse::<DocumentMut>()
        .map_err(|source| Error::Parse {
            path: path.to_path_buf(),
            source,
        })
}

fn io(path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        path: path.to_path_buf(),
        source,
    }
}
