use archivindex_wbm::{cas::import::CompressionType, digest::Sha1Digest};
use std::path::{Path, PathBuf};

#[derive(Default)]
pub struct SnapshotImport {
    pub paths: Vec<(Sha1Digest, PathBuf, Option<CompressionType>)>,
    pub skipped: Vec<PathBuf>,
    pub invalid_digests: Vec<(Sha1Digest, Sha1Digest)>,
}

pub fn snapshot_import<P: AsRef<Path>>(
    snapshot_dirs: &[P],
) -> Result<SnapshotImport, archivindex_wbm::cas::import::Error> {
    let importers = snapshot_dirs
        .iter()
        .map(archivindex_wbm::cas::import::Importer::new)
        .collect::<Vec<_>>();

    let mut result = SnapshotImport::default();

    for importer in importers {
        for file in importer {
            match file {
                Ok(archivindex_wbm::cas::import::File::Valid {
                    digest,
                    path,
                    compression_type,
                }) => result.paths.push((digest, path, compression_type)),
                Ok(archivindex_wbm::cas::import::File::Skipped { path }) => {
                    result.skipped.push(path);
                }
                Err(archivindex_wbm::cas::import::Error::InvalidDigest { expected, found }) => {
                    result.invalid_digests.push((expected, found));
                }
                Err(other) => {
                    return Err(other);
                }
            }
        }
    }

    result.paths.sort_by_key(|(digest, _, _)| *digest);

    Ok(result)
}
