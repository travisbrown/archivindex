//! Publish complete files with explicit overwrite and durability guarantees.
//!
//! Writing an archive can take a long time, and the process may fail before the file is complete.
//! This crate wraps [`tempfile`] to keep partial output separate from completed files. Temporary
//! files have unique, dot-prefixed names by default; [`Publication::with_partial_path`] lets you
//! choose a predictable name instead.
//!
//! A [`Publication`] owns a temporary file in the destination directory. By default, dropping an
//! instance before publication removes that file on a best-effort basis, or you can use
//! [`Publication::retain_partial`] to retain unfinished output. Neither behavior changes the
//! destination. The destination is not reserved, and multiple instances may target the same path,
//! with each instance's [`Policy`] determining what happens on publication in the case of
//! collisions.
//!
//! Callers must finish encoders and flush any buffers before [`Publication::publish`], which
//! synchronizes the file, persists it under the chosen policy, then synchronizes the parent
//! directory on Unix (other platforms get the file sync and persistence operation, but no directory
//! sync guarantee).
//!
//! ```
//! use std::io::Write;
//! use archivindex_publication::{Policy, Publication};
//!
//! # let directory = tempfile::tempdir()?;
//! let output = directory.path().join("result");
//! let mut pending = Publication::new(&output, Policy::CreateNew)?;
//! pending.write_all(b"complete contents")?;
//! pending.publish()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tempfile::{NamedTempFile, TempPath};

/// What to do with a destination that already exists at publication time.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Policy {
    /// Refuse to replace any existing directory entry, including a dangling symlink.
    CreateNew,
    /// Atomically replace the destination entry, leaving any link target untouched.
    Replace,
}

/// A failure synchronizing or publishing a completed file.
#[derive(Debug, thiserror::Error)]
pub struct Error {
    /// The publication stage that failed.
    pub kind: ErrorKind,
    /// The destination path. For [`ErrorKind::DirectorySync`], the completed file is already here.
    pub path: PathBuf,
    /// The filesystem error.
    #[source]
    pub source: std::io::Error,
}

/// The publication stage that failed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorKind {
    /// File synchronization failed before the destination was changed.
    FileSync,
    /// Persistence failed before the destination was changed.
    Persist,
    /// The destination is complete and visible, but its directory could not be synchronized.
    DirectorySync,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path = self.path.display();
        match self.kind {
            ErrorKind::FileSync => write!(f, "cannot sync pending output for {path}"),
            ErrorKind::Persist => write!(f, "cannot publish {path}"),
            ErrorKind::DirectorySync => {
                write!(f, "published {path}, but could not sync its directory")
            }
        }
    }
}

impl Error {
    /// Whether the completed output was made visible before this error occurred.
    #[must_use]
    pub const fn is_published(&self) -> bool {
        matches!(self.kind, ErrorKind::DirectorySync)
    }
}

/// Preserve the stage and source for APIs that expose only I/O errors.
///
/// The original [`Error`] can be recovered with [`std::io::Error::get_ref`] and `downcast_ref`.
impl From<Error> for std::io::Error {
    fn from(error: Error) -> Self {
        Self::new(error.source.kind(), error)
    }
}

/// An owned temporary file and the policy for publishing it.
///
/// By default its unique temporary name ends in `.tmp`, so programs reading the directory can
/// ignore or clean up unfinished files. Use [`Self::with_partial_path`] to choose a name yourself.
/// Neither constructor truncates an existing temporary file. On Unix, newly created files have
/// owner-only permissions, including after publication.
#[derive(Debug)]
pub struct Publication {
    temporary: NamedTempFile,
    destination: PathBuf,
    policy: Policy,
}

impl Publication {
    /// Create a uniquely named temporary file in the destination directory.
    ///
    /// With `CreateNew`, this fails early if the destination already exists. The path is not
    /// reserved, and publication still refuses to overwrite anything created there in the meantime.
    pub fn new(destination: impl AsRef<Path>, policy: Policy) -> std::io::Result<Self> {
        let destination = std::path::absolute(destination)?;
        check_destination(&destination, policy)?;
        let temporary = tempfile::Builder::new()
            .prefix(".archivindex-")
            .suffix(".tmp")
            .tempfile_in(parent(&destination))?;
        Ok(Self {
            temporary,
            destination,
            policy,
        })
    }

    /// Create a partial file at the chosen path, failing if that path already exists.
    ///
    /// Use this for a predictable name such as `output.warc.partial`. The partial file must be in
    /// the destination directory and have a different name. Both paths are made absolute so that
    /// changing the working directory does not change where the files are written.
    pub fn with_partial_path(
        destination: impl AsRef<Path>,
        partial: impl AsRef<Path>,
        policy: Policy,
    ) -> std::io::Result<Self> {
        let destination = std::path::absolute(destination)?;
        let partial = std::path::absolute(partial)?;
        if destination == partial || parent(&destination) != parent(&partial) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "partial must be a distinct sibling",
            ));
        }
        check_destination(&destination, policy)?;
        // Construct the path owner only after exclusive creation succeeds, so an occupied name is
        // never removed by a failed constructor.
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let file = options.open(&partial)?;
        let path = TempPath::try_from_path(partial)?;
        Ok(Self {
            temporary: NamedTempFile::from_parts(file, path),
            destination,
            policy,
        })
    }

    /// Adopt an existing temporary file in the destination directory.
    ///
    /// The temporary and destination paths must differ. The file keeps its existing permissions.
    /// If this function returns an error, it drops the supplied temporary file, applying its usual
    /// cleanup policy.
    pub fn from_temporary(
        destination: impl AsRef<Path>,
        temporary: NamedTempFile,
        policy: Policy,
    ) -> std::io::Result<Self> {
        let destination = std::path::absolute(destination)?;
        let temporary_path = std::path::absolute(temporary.path())?;
        if destination == temporary_path || parent(&destination) != parent(&temporary_path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "temporary file must be a distinct sibling",
            ));
        }
        Ok(Self {
            temporary,
            destination,
            policy,
        })
    }

    /// Retain the temporary file if this publication is dropped before it is published.
    ///
    /// Call this before writing to keep partial output if you return early after an error or
    /// cancellation, or unwind after a panic. The file also stays at its temporary path if
    /// synchronization or the move to the destination fails. A successful move puts it at the
    /// destination as usual.
    ///
    /// This disables automatic deletion; it does not flush external buffers, finish encoders, or
    /// synchronize the partial file. Retained output may be incomplete and is not guaranteed to
    /// survive a system crash or power loss. Signal handling remains the caller's responsibility.
    ///
    /// ```
    /// use std::io::Write;
    /// use archivindex_publication::{Policy, Publication};
    ///
    /// # let directory = tempfile::tempdir()?;
    /// # let output = directory.path().join("foo-12739137.warc");
    /// # let partial = directory.path().join("foo-12739137.warc.partial");
    /// let mut pending = Publication::with_partial_path(&output, &partial, Policy::CreateNew)?
    ///     .retain_partial();
    /// pending.write_all(b"unfinished output")?;
    /// drop(pending);
    /// assert_eq!(std::fs::read(&partial)?, b"unfinished output");
    /// assert!(!output.exists());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn retain_partial(mut self) -> Self {
        self.temporary.disable_cleanup(true);
        self
    }

    /// The temporary path, for diagnostics or observing an in-progress write.
    #[must_use]
    pub fn temporary_path(&self) -> &Path {
        self.temporary.path()
    }

    /// Open an independent handle to the temporary file, starting at offset zero.
    ///
    /// Finalize and flush every writer using this handle before publishing. The [`Publication`]
    /// retains ownership of the temporary path.
    pub fn reopen(&self) -> std::io::Result<File> {
        self.temporary.reopen()
    }

    /// Sync the completed file, publish it, and on Unix sync the parent directory.
    ///
    /// The returned file is still open, but its cursor position is unspecified, so seek before
    /// reading. Callers must finish encoders and flush any external buffers before calling this.
    pub fn publish(self) -> Result<File, Error> {
        self.publish_with(File::sync_all, sync_parent)
    }

    fn publish_with(
        self,
        sync_file: impl FnOnce(&File) -> std::io::Result<()>,
        sync_directory: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> Result<File, Error> {
        let Self {
            temporary,
            destination,
            policy,
        } = self;
        sync_file(temporary.as_file()).map_err(|source| Error {
            kind: ErrorKind::FileSync,
            path: destination.clone(),
            source,
        })?;
        let file = match policy {
            Policy::CreateNew => temporary.persist_noclobber(&destination),
            Policy::Replace => temporary.persist(&destination),
        }
        .map_err(|error| Error {
            kind: ErrorKind::Persist,
            path: destination.clone(),
            source: error.error,
        })?;
        sync_directory(parent(&destination)).map_err(|source| Error {
            kind: ErrorKind::DirectorySync,
            path: destination,
            source,
        })?;
        Ok(file)
    }
}

impl Write for Publication {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.temporary.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.temporary.flush()
    }
}

impl Seek for Publication {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.temporary.seek(position)
    }
}

fn parent(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new("."))
}

fn check_destination(path: &Path, policy: Policy) -> std::io::Result<()> {
    if policy == Policy::CreateNew {
        match path.symlink_metadata() {
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "destination exists",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn sync_parent(directory: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

#[cfg(test)]
mod tests;
