//! Publish complete files with explicit overwrite and durability guarantees.
//!
//! A [`Publication`] owns a temporary file in the destination directory. Dropping it before
//! publication removes that file on a best-effort basis and leaves the destination alone. The
//! destination is not reserved: concurrent [`Policy::CreateNew`] writers may prepare their output,
//! but only one can publish. A process crash may leave temporary files behind.
//!
//! Callers must finish encoders and flush any buffers before [`Publication::publish`]. This crate
//! synchronizes the file, persists it under the chosen policy, then synchronizes the parent
//! directory on Unix. Other platforms get the file sync and persistence operation but no directory
//! sync guarantee. The parent directory must already exist; durability of newly created ancestor
//! directories is the caller's responsibility. These guarantees assume a filesystem supporting the
//! corresponding operations and a directory not modified by an adversary during publication.
//!
//! [`Error::DirectorySync`] means publication succeeded but durability could not be confirmed. The
//! published file is retained on that error. Retrying blindly could overwrite valid output.
//! Failures before publication leave an existing destination untouched.
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
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tempfile::{NamedTempFile, TempPath};

/// What to do with a destination that already exists at publication time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Policy {
    /// Refuse to replace any existing directory entry, including a dangling symlink.
    CreateNew,
    /// Atomically replace the destination entry, leaving any link target untouched.
    Replace,
}

/// A failure synchronizing or publishing a completed file.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// File synchronization failed before the destination was changed.
    #[error("cannot sync pending output for {}", path.display())]
    FileSync {
        /// Destination that was not published.
        path: PathBuf,
        /// The filesystem error.
        #[source]
        source: io::Error,
    },
    /// Persistence failed before the destination was changed.
    #[error("cannot publish {}", path.display())]
    Persist {
        /// Destination that was not published.
        path: PathBuf,
        /// The filesystem error.
        #[source]
        source: io::Error,
    },
    /// The destination is complete and visible, but its directory could not be synchronized.
    #[error("published {}, but could not sync its directory", path.display())]
    DirectorySync {
        /// Published destination; it is not removed on this error.
        path: PathBuf,
        /// The filesystem error.
        #[source]
        source: io::Error,
    },
}

impl Error {
    /// Whether the completed output was made visible before this error occurred.
    #[must_use]
    pub const fn is_published(&self) -> bool {
        matches!(self, Self::DirectorySync { .. })
    }

    /// The underlying filesystem error.
    #[must_use]
    pub const fn io_error(&self) -> &io::Error {
        match self {
            Self::FileSync { source, .. }
            | Self::Persist { source, .. }
            | Self::DirectorySync { source, .. } => source,
        }
    }
}

/// Preserve the stage and source for APIs that expose only I/O errors.
///
/// The original [`Error`] can be recovered with [`io::Error::get_ref`] and `downcast_ref`.
impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        Self::new(error.io_error().kind(), error)
    }
}

/// An owned temporary file and the policy for publishing it.
///
/// Its unique temporary name ends in `.tmp`, so directory consumers can ignore leftovers. Named
/// partial files can instead be created with [`Self::with_partial_path`]. Neither constructor
/// truncates an existing temporary file. On Unix, newly created files have owner-only permissions,
/// including after publication.
#[derive(Debug)]
pub struct Publication {
    temporary: NamedTempFile,
    destination: PathBuf,
    policy: Policy,
}

impl Publication {
    /// Prepare a uniquely named temporary sibling of `destination`.
    ///
    /// `CreateNew` rejects an occupied destination early as a convenience, but does not reserve it:
    /// the authoritative no-overwrite check happens when publishing.
    pub fn new(destination: impl AsRef<Path>, policy: Policy) -> io::Result<Self> {
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

    /// Exclusively create a caller-named partial file in the destination directory.
    ///
    /// This supports visible `.partial` files and refuses to reuse files left by interrupted runs.
    /// A partial path outside the destination directory is rejected. The paths must be distinct.
    /// Both are made absolute so changing the working directory cannot retarget them.
    pub fn with_partial_path(
        destination: impl AsRef<Path>,
        partial: impl AsRef<Path>,
        policy: Policy,
    ) -> io::Result<Self> {
        let destination = std::path::absolute(destination)?;
        let partial = std::path::absolute(partial)?;
        if destination == partial || parent(&destination) != parent(&partial) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
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
    /// The paths must be distinct. Existing file permissions are retained. On error, dropping the
    /// supplied temporary file attempts to remove its path.
    pub fn from_temporary(
        destination: impl AsRef<Path>,
        temporary: NamedTempFile,
        policy: Policy,
    ) -> io::Result<Self> {
        let destination = std::path::absolute(destination)?;
        let temporary_path = std::path::absolute(temporary.path())?;
        if destination == temporary_path || parent(&destination) != parent(&temporary_path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary file must be a distinct sibling",
            ));
        }
        Ok(Self {
            temporary,
            destination,
            policy,
        })
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
    pub fn reopen(&self) -> io::Result<File> {
        self.temporary.reopen()
    }

    /// Sync the completed file, publish it, and on Unix sync the parent directory.
    ///
    /// The returned file is still open. Its cursor position is unspecified; seek before reading. No
    /// format finalization or external buffer flushing is performed here.
    pub fn publish(self) -> Result<File, Error> {
        self.publish_with(File::sync_all, sync_parent)
    }

    fn publish_with(
        self,
        sync_file: impl FnOnce(&File) -> io::Result<()>,
        sync_directory: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<File, Error> {
        let Self {
            temporary,
            destination,
            policy,
        } = self;
        sync_file(temporary.as_file()).map_err(|source| Error::FileSync {
            path: destination.clone(),
            source,
        })?;
        let file = match policy {
            Policy::CreateNew => temporary.persist_noclobber(&destination),
            Policy::Replace => temporary.persist(&destination),
        }
        .map_err(|error| Error::Persist {
            path: destination.clone(),
            source: error.error,
        })?;
        sync_directory(parent(&destination)).map_err(|source| Error::DirectorySync {
            path: destination,
            source,
        })?;
        Ok(file)
    }
}

impl Write for Publication {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.temporary.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.temporary.flush()
    }
}

impl Seek for Publication {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.temporary.seek(position)
    }
}

fn parent(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new("."))
}

fn check_destination(path: &Path, policy: Policy) -> io::Result<()> {
    if policy == Policy::CreateNew {
        match path.symlink_metadata() {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "destination exists",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn sync_parent(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

#[cfg(test)]
mod tests;
