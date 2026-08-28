use std::io::Write;

use super::{Error, ErrorKind, Policy, Publication};

fn failed_sync(_: &std::fs::File) -> std::io::Result<()> {
    Err(std::io::Error::other("injected file sync failure"))
}

#[test]
fn replacement_is_invisible_until_publish() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("output");
    std::fs::write(&target, b"old")?;
    let mut pending = Publication::new(&target, Policy::Replace)?;
    let temporary = pending.temporary_path().to_owned();
    pending.write_all(b"new")?;
    assert_eq!(std::fs::read(&target)?, b"old");
    pending.publish()?;
    assert_eq!(std::fs::read(&target)?, b"new");
    assert!(!temporary.exists());
    Ok(())
}

#[test]
fn concurrent_creators_never_overwrite_the_winner() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("output");
    let mut first = Publication::new(&target, Policy::CreateNew)?;
    let mut second = Publication::new(&target, Policy::CreateNew)?;
    let second_temp = second.temporary_path().to_owned();
    first.write_all(b"winner")?;
    second.write_all(b"loser")?;
    assert!(!target.exists());
    first.publish()?;
    let error = second.publish().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Persist);
    assert_eq!(error.path, target);
    assert!(!error.is_published());
    assert_eq!(error.source.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&target)?, b"winner");
    assert!(!second_temp.exists());
    assert!(Publication::new(&target, Policy::CreateNew).is_err());
    Ok(())
}

#[test]
fn abandoning_or_failing_before_publication_preserves_the_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("output");
    for policy in [Policy::CreateNew, Policy::Replace] {
        if policy == Policy::Replace {
            std::fs::write(&target, b"original")?;
        }
        let mut pending = Publication::new(&target, policy)?;
        let temporary = pending.temporary_path().to_owned();
        pending.write_all(b"unfinished")?;
        drop(pending);
        assert!(!temporary.exists());
        let pending = Publication::new(&target, policy)?;
        let temporary = pending.temporary_path().to_owned();
        let error = pending
            .publish_with(failed_sync, |_| panic!("must not sync directory"))
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::FileSync);
        assert_eq!(error.path, target);
        assert!(!error.is_published());
        assert!(!temporary.exists());
        if policy == Policy::Replace {
            assert_eq!(std::fs::read(&target)?, b"original");
        } else {
            assert!(!target.exists());
        }
    }
    Ok(())
}

#[test]
fn directory_sync_failure_retains_published_output_and_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    for policy in [Policy::CreateNew, Policy::Replace] {
        let target = dir.path().join(format!("{policy:?}"));
        let mut pending = Publication::new(&target, policy)?;
        let temporary = pending.temporary_path().to_owned();
        pending.write_all(b"complete")?;
        let error = pending
            .publish_with(std::fs::File::sync_all, |_| {
                Err(std::io::Error::other("injected directory sync failure"))
            })
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::DirectorySync);
        assert_eq!(error.path, target);
        assert!(error.is_published());
        assert!(!temporary.exists());
        assert_eq!(std::fs::read(&target)?, b"complete");
        let wrapped = std::io::Error::from(error);
        assert!(
            wrapped
                .get_ref()
                .unwrap()
                .downcast_ref::<Error>()
                .unwrap()
                .is_published()
        );
        assert!(wrapped.to_string().contains("published"));
    }
    Ok(())
}

#[test]
fn named_partial_is_exclusive_and_cleaned_up() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("output");
    let partial = dir.path().join("output.partial");
    std::fs::write(&partial, b"do not truncate")?;
    assert!(Publication::with_partial_path(&target, &partial, Policy::Replace).is_err());
    assert_eq!(std::fs::read(&partial)?, b"do not truncate");
    std::fs::remove_file(&partial)?;
    let pending = Publication::with_partial_path(&target, &partial, Policy::Replace)?;
    pending.reopen()?.write_all(b"finished encoder")?;
    pending.publish()?;
    assert_eq!(std::fs::read(&target)?, b"finished encoder");
    assert!(!partial.exists());
    assert!(Publication::with_partial_path(&target, &target, Policy::Replace).is_err());
    assert_eq!(std::fs::read(&target)?, b"finished encoder");
    Ok(())
}

#[test]
fn persistence_failure_cleans_temporary_and_retains_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("directory");
    std::fs::create_dir(&target)?;
    std::fs::write(target.join("child"), b"preserve")?;
    let pending = Publication::new(&target, Policy::Replace)?;
    let temporary = pending.temporary_path().to_owned();
    assert_eq!(pending.publish().unwrap_err().kind, ErrorKind::Persist);
    assert!(!temporary.exists());
    assert_eq!(std::fs::read(target.join("child"))?, b"preserve");
    Ok(())
}

#[test]
fn adoption_requires_a_sibling_and_publishes_finalized_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("output");
    let mut temp = tempfile::NamedTempFile::new_in(dir.path())?;
    temp.write_all(b"finished")?;
    Publication::from_temporary(&target, temp, Policy::CreateNew)?.publish()?;
    assert_eq!(std::fs::read(&target)?, b"finished");
    let other = tempfile::tempdir()?;
    let temp = tempfile::NamedTempFile::new_in(other.path())?;
    let path = temp.path().to_owned();
    assert!(Publication::from_temporary(&target, temp, Policy::Replace).is_err());
    assert!(!path.exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn links_are_never_followed_to_truncate_files() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let dir = tempfile::tempdir()?;
    let original = dir.path().join("original");
    std::fs::write(&original, b"original")?;
    let target = dir.path().join("output");
    symlink(&original, &target)?;
    assert!(Publication::new(&target, Policy::CreateNew).is_err());
    let partial = dir.path().join("output.partial");
    symlink(&original, &partial)?;
    assert!(Publication::with_partial_path(&target, &partial, Policy::Replace).is_err());
    let mut pending = Publication::new(&target, Policy::Replace)?;
    pending.write_all(b"replacement")?;
    pending.publish()?;
    assert_eq!(std::fs::read(&original)?, b"original");
    assert_eq!(std::fs::read(&target)?, b"replacement");
    assert_eq!(
        std::fs::metadata(&target)?.permissions().mode() & 0o777,
        0o600
    );
    let dangling = dir.path().join("dangling");
    symlink(dir.path().join("absent"), &dangling)?;
    assert!(Publication::new(&dangling, Policy::CreateNew).is_err());
    Ok(())
}

#[test]
fn error_messages_and_sources_survive_io_conversion() {
    for (kind, message) in [
        (ErrorKind::FileSync, "cannot sync pending output for output"),
        (ErrorKind::Persist, "cannot publish output"),
        (
            ErrorKind::DirectorySync,
            "published output, but could not sync its directory",
        ),
    ] {
        let error = Error {
            kind,
            path: "output".into(),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        assert_eq!(error.to_string(), message);
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert!(std::ptr::eq(source, &raw const error.source));
        let wrapped = std::io::Error::from(error);
        assert_eq!(wrapped.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(wrapped.to_string(), message);
        let original = wrapped.get_ref().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(original.kind, kind);
        assert_eq!(original.path, std::path::Path::new("output"));
        assert_eq!(original.source.kind(), std::io::ErrorKind::PermissionDenied);
    }
}

#[test]
fn retained_output_survives_failures_before_publication() -> Result<(), Box<dyn std::error::Error>>
{
    for policy in [Policy::CreateNew, Policy::Replace] {
        for kind in [
            ErrorKind::FileSync,
            ErrorKind::Persist,
            ErrorKind::DirectorySync,
        ] {
            let dir = tempfile::tempdir()?;
            let target = dir.path().join("output");
            let partial = dir.path().join("output.partial");
            let mut pending =
                Publication::with_partial_path(&target, &partial, policy)?.retain_partial();
            pending.write_all(b"completed records")?;
            if kind == ErrorKind::Persist {
                // An occupied directory prevents either policy from publishing the file.
                std::fs::create_dir(&target)?;
                std::fs::write(target.join("child"), b"preserve")?;
            } else if policy == Policy::Replace {
                std::fs::write(&target, b"original")?;
            }
            let error = pending
                .publish_with(
                    |file| {
                        if kind == ErrorKind::FileSync {
                            failed_sync(file)
                        } else {
                            file.sync_all()
                        }
                    },
                    |_| Err(std::io::Error::other("injected directory sync failure")),
                )
                .unwrap_err();
            assert_eq!(error.kind, kind);
            if kind == ErrorKind::DirectorySync {
                assert!(!partial.exists());
                assert_eq!(std::fs::read(&target)?, b"completed records");
            } else {
                assert_eq!(std::fs::read(&partial)?, b"completed records");
                if kind == ErrorKind::Persist {
                    assert_eq!(std::fs::read(target.join("child"))?, b"preserve");
                } else if policy == Policy::Replace {
                    assert_eq!(std::fs::read(&target)?, b"original");
                } else {
                    assert!(!target.exists());
                }
            }
        }
    }
    Ok(())
}
