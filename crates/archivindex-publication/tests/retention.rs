//! Partial output survives writer failures and process interruption when retention is enabled.

use std::io::Write;

use archivindex_publication::{Policy, Publication};

#[test]
fn named_partial_survives_errors_and_refuses_reuse() -> Result<(), Box<dyn std::error::Error>> {
    for kind in [
        std::io::ErrorKind::InvalidData,
        std::io::ErrorKind::Interrupted,
    ] {
        let dir = tempfile::tempdir()?;
        let target = dir.path().join("output.warc");
        let partial = dir.path().join("output.warc.partial");
        std::fs::write(&target, b"original")?;
        let result = (|| -> std::io::Result<()> {
            let mut pending = Publication::with_partial_path(&target, &partial, Policy::Replace)?
                .retain_partial();
            pending.write_all(b"completed records")?;
            Err(std::io::Error::from(kind))
        })();
        assert_eq!(result.unwrap_err().kind(), kind);
        assert_eq!(std::fs::read(&partial)?, b"completed records");
        assert_eq!(std::fs::read(&target)?, b"original");
        assert!(Publication::with_partial_path(&target, &partial, Policy::Replace).is_err());
        assert_eq!(std::fs::read(&partial)?, b"completed records");
    }
    Ok(())
}

#[test]
fn generated_and_adopted_partials_survive_panic_unwinding() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("output");
    for pending in [
        Publication::new(&target, Policy::CreateNew)?,
        Publication::from_temporary(
            &target,
            tempfile::NamedTempFile::new_in(dir.path())?,
            Policy::CreateNew,
        )?,
    ] {
        let mut pending = pending.retain_partial();
        let temporary = pending.temporary_path().to_owned();
        pending.write_all(b"completed records")?;
        let result = std::panic::catch_unwind(move || {
            let _pending = pending;
            panic!("interrupted writer");
        });
        assert!(result.is_err());
        assert_eq!(std::fs::read(&temporary)?, b"completed records");
        assert!(!target.exists());
    }
    Ok(())
}

#[test]
fn retained_partial_is_still_published() -> Result<(), Box<dyn std::error::Error>> {
    for policy in [Policy::CreateNew, Policy::Replace] {
        let dir = tempfile::tempdir()?;
        let target = dir.path().join("output");
        let partial = dir.path().join("output.partial");
        if policy == Policy::Replace {
            std::fs::write(&target, b"original")?;
        }
        let mut pending =
            Publication::with_partial_path(&target, &partial, policy)?.retain_partial();
        pending.write_all(b"complete")?;
        drop(pending.publish()?);
        assert!(!partial.exists());
        assert_eq!(std::fs::read(&target)?, b"complete");
    }
    Ok(())
}

// Deliver SIGINT to a separate process so the test runner itself is not interrupted.
#[cfg(unix)]
#[test]
fn named_partial_survives_sigint() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{BufRead as _, Read as _};
    use std::os::unix::process::ExitStatusExt as _;
    use std::process::{Command, Stdio};

    const CHILD_DIRECTORY: &str = "ARCHIVINDEX_PUBLICATION_SIGINT_TEST_DIRECTORY";
    if let Some(directory) = std::env::var_os(CHILD_DIRECTORY) {
        let directory = std::path::Path::new(&directory);
        let mut pending = Publication::with_partial_path(
            directory.join("output.warc"),
            directory.join("output.warc.partial"),
            Policy::CreateNew,
        )?
        .retain_partial();
        pending.write_all(b"completed records")?;
        writeln!(std::io::stdout(), "ready")?;
        std::io::stdout().flush()?;
        // The parent keeps stdin open until it has sent the signal.
        std::io::stdin().read_exact(&mut [0])?;
        panic!("the child should have been interrupted");
    }

    let dir = tempfile::tempdir()?;
    let mut child = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "named_partial_survives_sigint",
            "--nocapture",
            "--format=terse",
        ])
        .env(CHILD_DIRECTORY, dir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        assert_ne!(
            output.read_line(&mut line)?,
            0,
            "child exited before writing"
        );
        if line.trim_end() == "ready" {
            break;
        }
        line.clear();
    }
    let delivered = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()?;
    // Let the child exit even if SIGINT was unexpectedly ignored.
    drop(child.stdin.take());
    let status = child.wait()?;
    assert!(delivered.success());
    assert_eq!(status.signal(), Some(2));
    assert_eq!(
        std::fs::read(dir.path().join("output.warc.partial"))?,
        b"completed records"
    );
    assert!(!dir.path().join("output.warc").exists());
    Ok(())
}
