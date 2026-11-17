//! Command-line diagnostics and exit statuses.

use std::path::Path;
use std::process::{Command, Output};

fn check(manifest: &Path, flags: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cargo-archivindex-build"))
        .args(flags)
        .args(["check", "--manifest-path"])
        .arg(manifest)
        .output()
        .expect("the command runs")
}

/// Subscriber initialization must preserve stderr output and quiet-mode filtering.
#[test]
fn policy_diagnostics_go_to_stderr_and_respect_quiet() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .unwrap();
    std::fs::create_dir(directory.path().join("src")).unwrap();
    std::fs::write(directory.path().join("src/lib.rs"), "").unwrap();

    for flags in [&[][..], &["--quiet"][..]] {
        let output = check(&manifest, flags);
        let stderr = String::from_utf8(output.stderr).unwrap();

        assert_eq!(output.status.code(), Some(1), "{stderr}");
        assert_eq!(output.stdout, b"");
        assert!(!stderr.contains('\u{1b}'));
        assert_eq!(stderr.contains("WARN"), flags.is_empty(), "{stderr}");
        assert!(stderr.contains("ERROR"), "{stderr}");
        assert!(stderr.contains("policy violations found"), "{stderr}");
    }
}

/// Operational errors must remain visible even when warnings are suppressed.
#[test]
fn operational_errors_are_reported_in_quiet_mode() {
    let directory = tempfile::tempdir().unwrap();
    let manifest = directory.path().join("Cargo.toml");
    let output = check(&manifest, &["--quiet"]);
    let stderr = String::from_utf8(output.stderr).unwrap();

    assert_eq!(output.status.code(), Some(2), "{stderr}");
    assert_eq!(output.stdout, b"");
    assert!(!stderr.contains('\u{1b}'));
    assert!(stderr.contains("ERROR"), "{stderr}");
    assert!(stderr.contains("Cargo.toml"), "{stderr}");
}
