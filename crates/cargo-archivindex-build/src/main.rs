//! The `cargo archivindex-build` command-line entry point.

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use archivindex_cli_support::{CommandOutcome, Verbosity, plural};
use cargo_archivindex_build::{Error, SyncReport, Violation, check_project, sync_project};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "cargo archivindex-build",
    version,
    about = "Check and synchronize Archivindex Cargo workspace policy"
)]
struct Cli {
    #[command(flatten)]
    verbosity: Verbosity,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check the project without changing files.
    Check {
        /// Path to a Cargo manifest in the target workspace.
        #[arg(long, value_name = "PATH")]
        manifest_path: Option<PathBuf>,
    },
    /// Apply mechanical policy fixes, then check the project.
    Sync {
        /// Path to a Cargo manifest in the target workspace.
        #[arg(long, value_name = "PATH")]
        manifest_path: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    archivindex_cli_support::exit_code(run())
}

/// Run the selected command.
///
/// # Returns
///
/// [`CommandOutcome::ReportedProblems`] if the project violates the policy, and
/// [`CommandOutcome::Success`] otherwise.
///
/// # Errors
///
/// Returns an error if the workspace metadata cannot be read, or a project file cannot be read,
/// parsed, or written.
fn run() -> Result<CommandOutcome, Error> {
    // Cargo passes the subcommand's own name through as the first argument.
    let mut arguments: Vec<_> = env::args_os().collect();
    if arguments
        .get(1)
        .is_some_and(|argument| argument == "archivindex-build")
    {
        arguments.remove(1);
    }
    let cli = Cli::parse_from(arguments);
    cli.verbosity.init_logging();

    let outcome = match cli.command {
        Command::Check { manifest_path } => {
            let violations = check_project(manifest_path.as_deref())?;
            let outcome = report(&violations);

            if outcome == CommandOutcome::Success {
                println!("Archivindex project policy checks passed");
            }

            outcome
        }
        Command::Sync { manifest_path } => {
            let SyncReport {
                changed_files,
                violations,
            } = sync_project(manifest_path.as_deref())?;

            for path in &changed_files {
                println!("updated {}", path.display());
            }

            let outcome = report(&violations);

            if outcome == CommandOutcome::Success {
                println!(
                    "Archivindex project policy synchronized ({} changed)",
                    plural(changed_files.len(), "file")
                );
            }

            outcome
        }
    };

    Ok(outcome)
}

/// Log every violation, and report whether any was found.
fn report(violations: &[Violation]) -> CommandOutcome {
    for violation in violations {
        log::warn!("{violation}");
    }

    if !violations.is_empty() {
        log::error!("{} found", plural(violations.len(), "policy violation"));
    }

    CommandOutcome::from_reported_problems(!violations.is_empty())
}
