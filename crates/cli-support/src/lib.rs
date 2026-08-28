//! Command-line options, configuration files, logging, progress, exit statuses, formatting, and
//! interrupt handling shared by Archivindex tools.

pub mod config;
pub mod progress;

use std::fmt::Display;
use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};

/// A command result represented by its process exit status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CommandOutcome {
    /// The command completed successfully.
    Success = 0,
    /// The command completed but found reportable problems in its input.
    ReportedProblems = 1,
    /// The command failed because of an operational error.
    OperationalError = 2,
}

impl CommandOutcome {
    /// [`Self::ReportedProblems`] when problems were found, [`Self::Success`] otherwise.
    #[must_use]
    pub const fn from_reported_problems(found: bool) -> Self {
        if found {
            Self::ReportedProblems
        } else {
            Self::Success
        }
    }
}

impl From<CommandOutcome> for ExitCode {
    fn from(outcome: CommandOutcome) -> Self {
        Self::from(outcome as u8)
    }
}

/// Convert a command result to an exit code, logging operational errors.
///
/// Errors are logged with alternate [`Display`] formatting before exit status 2 is returned.
pub fn exit_code<E: Display>(result: Result<CommandOutcome, E>) -> ExitCode {
    match result {
        Ok(outcome) => outcome.into(),
        Err(error) => {
            log::error!("{error:#}");
            CommandOutcome::OperationalError.into()
        }
    }
}

/// Set by the first interrupt or termination signal; never cleared.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Install a signal handler and return the process-wide interrupt flag.
///
/// The first interrupt or termination signal sets the flag. Callers can poll it with
/// [`Ordering::Relaxed`] and stop between steps. A second signal exits immediately with status 130,
/// so a run whose current step never returns can still be stopped.
///
/// Installation is attempted only on the first call. If it fails, this function logs a warning and
/// the flag remains unset.
pub fn interrupt_flag() -> &'static AtomicBool {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let installed = ctrlc::set_handler(|| {
            if INTERRUPTED.swap(true, Ordering::Relaxed) {
                std::process::exit(130);
            }
        });
        if let Err(error) = installed {
            log::warn!("failed to install signal handler; shutdown may not be clean: {error}");
        }
    });

    &INTERRUPTED
}

/// A count followed by `noun`, with `s` appended unless the count is one.
///
/// Use only with nouns whose plural is formed by adding `s`.
#[must_use]
pub fn plural<N: Copy + Display + From<u8> + PartialEq>(count: N, noun: &str) -> String {
    let suffix = if count == N::from(1) { "" } else { "s" };
    format!("{count} {noun}{suffix}")
}

/// Logging detail selected by `--quiet` or repeated `-v` flags.
///
/// The default is warnings; `-v` enables information, `-vv` debug output, and `-vvv` trace output.
/// `--quiet` logs only errors. Commands can use [`Verbosity::is_quiet`] to suppress their own
/// output.
#[derive(Debug, clap::Args)]
// Without this, clap would describe every command that flattens `Verbosity` with the doc comment
// above, since a flattened `Args` struct contributes its own description.
#[command(about = None, long_about = None)]
pub struct Verbosity {
    /// Log errors only.
    #[arg(short, long, global = true, conflicts_with = "verbose")]
    quiet: bool,

    /// Log informational diagnostics; repeat twice for debug or three times for trace.
    #[arg(short, global = true, action = clap::ArgAction::Count)]
    verbose: u8,
}

impl Verbosity {
    /// Whether quiet mode was requested. Callers decide which output to suppress.
    #[must_use]
    pub const fn is_quiet(&self) -> bool {
        self.quiet
    }

    /// Whether informational or more detailed diagnostics are logged.
    #[must_use]
    pub const fn is_verbose(&self) -> bool {
        self.verbose > 0
    }

    /// The most detailed level to log.
    #[must_use]
    pub const fn level(&self) -> log::LevelFilter {
        if self.quiet {
            log::LevelFilter::Error
        } else {
            match self.verbose {
                0 => log::LevelFilter::Warn,
                1 => log::LevelFilter::Info,
                2 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            }
        }
    }

    /// Initialize standard-error logging at the selected level, enabling color when stderr is a
    /// terminal.
    ///
    /// # Panics
    ///
    /// Panics if a logger has already been installed in this process.
    pub fn init_logging(&self) {
        let config = simplelog::ConfigBuilder::new()
            .set_time_level(log::LevelFilter::Off)
            .build();

        simplelog::TermLogger::init(
            self.level(),
            config,
            simplelog::TerminalMode::Stderr,
            if std::io::stderr().is_terminal() {
                simplelog::ColorChoice::Auto
            } else {
                simplelog::ColorChoice::Never
            },
        )
        .expect("invariant violation: the logger is initialized once");
    }
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use clap::Parser;

    use super::{CommandOutcome, Verbosity};

    #[derive(Debug, Parser)]
    struct Cli {
        #[command(flatten)]
        verbosity: Verbosity,
    }

    fn verbosity(flags: &[&str]) -> Verbosity {
        let mut args = vec!["test"];
        args.extend_from_slice(flags);

        Cli::try_parse_from(args).expect("valid options").verbosity
    }

    #[test]
    fn selects_the_documented_levels() {
        assert_eq!(verbosity(&[]).level(), log::LevelFilter::Warn);
        assert_eq!(verbosity(&["--quiet"]).level(), log::LevelFilter::Error);
        assert_eq!(verbosity(&["-v"]).level(), log::LevelFilter::Info);
        assert_eq!(verbosity(&["-vv"]).level(), log::LevelFilter::Debug);
        assert_eq!(verbosity(&["-vvv"]).level(), log::LevelFilter::Trace);
        assert_eq!(verbosity(&["-vvvv"]).level(), log::LevelFilter::Trace);
    }

    #[test]
    fn reports_quiet_and_verbose_separately() {
        assert!(verbosity(&["-q"]).is_quiet());
        assert!(!verbosity(&["-q"]).is_verbose());
        assert!(!verbosity(&[]).is_quiet());
        assert!(!verbosity(&[]).is_verbose());
        assert!(verbosity(&["-v"]).is_verbose());
    }

    /// The two options describe opposite intents, so asking for both is an error.
    #[test]
    fn quiet_and_verbose_conflict() {
        assert!(Cli::try_parse_from(["test", "-q", "-v"]).is_err());
    }

    // The process-wide flag cannot be reset, so one test covers its entire lifecycle.
    #[test]
    fn the_interrupt_flag_is_shared_and_set_by_a_signal() {
        let flag = super::interrupt_flag();

        assert!(std::ptr::eq(flag, super::interrupt_flag()));
        assert!(!flag.load(Ordering::Relaxed));

        #[cfg(unix)]
        {
            let delivered = std::process::Command::new("kill")
                .args(["-INT", &std::process::id().to_string()])
                .status()
                .expect("a kill command");
            assert!(delivered.success());

            let deadline = Instant::now() + Duration::from_secs(5);
            while !flag.load(Ordering::Relaxed) {
                assert!(
                    Instant::now() < deadline,
                    "the signal did not set the interrupt flag"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    #[test]
    fn plural_agrees_with_its_count() {
        assert_eq!(super::plural(0_usize, "record"), "0 records");
        assert_eq!(super::plural(1_usize, "record"), "1 record");
        assert_eq!(super::plural(2_u64, "byte"), "2 bytes");
    }

    #[test]
    fn outcomes_have_the_documented_exit_codes() {
        assert_eq!(ExitCode::from(CommandOutcome::Success), ExitCode::from(0));
        assert_eq!(
            ExitCode::from(CommandOutcome::ReportedProblems),
            ExitCode::from(1)
        );
        assert_eq!(
            ExitCode::from(CommandOutcome::OperationalError),
            ExitCode::from(2)
        );
    }

    #[test]
    fn reported_problems_select_the_outcome() {
        assert_eq!(
            CommandOutcome::from_reported_problems(false),
            CommandOutcome::Success
        );
        assert_eq!(
            CommandOutcome::from_reported_problems(true),
            CommandOutcome::ReportedProblems
        );
    }
}
