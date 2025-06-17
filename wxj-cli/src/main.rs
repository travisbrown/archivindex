use archivindex_wbm::digest::Sha1Digest;
use archivindex_wxj::lines::SnapshotLine;
use cli_helpers::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

mod cdx;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::Validate { input } => {
            let mut count = 0;
            let mut hasher = Default::default();

            for path in input {
                let reader = BufReader::new(zstd::Decoder::new(File::open(&path)?)?);
                log::info!("Reading file: {}", path.as_os_str().to_string_lossy());

                let mut last_digest = Sha1Digest::MIN;

                for line in reader.lines() {
                    let line = line?;

                    let snapshot_line = SnapshotLine::parse(&line)?;

                    if snapshot_line.digest <= last_digest {
                        log::error!("Out of order: {}", snapshot_line.digest);
                    }

                    last_digest = snapshot_line.digest;

                    if let Err(found_digest) = snapshot_line.validate(&mut hasher) {
                        log::error!(
                            "Invalid: expected {}, found {}",
                            snapshot_line.digest,
                            found_digest
                        );
                    } else {
                        count += 1;
                    }
                }
            }

            log::info!("{} valid", count);
        }
        Command::Incomplete { input } => {
            let mut count = 0;

            for path in input {
                let reader = BufReader::new(zstd::Decoder::new(File::open(&path)?)?);
                log::info!("Reading file: {}", path.as_os_str().to_string_lossy());

                for line in reader.lines() {
                    let line = line?;

                    let snapshot_line = SnapshotLine::parse(&line)?;

                    if snapshot_line.timestamp.is_none() {
                        count += 1;

                        println!("{}", snapshot_line.digest);
                    }
                }
            }

            log::info!("{} incomplete", count);
        }
        Command::Merge {
            input,
            snapshots,
            output,
            compression,
        } => {
            let importers = snapshots
                .into_iter()
                .map(archivindex_wbm::cas::import::Importer::new)
                .collect::<Vec<_>>();

            let mut files = importers
                .into_iter()
                .flat_map(|importer| {
                    importer.filter_map(|file| match file {
                        Ok(archivindex_wbm::cas::import::File::Valid {
                            digest,
                            path,
                            compression_type,
                        }) => Some((digest, path, compression_type)),
                        Ok(archivindex_wbm::cas::import::File::Skipped { path }) => {
                            log::info!("Skipped: {}", path.as_os_str().to_string_lossy());
                            None
                        }
                        Err(archivindex_wbm::cas::import::Error::InvalidDigest {
                            expected,
                            found,
                        }) => {
                            log::warn!("Invalid digest: {} instead of {}", found, expected);
                            None
                        }
                        Err(other) => {
                            panic!("{:?}", other);
                        }
                    })
                })
                .collect::<Vec<_>>();

            log::info!("Prepared {} files", files.len());

            files.sort_by_key(|(digest, _, _)| *digest);
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("CLI argument reading error")]
    Args(#[from] cli_helpers::Error),
    #[error("CSV error")]
    Csv(#[from] csv::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("WXJ line parsing error")]
    WxjLine(#[from] archivindex_wxj::lines::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "archivindex-wxj-cli", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Validate {
        #[clap(long)]
        input: Vec<PathBuf>,
    },
    Incomplete {
        #[clap(long)]
        input: Vec<PathBuf>,
    },
    Merge {
        #[clap(long)]
        input: PathBuf,
        #[clap(long)]
        snapshots: Vec<PathBuf>,
        #[clap(long)]
        output: PathBuf,
        #[clap(long)]
        compression: Option<u16>,
    },
}
