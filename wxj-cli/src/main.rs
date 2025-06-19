use archivindex_wbm::digest::Sha1Digest;
use archivindex_wxj::lines::{Snapshot, SnapshotLine};
use birdsite::model::wxj::{TweetSnapshot, data, flat};
use cli_helpers::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

mod cdx;
mod snapshot;

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
            const FLAT_FILE_NAME: &str = "flat.ndjson.zst";
            const DATA_FILE_NAME: &str = "data.ndjson.zst";

            let snapshot::SnapshotImport {
                paths,
                skipped,
                invalid_digests,
            } = snapshot::snapshot_import(&snapshots)?;

            for path in skipped {
                log::info!("Skipped: {}", path.as_os_str().to_string_lossy());
            }

            for (expected, found) in invalid_digests {
                log::warn!("Invalid digest: {} instead of {}", found, expected);
            }

            log::info!("Prepared {} files", paths.len());

            let mut flat_input =
                archivindex_wxj::lines::io::SnapshotReader::open(input.join(FLAT_FILE_NAME))?
                    .peekable();
            let mut data_input =
                archivindex_wxj::lines::io::SnapshotReader::open(input.join(DATA_FILE_NAME))?
                    .peekable();

            std::fs::create_dir_all(&output)?;

            let mut flat_output = archivindex_wxj::lines::io::SnapshotWriter::create(
                output.join(FLAT_FILE_NAME),
                compression,
            )?;

            let mut data_output = archivindex_wxj::lines::io::SnapshotWriter::create(
                output.join(DATA_FILE_NAME),
                compression,
            )?;

            for (digest, path, _) in paths {
                let mut flat_next = flat_input
                    .peek()
                    .and_then(|result| result.as_ref().ok())
                    .map(|snapshot| snapshot.digest);

                let mut data_next = data_input
                    .peek()
                    .and_then(|result| result.as_ref().ok())
                    .map(|snapshot| snapshot.digest);

                while flat_next
                    .map(|flat_digest| flat_digest < digest)
                    .unwrap_or(false)
                {
                    // We can unwrap safely because of the peek.
                    let snapshot = flat_input.next().unwrap()?;
                    flat_output.write_snapshot(&snapshot)?;
                    flat_next = flat_input
                        .peek()
                        .and_then(|result| result.as_ref().ok())
                        .map(|snapshot| snapshot.digest);
                }

                while data_next
                    .map(|data_digest| data_digest < digest)
                    .unwrap_or(false)
                {
                    // We can unwrap safely because of the peek.
                    let snapshot = data_input.next().unwrap()?;
                    data_output.write_snapshot(&snapshot)?;
                    data_next = data_input
                        .peek()
                        .and_then(|result| result.as_ref().ok())
                        .map(|snapshot| snapshot.digest);
                }

                if flat_next == Some(digest) {
                    // We can unwrap safely because of the peek.
                    let snapshot = flat_input.next().unwrap()?;
                    flat_output.write_snapshot(&snapshot)?;
                } else if data_next == Some(digest) {
                    // We can unwrap safely because of the peek.
                    let snapshot = data_input.next().unwrap()?;
                    data_output.write_snapshot(&snapshot)?;
                } else {
                    match std::fs::read_to_string(&path).map_err(|error| Error::FileIo(path, error))
                    {
                        Ok(content) => {
                            let bytes = content.as_bytes();

                            let trimmed = content.trim();

                            if !trimmed.contains(['\n', '\r']) {
                                if content.starts_with("{\"created_at\":") {
                                    flat_output.write(digest, bytes)?;
                                } else if content.starts_with("{\"data\":") {
                                    data_output.write(digest, bytes)?;
                                } else {
                                    log::info!("Skipped: {}", digest);
                                }
                            } else {
                                log::info!("Skipped because not single line: {}", digest);
                            }
                        }
                        Err(error) => {
                            log::info!("File I/O error: {:?}", error);
                        }
                    }
                }
            }

            for snapshot_line in flat_input {
                flat_output.write_snapshot(&snapshot_line?)?;
            }

            for snapshot_line in data_input {
                data_output.write_snapshot(&snapshot_line?)?;
            }

            flat_output.finish()?;
            data_output.finish()?;
        }
        Command::TweetIds { input, flat } => {
            let reader = BufReader::new(zstd::Decoder::new(File::open(&input)?)?);

            for line in reader.lines() {
                let line = line?;

                let snapshot = if flat {
                    serde_json::from_str::<Snapshot<flat::TweetSnapshot>>(&line)?
                        .map_content(TweetSnapshot::Flat)
                } else {
                    serde_json::from_str::<Snapshot<data::TweetSnapshot>>(&line)?
                        .map_content(TweetSnapshot::Data)
                };

                let metadata =
                    birdsite::model::metadata::tweet::TweetMetadata::from_tweet_snapshot(
                        &snapshot.content,
                    )?;

                for tweet in metadata {
                    println!("{},{}", tweet.user.id, tweet.id);
                }
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("File I/O error")]
    FileIo(PathBuf, std::io::Error),
    #[error("CLI argument reading error")]
    Args(#[from] cli_helpers::Error),
    #[error("CSV error")]
    Csv(#[from] csv::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("WBM snapshot storage import error")]
    WbmCas(#[from] archivindex_wbm::cas::import::Error),
    #[error("WXJ line parsing error")]
    WxjLine(#[from] archivindex_wxj::lines::Error),
    #[error("WXJ data format error")]
    BirdsiteWxjDataFormat(#[from] birdsite::model::wxj::data::FormatError),
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
        #[clap(long, default_value = "14")]
        compression: u16,
    },
    TweetIds {
        #[clap(long)]
        input: PathBuf,
        #[clap(long)]
        flat: bool,
    },
}
