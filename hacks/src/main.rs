use archivindex_wbm::{
    cdx::{item::ItemList, mime_type::MimeType},
    digest,
    surt::Surt,
};
use archivindex_wxj::lines::{
    Snapshot, SnapshotLine,
    tweet::{TweetSnapshot as _, data::TweetSnapshot},
};
use cli_helpers::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

mod wxj;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::WxjUrls { input } => {
            let lines = BufReader::new(zstd::Decoder::new(File::open(input)?)?).lines();

            for line in lines {
                let line = line?;

                let snapshot_line = SnapshotLine::parse(&line)?;

                if snapshot_line.url.is_none() {
                    let snapshot = serde_json::from_str::<Snapshot<TweetSnapshot>>(&line)?;

                    match snapshot.content.canonical_url(false) {
                        Some(url) => {
                            println!("{},{}", snapshot_line.digest, url);
                        }
                        None => {
                            println!("{},", snapshot_line.digest);
                            log::error!("No canonical URL: {}", snapshot_line.digest);
                        }
                    }
                }
            }
        }
        Command::WxjEnhance {
            data,
            urls,
            cdx,
            invalid_digests,
        } => {
            let url_paths = wxj::read_url_paths(urls)?;
            log::info!("{} URL path entries", url_paths.len());

            let mut digest_metadata = wxj::read_cdx(cdx, &url_paths)?;
            log::info!("{} digest metadata entries", digest_metadata.len());

            wxj::read_invalid_digests(invalid_digests, &url_paths, &mut digest_metadata)?;
            log::info!("{} digest metadata entries", digest_metadata.len());

            let inferred_urls = digest_metadata
                .values()
                .filter(|digest_metadata| digest_metadata.url_path.is_none())
                .count();

            log::info!(
                "{} inferred ({}%)",
                inferred_urls,
                (inferred_urls as f64) * 100.0 / digest_metadata.len() as f64
            );

            for (digest, uninferred) in
                digest_metadata
                    .iter()
                    .filter_map(|(digest, digest_metadata)| {
                        digest_metadata
                            .url_path
                            .as_ref()
                            .map(|url_path| (digest, url_path))
                    })
            {
                println!("{}: {}", digest, uninferred);
            }
        }
        Command::ValidatedWxjLines { input } => {
            let validation = if input.as_os_str().to_string_lossy().ends_with("zst") {
                let lines = BufReader::new(zstd::Decoder::new(File::open(input)?)?).lines();

                archivindex_wxj::lines::SnapshotLine::validate_lines(lines)
            } else {
                let lines = BufReader::new(File::open(input)?).lines();

                archivindex_wxj::lines::SnapshotLine::validate_lines(lines)
            }?;

            println!("Successful: {}", validation.valid_count);
            println!("Invalid lines: {}", validation.invalid_lines.len());
            println!(
                "Unexpected digests: {}",
                validation.unexpected_digests.len()
            );
            println!("Out of order lines: {}", validation.out_of_order.len());
        }
        Command::CdxList { base } => {
            for path in wxj::cdx_files(base).unwrap() {
                println!("{:?}", path);
            }
        }
        Command::CheckSurts { input } => {
            let cdx_paths = find_cdx_files(input)?;
            let mut success_count = 0;
            let mut failure_count = 0;

            for path in cdx_paths {
                let contents = std::fs::read_to_string(&path)?;

                match serde_json::from_str::<ItemList>(&contents) {
                    Ok(items) => {
                        for item in items.values {
                            if item.mime_type == MimeType::ApplicationJson {
                                let converted_surt = Surt::from_url(&item.original)?;

                                if converted_surt == item.key {
                                    success_count += 1;
                                } else {
                                    log::error!(
                                        "Invalid conversion in {:?}:\nConverted: {}\nOriginal:  {}",
                                        path,
                                        converted_surt,
                                        item.key
                                    );

                                    failure_count += 1;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        log::error!("At {:?}: {:?}", path, error);
                    }
                }
            }

            log::info!("Good: {}; bad: {}", success_count, failure_count);
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
    #[error("SURT error")]
    Surt(#[from] archivindex_wbm::surt::Error),
    #[error("WXJ lines error")]
    WxjLines(#[from] archivindex_wxj::lines::Error),
    #[error("WXJ hacking error")]
    Wxj(#[from] wxj::Error),
}

#[derive(Debug, Parser)]
#[clap(name = "archivindex-hacks", version, author)]
struct Opts {
    #[clap(flatten)]
    verbose: Verbosity,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    WxjUrls {
        #[clap(long)]
        input: PathBuf,
    },
    WxjEnhance {
        #[clap(long)]
        data: PathBuf,
        #[clap(long)]
        urls: PathBuf,
        #[clap(long)]
        cdx: PathBuf,
        #[clap(long)]
        invalid_digests: PathBuf,
    },
    ValidatedWxjLines {
        #[clap(long)]
        input: PathBuf,
    },
    CheckSurts {
        #[clap(long)]
        input: PathBuf,
    },
    CdxList {
        #[clap(long)]
        base: PathBuf,
    },
}

fn find_cdx_files<P: AsRef<Path>>(root: P) -> Result<Vec<PathBuf>, Error> {
    let mut cdx_paths = std::fs::read_dir(root)?
        .flat_map(|collection_entry| {
            collection_entry
                .and_then(|entry| std::fs::read_dir(entry.path()))
                .map_or_else(|error| vec![Err(error)], |paths| paths.collect())
        })
        .flat_map(|screen_name_entry| {
            screen_name_entry
                .and_then(|entry| std::fs::read_dir(entry.path().join("data")))
                .map_or_else(|error| vec![Err(error)], |paths| paths.collect())
        })
        .map(|entry| {
            entry.and_then(|entry| {
                let modified = entry.metadata()?.modified()?;

                Ok((modified, entry.path()))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    cdx_paths.sort_by_key(|(timestamp, _)| std::cmp::Reverse(*timestamp));

    Ok(cdx_paths.into_iter().map(|(_, path)| path).collect())
}
