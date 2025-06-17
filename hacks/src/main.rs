use archivindex_wbm::{
    cdx::{item::ItemList, mime_type::MimeType},
    surt::Surt,
};
use archivindex_wxj::lines::{
    Snapshot, SnapshotLine,
    tweet::{TweetSnapshot as _, data, flat},
};
use cli_helpers::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

mod wxj;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    opts.verbose.init_logging()?;

    match opts.command {
        Command::WxjUrls { input, flat } => {
            let lines = BufReader::new(zstd::Decoder::new(File::open(input)?)?).lines();

            for line in lines {
                let line = line?;

                let snapshot_line = SnapshotLine::parse(&line)?;

                if snapshot_line.url.is_none() {
                    let url = if flat {
                        serde_json::from_str::<Snapshot<flat::TweetSnapshot>>(&line)?
                            .content
                            .canonical_url(false)
                    } else {
                        serde_json::from_str::<Snapshot<data::TweetSnapshot>>(&line)?
                            .content
                            .canonical_url(false)
                    };

                    match url {
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
            output,
            compression_level,
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

            let mut output = zstd::Encoder::new(File::create(output)?, compression_level)?;

            let lines = BufReader::new(zstd::Decoder::new(File::open(data)?)?).lines();
            for line in lines {
                let line = line?;

                let mut snapshot_line = SnapshotLine::parse(&line)?;
                let new_line = match digest_metadata.get(&snapshot_line.digest) {
                    Some(metadata) => {
                        if let Some((previous, replacement)) = snapshot_line
                            .expected_digest
                            .zip(metadata.expected_digest)
                            .filter(|(previous, replacement)| previous != replacement)
                        {
                            log::warn!("Replacing expected digest: {previous}, {replacement}");
                        }

                        snapshot_line.expected_digest = metadata.expected_digest;

                        if let Some(previous) = snapshot_line
                            .timestamp
                            .filter(|previous| *previous != metadata.timestamp)
                        {
                            log::warn!("Replacing metadata: {previous}, {}", metadata.timestamp);
                        }

                        snapshot_line.timestamp = Some(metadata.timestamp);

                        let new_url = metadata.url();

                        if let Some((previous, replacement)) = snapshot_line
                            .url
                            .zip(new_url.as_ref())
                            .filter(|(previous, replacement)| previous != *replacement)
                        {
                            log::warn!("Replacing URL: {previous}, {replacement}");
                        }

                        snapshot_line.url = new_url;
                        snapshot_line.to_string()
                    }
                    None => line,
                };

                writeln!(output, "{new_line}")?;
            }

            output.do_finish()?;
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
                println!("{path:?}");
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
                                        "Invalid conversion in {path:?}:\nConverted: {converted_surt}\nOriginal:  {}",
                                        item.key
                                    );

                                    failure_count += 1;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        log::error!("At {path:?}: {error:?}");
                    }
                }
            }

            log::info!("Good: {success_count}; bad: {failure_count}");
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
        #[clap(long)]
        flat: bool,
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
        #[clap(long)]
        output: PathBuf,
        #[clap(long, default_value = "14")]
        compression_level: i32,
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
