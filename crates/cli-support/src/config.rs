//! Configuration files in TOML or JSON, recognized by extension.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

/// The reason a configuration file could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file has neither a `.toml` nor a `.json` extension.
    #[error("configuration file {} must have a .toml or .json extension", .0.display())]
    Extension(PathBuf),
    /// The file could not be read.
    #[error("cannot read configuration file {}: {source}", path.display())]
    Read {
        /// The file that could not be read.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// The file is not a document of its format, or does not describe a configuration.
    #[error("cannot parse configuration file {}: {source}", path.display())]
    Parse {
        /// The file that could not be parsed.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: ConfigParseError,
    },
}

/// The reason a configuration document could not be parsed.
#[derive(Debug, thiserror::Error)]
pub enum ConfigParseError {
    /// The TOML document is malformed or does not describe a configuration.
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    /// The JSON document is malformed or does not describe a configuration.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// The document formats a configuration file is read as, recognized by extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigFormat {
    /// A `.toml` file.
    Toml,
    /// A `.json` file.
    Json,
}

impl ConfigFormat {
    /// The format of the file at `path`, from its `.toml` or `.json` extension in any case.
    #[must_use]
    pub fn of(path: &Path) -> Option<Self> {
        let extension = path.extension().and_then(OsStr::to_str)?;

        if extension.eq_ignore_ascii_case("toml") {
            Some(Self::Toml)
        } else if extension.eq_ignore_ascii_case("json") {
            Some(Self::Json)
        } else {
            None
        }
    }

    /// Parse a configuration from a document in this format.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigParseError`] when `text` is not a document of this format or does not
    /// describe a `T`.
    pub fn parse<T: DeserializeOwned>(self, text: &str) -> Result<T, ConfigParseError> {
        match self {
            Self::Toml => toml::from_str(text).map_err(ConfigParseError::from),
            Self::Json => serde_json::from_str(text).map_err(ConfigParseError::from),
        }
    }
}

/// Read the configuration file at `path`, or take the default configuration without one.
///
/// # Errors
///
/// Returns [`ConfigError::Extension`] when the file's extension names no supported format,
/// [`ConfigError::Read`] when it cannot be read, and [`ConfigError::Parse`] when it does not
/// describe a `T`.
pub fn load_config<T: Default + DeserializeOwned>(path: Option<&Path>) -> Result<T, ConfigError> {
    let Some(path) = path else {
        return Ok(T::default());
    };
    let format = ConfigFormat::of(path).ok_or_else(|| ConfigError::Extension(path.to_owned()))?;
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_owned(),
        source,
    })?;

    format.parse(&text).map_err(|source| ConfigError::Parse {
        path: path.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{ConfigError, ConfigFormat, load_config};

    #[derive(Debug, Default, Eq, PartialEq, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        #[serde(default)]
        count: u32,
        #[serde(default)]
        name: String,
    }

    fn write(name: &str, text: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join(name);
        std::fs::write(&path, text).expect("a written file");

        (directory, path)
    }

    #[test]
    fn a_configuration_file_is_recognized_by_its_extension() {
        assert_eq!(
            ConfigFormat::of(Path::new("capture.toml")),
            Some(ConfigFormat::Toml)
        );
        assert_eq!(
            ConfigFormat::of(Path::new("capture.JSON")),
            Some(ConfigFormat::Json)
        );
        assert_eq!(ConfigFormat::of(Path::new("capture.yaml")), None);
        assert_eq!(ConfigFormat::of(Path::new("capture")), None);
    }

    #[test]
    fn no_configuration_file_is_the_default_configuration() {
        let config: Config = load_config(None).expect("the default configuration");

        assert_eq!(config, Config::default());
    }

    #[test]
    fn each_format_is_read_from_its_file() {
        let (_toml_directory, toml) = write("capture.toml", "count = 3\nname = \"toml\"\n");
        let (_json_directory, json) = write("capture.json", r#"{"count": 4, "name": "json"}"#);

        let from_toml: Config = load_config(Some(&toml)).expect("a configuration");
        let from_json: Config = load_config(Some(&json)).expect("a configuration");

        assert_eq!(
            from_toml,
            Config {
                count: 3,
                name: "toml".to_owned()
            }
        );
        assert_eq!(
            from_json,
            Config {
                count: 4,
                name: "json".to_owned()
            }
        );
    }

    #[test]
    fn each_failure_names_the_file() {
        let (_directory, unknown) = write("capture.toml", "unknown = true\n");
        let missing = unknown.with_file_name("missing.json");
        let unsupported = unknown.with_file_name("capture.yaml");

        let parse = load_config::<Config>(Some(&unknown)).expect_err("an unknown key");
        let read = load_config::<Config>(Some(&missing)).expect_err("a missing file");
        let extension = load_config::<Config>(Some(&unsupported)).expect_err("an extension");

        assert!(matches!(parse, ConfigError::Parse { ref path, .. } if *path == unknown));
        assert!(matches!(read, ConfigError::Read { ref path, .. } if *path == missing));
        assert!(matches!(extension, ConfigError::Extension(ref path) if *path == unsupported));
        assert!(parse.to_string().starts_with(&format!(
            "cannot parse configuration file {}",
            unknown.display()
        )));
    }
}
