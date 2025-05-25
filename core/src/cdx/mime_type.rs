use serde::de::{Deserialize, Deserializer, Unexpected, Visitor};
use std::fmt::Display;
use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid MIME type: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MimeType {
    TextHtml,
    ApplicationJson,
    Other(String),
}

impl Display for MimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TextHtml => f.write_str("text/html"),
            Self::ApplicationJson => f.write_str("application/json"),
            Self::Other(value) => f.write_str(value),
        }
    }
}

impl FromStr for MimeType {
    type Err = Error;

    // TODO: Add validation here.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text/html" => Ok(Self::TextHtml),
            "application/json" => Ok(Self::ApplicationJson),
            other => Ok(Self::Other(other.to_string())),
        }
    }
}

impl<'de> Deserialize<'de> for MimeType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MimeTypeVisitor;

        impl Visitor<'_> for MimeTypeVisitor {
            type Value = MimeType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("enum MimeType")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse()
                    .map_err(|_| serde::de::Error::invalid_value(Unexpected::Str(v), &self))
            }
        }

        deserializer.deserialize_str(MimeTypeVisitor)
    }
}
