use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{
    de::{Deserialize, Deserializer, Unexpected, Visitor},
    ser::{Serialize, Serializer},
};
use std::fmt::Display;
use std::str::FromStr;

const TIMESTAMP_FMT: &str = "%Y%m%d%H%M%S";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid timestamp length")]
    InvalidLength(String),
    #[error("Invalid timestamp input")]
    InvalidDateTime(#[from] chrono::format::ParseError),
    #[error("Invalid timestamp")]
    InvalidTimestamp(i64),
}

/// Represents a Wayback Machine URL timestamp.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(DateTime<Utc>);

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.format(TIMESTAMP_FMT))
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl TryFrom<i64> for Timestamp {
    type Error = Error;
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Self(
            DateTime::from_timestamp(value, 0).ok_or(Error::InvalidTimestamp(value))?,
        ))
    }
}

impl From<Timestamp> for i64 {
    fn from(value: Timestamp) -> Self {
        DateTime::<Utc>::from(value).timestamp()
    }
}

impl FromStr for Timestamp {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() == 14 {
            Ok(Timestamp(
                NaiveDateTime::parse_from_str(s, TIMESTAMP_FMT)?.and_utc(),
            ))
        } else {
            Err(Self::Err::InvalidLength(s.to_string()))
        }
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TimestampVisitor;

        impl Visitor<'_> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Timestamp")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse()
                    .map_err(|_| serde::de::Error::invalid_value(Unexpected::Str(v), &self))
            }
        }

        deserializer.deserialize_str(TimestampVisitor)
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{SubsecRound, Utc};

    #[test]
    fn round_trip() {
        let timestamp = super::Timestamp(Utc::now().trunc_subsecs(0));

        let timestamp_str = timestamp.to_string();
        let timestamp_parsed = timestamp_str.parse().unwrap();

        assert_eq!(timestamp, timestamp_parsed);
    }
}
