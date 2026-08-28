//! Capture timestamps shared by CDX representations.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Datelike as _, NaiveDateTime, SubsecRound as _, Utc};

const SECONDS_FORMAT: &str = "%Y%m%d%H%M%S";
const SECONDS_LENGTH: usize = 14;
const MILLISECONDS_LENGTH: usize = 17;

/// A CDX timestamp is malformed or names an invalid UTC instant.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid CDX timestamp: {0}")]
pub struct Error(pub String);

/// A 14- or 17-digit CDX timestamp (`YYYYmmddHHMMSS[sss]`, always UTC).
///
/// The shorter form has whole-second precision; the longer form appends milliseconds. Parsing and
/// display preserve the precision. Equality, hashing, and ordering follow the serialized form, so
/// whole seconds sort before the millisecond form of the same instant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp {
    instant: DateTime<Utc>,
    milliseconds: bool,
}

impl Timestamp {
    /// Create a timestamp, truncating the instant to whole seconds.
    ///
    /// Rejects years outside `0000..=9999` and leap seconds, which cannot round-trip through the
    /// supported CDX representations.
    pub fn new(instant: DateTime<Utc>) -> Result<Self, Error> {
        Self::with_precision(instant, false)
    }

    /// Create a 17-digit timestamp, truncating the instant to milliseconds.
    ///
    /// Rejects years outside `0000..=9999` and leap seconds, as [`Self::new`] does.
    pub fn new_with_milliseconds(instant: DateTime<Utc>) -> Result<Self, Error> {
        Self::with_precision(instant, true)
    }

    fn with_precision(instant: DateTime<Utc>, milliseconds: bool) -> Result<Self, Error> {
        if !(0..=9999).contains(&instant.year())
            || instant.timestamp_subsec_nanos() >= 1_000_000_000
        {
            return Err(Error(instant.to_string()));
        }
        Ok(Self {
            instant: instant.trunc_subsecs(if milliseconds { 3 } else { 0 }),
            milliseconds,
        })
    }

    /// The represented UTC instant.
    #[must_use]
    pub const fn datetime(self) -> DateTime<Utc> {
        self.instant
    }

    /// Whether the timestamp includes milliseconds.
    #[must_use]
    pub const fn has_milliseconds(self) -> bool {
        self.milliseconds
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.instant.format(SECONDS_FORMAT))?;
        if self.milliseconds {
            write!(formatter, "{:03}", self.instant.timestamp_subsec_millis())?;
        }
        Ok(())
    }
}

impl FromStr for Timestamp {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !matches!(value.len(), SECONDS_LENGTH | MILLISECONDS_LENGTH)
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(Error(value.to_owned()));
        }

        let seconds = NaiveDateTime::parse_from_str(&value[..SECONDS_LENGTH], SECONDS_FORMAT)
            .map_err(|_| Error(value.to_owned()))?
            .and_utc();
        // Chrono represents a leap second as second 59 plus at least one billion nanoseconds.
        if seconds.timestamp_subsec_nanos() >= 1_000_000_000 {
            return Err(Error(value.to_owned()));
        }

        if value.len() == MILLISECONDS_LENGTH {
            let milliseconds = value[SECONDS_LENGTH..]
                .parse::<i64>()
                .map_err(|_| Error(value.to_owned()))?;
            Self::new_with_milliseconds(seconds + chrono::TimeDelta::milliseconds(milliseconds))
        } else {
            Self::new(seconds)
        }
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        archivindex_serde::from_str(deserializer, "a CDX timestamp")
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for Timestamp {
    type Static = Self;

    fn to_static(&self) -> Self::Static {
        *self
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Timestamp {
    type Static = Self;

    fn into_static(self) -> Self::Static {
        self
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::prop;

    #[test]
    fn preserves_precision_and_text_order() -> Result<(), Error> {
        let forms = [
            "20201007212235999",
            "20201007212236",
            "20201007212236000",
            "20201007212236001",
            "20201007212237",
        ];
        let values = forms
            .iter()
            .map(|value| value.parse::<Timestamp>())
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(values[1].to_string(), forms[1]);
        assert_eq!(values[2].to_string(), forms[2]);
        assert!(!values[1].has_milliseconds());
        assert!(values[2].has_milliseconds());
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        Ok(())
    }

    #[test]
    fn rejects_invalid_forms() {
        for value in [
            "2020100721223",
            "2020100721223a",
            "2020100721223600",
            "20201007212236a00",
            "20201307212236",
            "20201007216000",
            "20201007212260",
        ] {
            assert!(value.parse::<Timestamp>().is_err(), "accepted {value}");
        }
    }

    #[test]
    fn serde_uses_the_cdx_text_form() -> Result<(), Box<dyn std::error::Error>> {
        let timestamp = "20201007212236123".parse::<Timestamp>()?;
        let json = serde_json::to_string(&timestamp)?;
        assert_eq!(json, "\"20201007212236123\"");
        assert_eq!(serde_json::from_str::<Timestamp>(&json)?, timestamp);
        Ok(())
    }

    /// Parsing is delegated to `archivindex_serde::from_str`, whose error names the rejected value
    /// and what was expected in its place, rather than repeating the parse error.
    #[test]
    fn serde_rejects_text_that_is_not_a_timestamp() {
        let error = serde_json::from_str::<Timestamp>("\"20201307212236\"").unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid value: string \"20201307212236\", expected a CDX timestamp at line 1 column 16"
        );
    }

    #[proptest::property_test]
    fn text_round_trips(#[strategy = prop::timestamp()] timestamp: Timestamp) {
        prop_assert_eq!(
            timestamp.to_string().parse::<Timestamp>().ok(),
            Some(timestamp)
        );
    }

    /// CDX indexes are sorted as text, so the text order has to agree with the value order.
    #[proptest::property_test]
    fn text_order_matches_value_order(
        #[strategy = prop::timestamp()] first: Timestamp,
        #[strategy = prop::timestamp()] second: Timestamp,
    ) {
        prop_assert_eq!(
            first.cmp(&second),
            first.to_string().cmp(&second.to_string())
        );
    }

    #[test]
    fn constructors_reject_unrepresentable_instants() {
        use chrono::TimeZone as _;
        for instant in [
            Utc.with_ymd_and_hms(-1, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(10000, 1, 1, 0, 0, 0).unwrap(),
            DateTime::from_timestamp(1_483_228_799, 1_000_000_000).unwrap(),
        ] {
            assert!(Timestamp::new(instant).is_err());
            assert!(Timestamp::new_with_milliseconds(instant).is_err());
        }
    }

    #[test]
    fn constructors_round_trip_at_the_date_boundaries() {
        use chrono::TimeZone as _;
        for instant in [
            Utc.with_ymd_and_hms(0, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap(),
        ] {
            for timestamp in [
                Timestamp::new(instant).unwrap(),
                Timestamp::new_with_milliseconds(instant).unwrap(),
            ] {
                assert_eq!(
                    timestamp.to_string().parse::<Timestamp>().unwrap(),
                    timestamp
                );
            }
        }
    }
}
