//! Capture timestamps shared by CDX representations.

use std::str::FromStr;

use bounded_static::{IntoBoundedStatic, ToBoundedStatic};
use jiff::civil::{DateTime, DateTimeRound};
use jiff::{RoundMode, SignedDuration, Unit};

const SECONDS_FORMAT: &str = "%Y%m%d%H%M%S";
const SECONDS_LENGTH: usize = 14;
const MILLISECONDS_LENGTH: usize = 17;

/// A CDX timestamp is malformed or contains an invalid UTC date or time.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid CDX timestamp: {0}")]
pub struct Error(pub String);

/// A 14- or 17-digit CDX timestamp (`YYYYmmddHHMMSS[sss]`, always UTC).
///
/// The shorter form has whole-second precision; the longer form appends milliseconds. Parsing and
/// display preserve the precision. Equality and hashing distinguish the two precisions. Ordering
/// matches the text order, so a timestamp in whole seconds sorts before the millisecond form of
/// the same instant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp {
    datetime: DateTime,
    milliseconds: bool,
}

impl Timestamp {
    /// Create a timestamp from a UTC datetime, truncating it to whole seconds.
    ///
    /// Rejects years before `0000`, which cannot round-trip through the supported CDX
    /// representations. Jiff datetimes cannot represent years after `9999` or leap seconds.
    ///
    /// Convert an instant to UTC before constructing a CDX timestamp:
    ///
    /// ```
    /// use archivindex_cdx::timestamp::Timestamp;
    /// use jiff::tz::Offset;
    ///
    /// let instant: jiff::Timestamp = "2020-10-07T21:22:36Z".parse()?;
    /// let timestamp = Timestamp::new(Offset::UTC.to_datetime(instant))?;
    /// assert_eq!(timestamp.to_string(), "20201007212236");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(datetime: DateTime) -> Result<Self, Error> {
        Self::with_precision(datetime, false)
    }

    /// Create a 17-digit timestamp from a UTC datetime, truncating it to milliseconds.
    ///
    /// Rejects years before `0000`, as [`Self::new`] does.
    pub fn new_with_milliseconds(datetime: DateTime) -> Result<Self, Error> {
        Self::with_precision(datetime, true)
    }

    fn with_precision(datetime: DateTime, milliseconds: bool) -> Result<Self, Error> {
        if datetime.year() < 0 {
            Err(Error(datetime.to_string()))
        } else {
            let unit = if milliseconds {
                Unit::Millisecond
            } else {
                Unit::Second
            };
            Ok(Self {
                datetime: datetime
                    .round(DateTimeRound::new().smallest(unit).mode(RoundMode::Floor))
                    .map_err(|_| Error(datetime.to_string()))?,
                milliseconds,
            })
        }
    }

    /// The represented UTC datetime.
    #[must_use]
    pub const fn datetime(self) -> DateTime {
        self.datetime
    }

    /// Whether the timestamp includes milliseconds.
    #[must_use]
    pub const fn has_milliseconds(self) -> bool {
        self.milliseconds
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.datetime.strftime(SECONDS_FORMAT))?;
        if self.milliseconds {
            write!(formatter, "{:03}", self.datetime.millisecond())?;
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

        // Jiff constrains leap seconds to second 59, but CDX must reject them.
        if &value[12..SECONDS_LENGTH] == "60" {
            return Err(Error(value.to_owned()));
        }
        let seconds = DateTime::strptime(SECONDS_FORMAT, &value[..SECONDS_LENGTH])
            .map_err(|_| Error(value.to_owned()))?;

        if value.len() == MILLISECONDS_LENGTH {
            let milliseconds = value[SECONDS_LENGTH..]
                .parse::<i64>()
                .map_err(|_| Error(value.to_owned()))?;
            Self::new_with_milliseconds(seconds + SignedDuration::from_millis(milliseconds))
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

impl ToBoundedStatic for Timestamp {
    type Static = Self;

    fn to_static(&self) -> Self::Static {
        *self
    }
}

impl IntoBoundedStatic for Timestamp {
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
            "20161231235960",
            "20161231235960123",
            "20230229000000",
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

    /// A Serde parsing error includes the rejected value and describes the expected input.
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
        for instant in ["-000001-01-01T00:00:00".parse().unwrap(), DateTime::MIN] {
            assert!(Timestamp::new(instant).is_err());
            assert!(Timestamp::new_with_milliseconds(instant).is_err());
        }
    }

    #[test]
    fn constructors_round_trip_at_the_date_boundaries() {
        for (instant, seconds, milliseconds) in [
            (
                "0000-01-01T00:00:00".parse().unwrap(),
                "00000101000000",
                "00000101000000000",
            ),
            (DateTime::MAX, "99991231235959", "99991231235959999"),
        ] {
            for (timestamp, text) in [
                (Timestamp::new(instant).unwrap(), seconds),
                (
                    Timestamp::new_with_milliseconds(instant).unwrap(),
                    milliseconds,
                ),
            ] {
                assert_eq!(timestamp.to_string(), text);
                assert_eq!(
                    timestamp.to_string().parse::<Timestamp>().unwrap(),
                    timestamp
                );
            }
        }
    }

    /// Truncation drops calendar fractions, including for instants before the Unix epoch.
    #[test]
    fn constructors_truncate_fractions_without_changing_the_second() {
        for (value, seconds, milliseconds) in [
            (
                "1969-12-31T23:59:59.123456789",
                "19691231235959",
                "19691231235959123",
            ),
            (
                "1969-12-31T23:59:59.999999999",
                "19691231235959",
                "19691231235959999",
            ),
            (
                "2020-10-07T21:22:36.123456789",
                "20201007212236",
                "20201007212236123",
            ),
        ] {
            let instant = value.parse().unwrap();
            for (timestamp, text) in [
                (Timestamp::new(instant).unwrap(), seconds),
                (
                    Timestamp::new_with_milliseconds(instant).unwrap(),
                    milliseconds,
                ),
            ] {
                assert_eq!(timestamp.to_string(), text);
                assert_eq!(timestamp, text.parse().unwrap());
                assert!(timestamp.datetime() <= instant);
            }
        }
    }
}
