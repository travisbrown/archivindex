//! Models for the CDX representations commonly encountered in web archiving.
//!
//! - [`classic`]: header-described, delimiter-separated CDX records;
//! - [`cdxj`]: records with a search key and timestamp followed by a JSON object;
//! - [`json`]: CDX server documents with a header row and records represented as JSON arrays.
//!
//! The models follow [IIPC CDX], the [CDXJ 0.1.0 specification], and the header-driven JSON output
//! of the [Wayback CDX Server].
//!
//! [IIPC CDX]: https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
//! [CDXJ 0.1.0 specification]: https://specs.webrecorder.net/cdxj/0.1.0/
//! [Wayback CDX Server]: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server

pub mod cdxj;
pub mod classic;
pub mod json;

use std::fmt::{self, Display};
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::{Deserializer, Unexpected, Visitor};
use serde::ser::Serializer;

pub(crate) fn optional_integer<'de, D: Deserializer<'de>, T: TryFrom<u64> + FromStr>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    struct IntegerVisitor<T>(PhantomData<T>);

    impl<'de, T: TryFrom<u64> + FromStr> Visitor<'de> for IntegerVisitor<T> {
        type Value = Option<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("unsigned integer or unsigned integer string")
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
            T::try_from(value).map(Some).map_err(|_| {
                E::invalid_value(Unexpected::Unsigned(value), &"unsigned integer in range")
            })
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
            value
                .parse()
                .map(Some)
                .map_err(|_| E::invalid_value(Unexpected::Str(value), &"unsigned integer string"))
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_any(Self(PhantomData))
        }
    }

    deserializer.deserialize_option(IntegerVisitor(PhantomData))
}

#[expect(
    clippy::ref_option,
    reason = "serde's `serialize_with` passes the field by reference"
)]
pub(crate) fn optional_integer_str<S: Serializer, T: Display>(
    value: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.collect_str(value),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn integer_str<S: Serializer, T: Display>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}
