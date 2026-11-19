//! Borrow strings inside an `Option` or a `Vec`.
//!
//! Use `#[serde(borrow, with = "...")]` with [`option`] for optional strings or [`seq`] for
//! sequences. Serialization is unchanged.

use std::borrow::Cow;
use std::marker::PhantomData;

use serde::de::{Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};

use crate::BorrowableStr;

pub mod option;
pub mod seq;

// The input must outlive the borrowed string. Separate `'de` and `'a` lifetimes let a struct
// use its own lifetime parameter for a `BorrowableStr` field with `#[serde(borrow)]`.
impl<'de: 'a, 'a> Deserialize<'de> for BorrowableStr<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BorrowableStrVisitor<'a>(PhantomData<&'a str>);

        impl<'de: 'a, 'a> Visitor<'de> for BorrowableStrVisitor<'a> {
            type Value = BorrowableStr<'a>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_borrowed_str<E: serde::de::Error>(
                self,
                value: &'de str,
            ) -> Result<Self::Value, E> {
                Ok(BorrowableStr(Cow::Borrowed(value)))
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(BorrowableStr(Cow::Owned(value.to_owned())))
            }

            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(BorrowableStr(Cow::Owned(value)))
            }
        }

        deserializer.deserialize_str(BorrowableStrVisitor(PhantomData))
    }
}

/// Serialize the value as a plain string.
impl Serialize for BorrowableStr<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}
