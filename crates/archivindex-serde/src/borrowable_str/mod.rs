//! Borrow strings inside an `Option` or a `Vec`.
//!
//! For a `Cow<'a, str>` field, `#[serde(borrow)]` is enough to borrow from the input. For an
//! `Option<Cow<'a, str>>` or a `Vec<Cow<'a, str>>`, use it with `#[serde(with)]` and the
//! corresponding module below. These modules use [`crate::BorrowableStr`] to borrow each string
//! when possible.
//!
//! Both modules serialize exactly as the derived implementation would, and the `serialize`
//! definitions are provided solely to support use with `with`.

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

            /// Called when the format can lend a slice that lives as long as the input.
            fn visit_borrowed_str<E: serde::de::Error>(
                self,
                value: &'de str,
            ) -> Result<Self::Value, E> {
                Ok(BorrowableStr(Cow::Borrowed(value)))
            }

            /// Called when the string cannot be borrowed, for example after decoding a JSON escape.
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
