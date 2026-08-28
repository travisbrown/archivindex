//! String fields that `#[serde(borrow)]` cannot reach into.
//!
//! A bare `Cow<'a, str>` field needs nothing from here: `#[serde(borrow)]` on one already takes
//! `serde`'s borrowing path. The attribute stops at that first layer, though, so a string behind an
//! `Option` or a `Vec` needs one of the modules below as its `#[serde(with)]`. Each is that
//! container deserialized through [`crate::BorrowableStr`] and unwrapped again.
//!
//! Both modules serialize exactly as the derived implementation would. Their `serialize` halves
//! exist so that a single `with` can stand in for a `serialize_with`/`deserialize_with` pair.

use std::borrow::Cow;
use std::marker::PhantomData;

use serde::de::{Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};

use crate::BorrowableStr;

pub mod option;
pub mod seq;

// `'de: 'a` is what lets the value borrow from the input: a `BorrowableStr<'a>` may hold a slice
// of data the deserializer lends for at least as long. The bound is written this way rather than
// as `BorrowableStr<'de>` so that `#[serde(borrow)]` can reach the type through a struct's own
// lifetime parameter.
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

            /// Called when the string is transient, as it is when a JSON escape had to be decoded
            /// into a fresh buffer.
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

/// Writes the string itself, so the wrapper leaves no trace in the output.
impl Serialize for BorrowableStr<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}
