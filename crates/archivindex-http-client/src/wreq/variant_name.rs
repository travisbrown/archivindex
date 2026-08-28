//! Recovery of the name a unit variant is serialized under.

use serde::de::value::Error;
use serde::ser::{Error as _, Impossible, Serialize, Serializer};

/// Serializes a unit variant as its name, and nothing else.
pub struct VariantName;

/// Define the `Serializer` methods for input that is not a unit variant.
macro_rules! refuse {
    ($($method:ident$(<$value:ident>)?($($argument:ty),*) -> $output:ident;)*) => {$(
        fn $method$(<$value: ?Sized + Serialize>)?(
            self,
            $(_: $argument),*
        ) -> Result<Self::$output, Error> {
            Err(Error::custom("expected a unit variant"))
        }
    )*};
}

impl Serializer for VariantName {
    type Ok = &'static str;
    type Error = Error;
    type SerializeSeq = Impossible<Self::Ok, Error>;
    type SerializeTuple = Impossible<Self::Ok, Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Error>;
    type SerializeMap = Impossible<Self::Ok, Error>;
    type SerializeStruct = Impossible<Self::Ok, Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Error>;

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Error> {
        Ok(variant)
    }

    refuse! {
        serialize_bool(bool) -> Ok;
        serialize_i8(i8) -> Ok;
        serialize_i16(i16) -> Ok;
        serialize_i32(i32) -> Ok;
        serialize_i64(i64) -> Ok;
        serialize_u8(u8) -> Ok;
        serialize_u16(u16) -> Ok;
        serialize_u32(u32) -> Ok;
        serialize_u64(u64) -> Ok;
        serialize_f32(f32) -> Ok;
        serialize_f64(f64) -> Ok;
        serialize_char(char) -> Ok;
        serialize_str(&str) -> Ok;
        serialize_bytes(&[u8]) -> Ok;
        serialize_none() -> Ok;
        serialize_some<T>(&T) -> Ok;
        serialize_unit() -> Ok;
        serialize_unit_struct(&'static str) -> Ok;
        serialize_newtype_struct<T>(&'static str, &T) -> Ok;
        serialize_newtype_variant<T>(&'static str, u32, &'static str, &T) -> Ok;
        serialize_seq(Option<usize>) -> SerializeSeq;
        serialize_tuple(usize) -> SerializeTuple;
        serialize_tuple_struct(&'static str, usize) -> SerializeTupleStruct;
        serialize_tuple_variant(&'static str, u32, &'static str, usize) -> SerializeTupleVariant;
        serialize_map(Option<usize>) -> SerializeMap;
        serialize_struct(&'static str, usize) -> SerializeStruct;
        serialize_struct_variant(&'static str, u32, &'static str, usize) -> SerializeStructVariant;
    }
}

#[cfg(test)]
mod tests {
    use serde::ser::Serialize;

    use super::VariantName;

    /// Only a unit variant has a name to report. A string and a variant holding a value stand for
    /// the scalar and generic methods that refuse their input.
    #[test]
    fn input_other_than_a_unit_variant_is_refused() {
        assert_eq!(None::<u8>.serialize(VariantName).ok(), None);
        assert_eq!("chrome_136".serialize(VariantName).ok(), None);
        assert_eq!(Some("chrome_136").serialize(VariantName).ok(), None);
        assert_eq!(Ok::<u8, u8>(1).serialize(VariantName).ok(), None);
    }
}
