//! Extension properties preserved by JSON-backed models.

/// Additional JSON properties preserved in insertion order for round-tripping.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct ExtraProperties(serde_json::Map<String, serde_json::Value>);

/// An extension property duplicates a modeled property.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("extension property `{property}` duplicates a modeled property of {model}")]
pub struct Error {
    /// The model containing the extension map.
    pub model: &'static str,
    /// The duplicated property name.
    pub property: String,
}

impl ExtraProperties {
    /// Check that none of the extension keys duplicate a modeled property.
    pub fn validate(&self, model: &'static str, reserved: &[&str]) -> Result<(), Error> {
        reserved
            .iter()
            .find(|property| self.contains_key(**property))
            .map_or(Ok(()), |property| {
                Err(Error {
                    model,
                    property: (*property).to_owned(),
                })
            })
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for ExtraProperties {
    fn from(map: serde_json::Map<String, serde_json::Value>) -> Self {
        Self(map)
    }
}

impl From<ExtraProperties> for serde_json::Map<String, serde_json::Value> {
    fn from(properties: ExtraProperties) -> Self {
        properties.0
    }
}

impl std::ops::Deref for ExtraProperties {
    type Target = serde_json::Map<String, serde_json::Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ExtraProperties {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for ExtraProperties {
    type Static = Self;

    fn to_static(&self) -> Self::Static {
        self.clone()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for ExtraProperties {
    type Static = Self;

    fn into_static(self) -> Self::Static {
        self
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::strategies;

    #[test]
    fn validation_reports_the_first_reserved_property_present() {
        let mut properties = ExtraProperties::default();
        properties.insert("second".to_owned(), true.into());
        properties.insert("first".to_owned(), false.into());

        let error = properties
            .validate("test model", &["absent", "first", "second"])
            .unwrap_err();

        assert_eq!(error.model, "test model");
        assert_eq!(error.property, "first");
        assert_eq!(
            error.to_string(),
            "extension property `first` duplicates a modeled property of test model"
        );
        assert_eq!(properties.validate("test model", &["absent"]), Ok(()));
    }

    #[test]
    fn map_conversion_preserves_values_and_insertion_order() {
        let mut map = serde_json::Map::new();
        map.insert("first".to_owned(), 1.into());
        map.insert(
            "second".to_owned(),
            serde_json::Value::String("two".to_owned()),
        );

        let properties = ExtraProperties::from(map.clone());
        assert_eq!(properties.keys().collect::<Vec<_>>(), ["first", "second"]);
        assert_eq!(serde_json::Map::from(properties), map);
    }

    #[cfg(feature = "bounded-static")]
    #[test]
    fn bounded_static_conversions_preserve_properties() {
        use bounded_static::{IntoBoundedStatic as _, ToBoundedStatic as _};

        let mut properties = ExtraProperties::default();
        properties.insert("name".to_owned(), "value".into());

        assert_eq!(properties.to_static(), properties);
        assert_eq!(properties.clone().into_static(), properties);
    }

    #[test_strategy::proptest]
    fn arbitrary_maps_round_trip(
        #[strategy(strategies::extra_properties())] properties: ExtraProperties,
    ) {
        let expected = properties.clone();
        let map = serde_json::Map::from(properties);

        prop_assert_eq!(ExtraProperties::from(map), expected);
    }

    #[test_strategy::proptest]
    fn serde_round_trips(#[strategy(strategies::extra_properties())] properties: ExtraProperties) {
        let text = serde_json::to_string(&properties).unwrap();
        let decoded = serde_json::from_str::<ExtraProperties>(&text).unwrap();

        prop_assert_eq!(decoded, properties);
    }

    #[test_strategy::proptest]
    fn inserted_reserved_names_are_rejected(#[strategy(strategies::bare_text())] name: String) {
        let mut properties = ExtraProperties::default();
        properties.insert(name.clone(), serde_json::Value::Null);

        prop_assert_eq!(
            properties.validate("model", &["absent", &name]),
            Err(Error {
                model: "model",
                property: name,
            })
        );
    }
}
