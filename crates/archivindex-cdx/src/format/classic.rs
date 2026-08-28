//! Classic CDX entries, with a header defining the delimiter and fields.

use std::borrow::Cow;
use std::str::FromStr;

use crate::field::{self, Field};

/// A classic CDX header or entry is malformed.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// The header does not begin with `CDX` in either accepted form.
    #[error("invalid CDX header: {0}")]
    InvalidHeader(String),
    /// The legend has no fields or contains an empty field marker.
    #[error("invalid CDX legend: {0}")]
    InvalidLegend(String),
    /// An entry has a different number of values from its legend.
    #[error("CDX entry has {actual} fields, expected {expected}")]
    FieldCount {
        /// Number of fields named by the header.
        expected: usize,
        /// Number of values in the entry.
        actual: usize,
    },

    /// An entry cannot be converted to the common entry model.
    #[error(transparent)]
    Field(#[from] field::Error),
}

/// A classic CDX legend and its delimiter.
///
/// The legend lists the fields in each entry. Headers may start with the delimiter (for example,
/// ` CDX N b a`) or directly with `CDX`. Parsing and formatting preserve that choice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header<'a> {
    delimiter: char,
    leading_delimiter: bool,
    markers: Vec<Cow<'a, str>>,
}

impl<'a> Header<'a> {
    /// Construct a header from a delimiter and legend markers.
    pub fn new(
        delimiter: char,
        leading_delimiter: bool,
        markers: Vec<Cow<'a, str>>,
    ) -> Result<Self, Error> {
        if matches!(delimiter, '\r' | '\n')
            || markers.is_empty()
            || markers
                .iter()
                .any(|marker| marker.is_empty() || marker.contains(delimiter))
        {
            return Err(Error::InvalidLegend(join(delimiter, &markers)));
        }
        Ok(Self {
            delimiter,
            leading_delimiter,
            markers,
        })
    }

    /// Parse a classic CDX header without its trailing newline.
    pub fn parse(line: &'a str) -> Result<Self, Error> {
        if line.contains(['\r', '\n']) {
            return Err(Error::InvalidHeader(line.to_owned()));
        }

        let (leading_delimiter, delimiter, rest) = if let Some(rest) = line.strip_prefix("CDX") {
            let delimiter = rest
                .chars()
                .next()
                .ok_or_else(|| Error::InvalidLegend(line.to_owned()))?;
            (false, delimiter, &rest[delimiter.len_utf8()..])
        } else {
            let delimiter = line
                .chars()
                .next()
                .ok_or_else(|| Error::InvalidHeader(line.to_owned()))?;
            let rest = &line[delimiter.len_utf8()..];
            let rest = rest
                .strip_prefix("CDX")
                .ok_or_else(|| Error::InvalidHeader(line.to_owned()))?;
            let rest = rest
                .strip_prefix(delimiter)
                .ok_or_else(|| Error::InvalidHeader(line.to_owned()))?;
            (true, delimiter, rest)
        };

        // `new` can reject only the legend because the delimiter came from this newline-free input.
        // Report the complete header rather than the legend fragment.
        let markers = rest.split(delimiter).map(Cow::Borrowed).collect::<Vec<_>>();
        Self::new(delimiter, leading_delimiter, markers)
            .map_err(|_| Error::InvalidLegend(line.to_owned()))
    }

    /// Construct the common 11-field CDX legend (`N b a m s k r M S V g`).
    #[must_use]
    pub fn standard_11() -> Header<'static> {
        Self::standard(&["N", "b", "a", "m", "s", "k", "r", "M", "S", "V", "g"])
    }

    /// Construct the older 9-field CDX legend (`N b a m s k r V g`).
    #[must_use]
    pub fn standard_9() -> Header<'static> {
        Self::standard(&["N", "b", "a", "m", "s", "k", "r", "V", "g"])
    }

    /// The field delimiter.
    #[must_use]
    pub const fn delimiter(&self) -> char {
        self.delimiter
    }

    /// Whether the serialized header starts with the delimiter.
    #[must_use]
    pub const fn has_leading_delimiter(&self) -> bool {
        self.leading_delimiter
    }

    /// The legend markers in entry order.
    #[must_use]
    pub fn markers(&self) -> &[Cow<'a, str>] {
        &self.markers
    }

    /// Return an owned header that can outlive the input.
    #[must_use]
    pub fn into_owned(self) -> Header<'static> {
        Header {
            delimiter: self.delimiter,
            leading_delimiter: self.leading_delimiter,
            markers: self
                .markers
                .into_iter()
                .map(|marker| Cow::Owned(marker.into_owned()))
                .collect(),
        }
    }

    /// Look up which field a legend marker represents.
    #[must_use]
    pub fn field(&self, index: usize) -> Option<Field<'_>> {
        self.markers.get(index).map(|marker| Field::legend(marker))
    }

    /// Construct an entry from values in legend order.
    ///
    /// The number of values must match the number of fields in the legend.
    pub fn entry<'b>(&self, values: Vec<Cow<'b, str>>) -> Result<Entry<'b>, Error> {
        self.check_count(values.len())?;
        Ok(Entry { values })
    }

    /// Parse an entry using this header.
    pub fn parse_entry<'b>(&self, line: &'b str) -> Result<Entry<'b>, Error> {
        self.entry(line.split(self.delimiter).map(Cow::Borrowed).collect())
    }

    /// Convert an entry to the shared [`crate::Entry`] model.
    pub fn decode<'b>(&self, entry: &Entry<'b>) -> Result<crate::Entry<'b>, Error> {
        let fields = self
            .markers
            .iter()
            .zip(&entry.values)
            .map(|(marker, value)| (Field::legend(marker), value.clone()));
        Ok(crate::entry::from_fields(fields)?)
    }

    /// Format an entry with this header's delimiter.
    #[must_use]
    pub fn render(&self, entry: &Entry<'_>) -> String {
        join(self.delimiter, &entry.values)
    }

    fn standard(markers: &[&'static str]) -> Header<'static> {
        Header {
            delimiter: ' ',
            leading_delimiter: true,
            markers: markers
                .iter()
                .map(|marker| Cow::Borrowed(*marker))
                .collect(),
        }
    }

    const fn check_count(&self, actual: usize) -> Result<(), Error> {
        if actual == self.markers.len() {
            Ok(())
        } else {
            Err(Error::FieldCount {
                expected: self.markers.len(),
                actual,
            })
        }
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for Header<'_> {
    type Static = Header<'static>;

    fn to_static(&self) -> Self::Static {
        self.clone().into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Header<'_> {
    type Static = Header<'static>;

    fn into_static(self) -> Self::Static {
        self.into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::ToBoundedStatic for Entry<'_> {
    type Static = Entry<'static>;

    fn to_static(&self) -> Self::Static {
        self.clone().into_owned()
    }
}

#[cfg(feature = "bounded-static")]
#[cfg_attr(docsrs, doc(cfg(feature = "bounded-static")))]
impl bounded_static::IntoBoundedStatic for Entry<'_> {
    type Static = Entry<'static>;

    fn into_static(self) -> Self::Static {
        self.into_owned()
    }
}

impl std::fmt::Display for Header<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.leading_delimiter {
            write!(formatter, "{}", self.delimiter)?;
        }
        write!(formatter, "CDX{}", self.delimiter)?;
        formatter.write_str(&join(self.delimiter, &self.markers))
    }
}

impl FromStr for Header<'static> {
    type Err = Error;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let header: Header<'_> = Header::parse(line)?;
        Ok(header.into_owned())
    }
}

/// Join values with a delimiter character.
fn join(delimiter: char, values: &[Cow<'_, str>]) -> String {
    let mut joined = String::with_capacity(values.iter().map(|value| value.len() + 1).sum());
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            joined.push(delimiter);
        }
        joined.push_str(value);
    }
    joined
}

/// The values of a classic CDX entry, in the order defined by its header.
///
/// [`Header::entry`] and [`Header::parse_entry`] check that the number of values matches the
/// header's legend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    values: Vec<Cow<'a, str>>,
}

impl<'a> Entry<'a> {
    /// The values in legend order.
    #[must_use]
    pub fn values(&self) -> &[Cow<'a, str>] {
        &self.values
    }

    /// Return an owned entry that can outlive the input.
    #[must_use]
    pub fn into_owned(self) -> Entry<'static> {
        Entry {
            values: self
                .values
                .into_iter()
                .map(|value| Cow::Owned(value.into_owned()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::prop;

    #[test]
    fn parses_standard_entry() -> Result<(), Box<dyn std::error::Error>> {
        let header = Header::parse(" CDX N b a m s k r M S V g")?;
        let entry = header.parse_entry(concat!(
            "com,example)/ 20201007212236 https://example.com/ text/html 200 ",
            "sha1:TEST - - 1300 784 data.warc.gz"
        ))?;
        let entry = header.decode(&entry)?;

        assert_eq!(entry.key, "com,example)/");
        assert_eq!(entry.status, Some(200));
        assert_eq!(entry.offset, Some(784));
        assert_eq!(entry.length, Some(1300));
        assert_eq!(entry.redirect, None);
        Ok(())
    }

    #[test]
    fn retains_header_form_and_unknown_fields() -> Result<(), Box<dyn std::error::Error>> {
        let header = Header::parse("CDX|urlkey|timestamp|url|custom")?;
        let entry =
            header.parse_entry("com,example)/|20201007212236|https://example.com/|value")?;
        assert_eq!(header.to_string(), "CDX|urlkey|timestamp|url|custom");
        assert_eq!(
            header.render(&entry),
            "com,example)/|20201007212236|https://example.com/|value"
        );
        assert_eq!(header.decode(&entry)?.extra["custom"], "value");
        Ok(())
    }

    #[test]
    fn keeps_unmodeled_fields_under_their_markers() -> Result<(), Box<dyn std::error::Error>> {
        let header = Header::parse(" CDX N b a e c")?;
        let entry =
            header.parse_entry("com,example)/ 20201007212236 https://example.com/ 10.0.0.1 -")?;

        assert_eq!(header.field(3), Some(Field::Other(Cow::Borrowed("e"))));
        let entry = header.decode(&entry)?;
        assert_eq!(entry.extra["e"], "10.0.0.1");
        assert!(!entry.extra.contains_key("c"));
        Ok(())
    }

    #[proptest::property_test]
    fn header_and_entry_round_trip(
        #[strategy = prop::legend_and_values()] legend: (char, bool, Vec<String>, Vec<String>),
    ) {
        let (delimiter, leading_delimiter, markers, values) = legend;
        let header = Header::new(
            delimiter,
            leading_delimiter,
            markers.into_iter().map(Cow::Owned).collect(),
        )
        .unwrap();
        let entry = header
            .entry(values.into_iter().map(Cow::Owned).collect())
            .unwrap();
        let header_text = header.to_string();
        let entry_text = header.render(&entry);

        prop_assert_eq!(
            Header::parse(&header_text).map(Header::into_owned).ok(),
            Some(header.clone())
        );
        prop_assert_eq!(
            header.parse_entry(&entry_text).map(Entry::into_owned).ok(),
            Some(entry)
        );
    }

    #[proptest::property_test]
    fn the_standard_legend_recovers_every_value(
        #[strategy = prop::entry_parts()] parts: prop::EntryParts,
    ) {
        let entry = Header::standard_11().decode(&parts.entry()).unwrap();

        prop_assert_eq!(entry.key.as_ref(), parts.key.as_str());
        prop_assert_eq!(entry.timestamp, parts.timestamp);
        prop_assert_eq!(entry.url.as_ref(), parts.url.as_str());
        prop_assert_eq!(entry.mime.as_deref(), parts.mime.as_deref());
        prop_assert_eq!(entry.status, parts.status);
        prop_assert_eq!(entry.digest.as_deref(), parts.digest.as_deref());
        prop_assert_eq!(entry.redirect.as_deref(), parts.redirect.as_deref());
        prop_assert_eq!(entry.robot_flags.as_deref(), parts.robot_flags.as_deref());
        prop_assert_eq!(entry.length, parts.length);
        prop_assert_eq!(entry.offset, parts.offset);
        prop_assert_eq!(entry.filename.as_deref(), parts.filename.as_deref());
        prop_assert!(entry.original.is_none());
        prop_assert!(entry.extra.is_empty());
    }
}
