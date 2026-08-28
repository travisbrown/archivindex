//! The query model of the CDX server protocol.

use std::fmt;
use std::str::FromStr;

/// The scope used to match a requested URL.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum MatchType {
    /// Return captures of exactly the requested URL.
    Exact,
    /// Return captures whose URLs begin with the requested URL.
    Prefix,
    /// Return captures from the requested host.
    Host,
    /// Return captures from the requested host and its subdomains.
    Domain,
}

impl MatchType {
    /// The value accepted by the CDX server's `matchType` parameter.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Prefix => "prefix",
            Self::Host => "host",
            Self::Domain => "domain",
        }
    }
}

impl fmt::Display for MatchType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for MatchType {
    type Err = InvalidMatchType;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "exact" => Ok(Self::Exact),
            "prefix" => Ok(Self::Prefix),
            "host" => Ok(Self::Host),
            "domain" => Ok(Self::Domain),
            _ => Err(InvalidMatchType(value.to_owned())),
        }
    }
}

/// A value is not one of the match scopes supported by the CDX server.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid CDX match type: {0}")]
pub struct InvalidMatchType(String);

/// Parameters for one CDX server query.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    /// The URL or URL prefix to look up.
    pub url: String,
    /// The scope of URL matches to return.
    pub match_type: MatchType,
    /// Whether the server should use its faster latest-results lookup.
    pub fast_latest: bool,
    /// The maximum number of results. A negative value requests the last N results.
    ///
    /// When absent, the query leaves the limit up to the CDX server.
    #[serde(default)]
    pub limit: Option<i64>,
}

impl Request {
    /// Construct one CDX server request.
    #[must_use]
    pub fn new(
        url: impl Into<String>,
        match_type: MatchType,
        fast_latest: bool,
        limit: impl Into<Option<i64>>,
    ) -> Self {
        Self {
            url: url.into(),
            match_type,
            fast_latest,
            limit: limit.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InvalidMatchType, MatchType, Request};

    #[test]
    fn match_types_round_trip_through_their_protocol_values() {
        for match_type in [
            MatchType::Exact,
            MatchType::Prefix,
            MatchType::Host,
            MatchType::Domain,
        ] {
            assert_eq!(match_type.to_string(), match_type.as_str());
            assert_eq!(match_type.as_str().parse::<MatchType>(), Ok(match_type));
        }
    }

    #[test]
    fn an_unknown_scope_is_rejected() {
        assert_eq!(
            "everything".parse::<MatchType>(),
            Err(InvalidMatchType("everything".to_owned()))
        );
    }

    #[test]
    fn a_request_serializes_under_the_protocol_field_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = Request::new("example.com/docs/", MatchType::Prefix, true, -5);
        let json = serde_json::to_string(&request)?;

        assert_eq!(
            json,
            r#"{"url":"example.com/docs/","matchType":"prefix","fastLatest":true,"limit":-5}"#
        );
        assert_eq!(serde_json::from_str::<Request>(&json)?, request);

        Ok(())
    }

    /// An absent limit leaves the choice to the server rather than failing to parse.
    #[test]
    fn a_request_without_a_limit_parses() -> Result<(), Box<dyn std::error::Error>> {
        let request = serde_json::from_str::<Request>(
            r#"{"url":"example.com","matchType":"exact","fastLatest":false}"#,
        )?;

        assert_eq!(
            request,
            Request::new("example.com", MatchType::Exact, false, None)
        );

        Ok(())
    }
}
