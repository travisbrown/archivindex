use std::{fmt::Display, str::FromStr};

pub const STATUS_CODE_VALUES: [StatusCode; 20] = [
    StatusCode::Empty,
    StatusCode::Ok,
    StatusCode::MovedPermanently,
    StatusCode::Found,
    StatusCode::SeeOther,
    StatusCode::TemporaryRedirest,
    StatusCode::BadRequest,
    StatusCode::Unauthorized,
    StatusCode::Forbidden,
    StatusCode::NotFound,
    StatusCode::UpgradeRequired,
    StatusCode::TooManyRequests,
    StatusCode::RequestHeaderFieldsTooLarge,
    StatusCode::InternalServerError,
    StatusCode::BadGateway,
    StatusCode::ServiceUnavailable,
    StatusCode::GatewayTimeout,
    StatusCode::CloudflareUnknownError,
    StatusCode::CloudflareWebServerDown,
    StatusCode::CloudflareTimeout,
];

#[derive(thiserror::Error, Debug, Eq, PartialEq)]
pub enum Error {
    #[error("Unsupported status code")]
    Unsupported,
}

/// An HTTP status code.
///
/// This is a simplified representation that only provides coverage for values relevant to our CDX
/// index results. The serialization encoding provided here is the one seen in these results.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub enum StatusCode {
    // Represents a hyphen in the CDX result, which typically indicates a `200` response.
    #[serde(alias = "-")]
    Empty,
    #[serde(alias = "200")]
    Ok,
    // Temporary redirect.
    #[serde(alias = "301")]
    MovedPermanently,
    // Temporary redirect.
    #[serde(alias = "302")]
    Found,
    #[serde(alias = "303")]
    SeeOther,
    #[serde(alias = "307")]
    TemporaryRedirest,
    #[serde(alias = "400")]
    BadRequest,
    #[serde(alias = "401")]
    Unauthorized,
    #[serde(alias = "403")]
    Forbidden,
    #[serde(alias = "404")]
    NotFound,
    #[serde(alias = "426")]
    UpgradeRequired,
    // Temporary redirect.
    #[serde(alias = "429")]
    TooManyRequests,
    #[serde(alias = "431")]
    RequestHeaderFieldsTooLarge,
    #[serde(alias = "500")]
    InternalServerError,
    #[serde(alias = "502")]
    BadGateway,
    #[serde(alias = "503")]
    ServiceUnavailable,
    #[serde(alias = "504")]
    GatewayTimeout,
    #[serde(alias = "520")]
    CloudflareUnknownError,
    #[serde(alias = "521")]
    CloudflareWebServerDown,
    #[serde(alias = "524")]
    CloudflareTimeout,
}

impl StatusCode {
    /// Integer value of the status code.
    ///
    /// Note that this returns zero for an empty value, even though these typically indicate a
    /// `200` response. Use the `From` instance for `http::status::StatusCode` if you want a
    /// logical status code.
    pub const fn value(&self) -> u16 {
        match self {
            Self::Empty => 0,
            Self::Ok => 200,
            Self::MovedPermanently => 301,
            Self::Found => 302,
            Self::SeeOther => 303,
            Self::TemporaryRedirest => 307,
            Self::BadRequest => 400,
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::UpgradeRequired => 426,
            Self::TooManyRequests => 429,
            Self::RequestHeaderFieldsTooLarge => 431,
            Self::InternalServerError => 500,
            Self::BadGateway => 502,
            Self::ServiceUnavailable => 503,
            Self::GatewayTimeout => 504,
            Self::CloudflareUnknownError => 520,
            Self::CloudflareWebServerDown => 521,
            Self::CloudflareTimeout => 524,
        }
    }

    pub const fn from_value(value: u16) -> Result<Self, Error> {
        match value {
            0 => Ok(Self::Empty),
            200 => Ok(Self::Ok),
            301 => Ok(Self::MovedPermanently),
            302 => Ok(Self::Found),
            303 => Ok(Self::SeeOther),
            307 => Ok(Self::TemporaryRedirest),
            400 => Ok(Self::BadRequest),
            401 => Ok(Self::Unauthorized),
            403 => Ok(Self::Forbidden),
            404 => Ok(Self::NotFound),
            426 => Ok(Self::UpgradeRequired),
            429 => Ok(Self::TooManyRequests),
            431 => Ok(Self::RequestHeaderFieldsTooLarge),
            500 => Ok(Self::InternalServerError),
            502 => Ok(Self::BadGateway),
            503 => Ok(Self::ServiceUnavailable),
            504 => Ok(Self::GatewayTimeout),
            520 => Ok(Self::CloudflareUnknownError),
            521 => Ok(Self::CloudflareWebServerDown),
            524 => Ok(Self::CloudflareTimeout),
            _ => Err(Error::Unsupported),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "-",
            Self::Ok => "200",
            Self::MovedPermanently => "301",
            Self::Found => "302",
            Self::SeeOther => "303",
            Self::TemporaryRedirest => "307",
            Self::BadRequest => "400",
            Self::Unauthorized => "401",
            Self::Forbidden => "403",
            Self::NotFound => "404",
            Self::UpgradeRequired => "426",
            Self::TooManyRequests => "429",
            Self::RequestHeaderFieldsTooLarge => "431",
            Self::InternalServerError => "500",
            Self::BadGateway => "502",
            Self::ServiceUnavailable => "503",
            Self::GatewayTimeout => "504",
            Self::CloudflareUnknownError => "520",
            Self::CloudflareWebServerDown => "521",
            Self::CloudflareTimeout => "524",
        }
    }
}

impl Display for StatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StatusCode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "-" => Ok(Self::Empty),
            "200" => Ok(Self::Ok),
            "301" => Ok(Self::MovedPermanently),
            "302" => Ok(Self::Found),
            "303" => Ok(Self::SeeOther),
            "307" => Ok(Self::TemporaryRedirest),
            "400" => Ok(Self::BadRequest),
            "401" => Ok(Self::Unauthorized),
            "403" => Ok(Self::Forbidden),
            "404" => Ok(Self::NotFound),
            "426" => Ok(Self::UpgradeRequired),
            "429" => Ok(Self::TooManyRequests),
            "431" => Ok(Self::RequestHeaderFieldsTooLarge),
            "500" => Ok(Self::InternalServerError),
            "502" => Ok(Self::BadGateway),
            "503" => Ok(Self::ServiceUnavailable),
            "504" => Ok(Self::GatewayTimeout),
            "520" => Ok(Self::CloudflareUnknownError),
            "521" => Ok(Self::CloudflareWebServerDown),
            "524" => Ok(Self::CloudflareTimeout),
            _ => Err(Self::Err::Unsupported),
        }
    }
}

impl From<StatusCode> for http::status::StatusCode {
    /// Note that the Cloudflare error status codes are converted to the generic `500`.
    fn from(value: StatusCode) -> Self {
        match value {
            StatusCode::Empty => http::status::StatusCode::OK,
            StatusCode::Ok => http::status::StatusCode::OK,
            StatusCode::MovedPermanently => http::status::StatusCode::MOVED_PERMANENTLY,
            StatusCode::Found => http::status::StatusCode::FOUND,
            StatusCode::SeeOther => http::status::StatusCode::SEE_OTHER,
            StatusCode::TemporaryRedirest => http::status::StatusCode::TEMPORARY_REDIRECT,
            StatusCode::BadRequest => http::status::StatusCode::BAD_REQUEST,
            StatusCode::Unauthorized => http::status::StatusCode::UNAUTHORIZED,
            StatusCode::Forbidden => http::status::StatusCode::FORBIDDEN,
            StatusCode::NotFound => http::status::StatusCode::NOT_FOUND,
            StatusCode::UpgradeRequired => http::status::StatusCode::UPGRADE_REQUIRED,
            StatusCode::TooManyRequests => http::status::StatusCode::TOO_MANY_REQUESTS,
            StatusCode::RequestHeaderFieldsTooLarge => {
                http::status::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
            }
            StatusCode::InternalServerError => http::status::StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BadGateway => http::status::StatusCode::BAD_GATEWAY,
            StatusCode::ServiceUnavailable => http::status::StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GatewayTimeout => http::status::StatusCode::GATEWAY_TIMEOUT,
            StatusCode::CloudflareUnknownError => http::status::StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::CloudflareWebServerDown => http::status::StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::CloudflareTimeout => http::status::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[cfg(test)]
mod test {
    impl quickcheck::Arbitrary for super::StatusCode {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            // Safe because we know the slice is non-empty.
            *g.choose(&super::STATUS_CODE_VALUES).unwrap()
        }
    }

    #[test]
    fn ordering() {
        let mut values = super::STATUS_CODE_VALUES.to_vec();

        values.sort();
        assert_eq!(values, super::STATUS_CODE_VALUES);

        values.sort_by_key(|status_code| status_code.value());
        assert_eq!(values, super::STATUS_CODE_VALUES);
    }

    #[test]
    fn round_trip_value() {
        for status_code in super::STATUS_CODE_VALUES {
            let status_code_value = status_code.value();
            let parsed = super::StatusCode::from_value(status_code_value);

            assert_eq!(parsed, Ok(status_code));
        }
    }

    #[test]
    fn round_trip_str() {
        for status_code in super::STATUS_CODE_VALUES {
            let status_code_str = status_code.to_string();
            let parsed = status_code_str.parse();

            assert_eq!(parsed, Ok(status_code));
        }
    }

    #[test]
    fn round_trip_json() {
        for status_code in super::STATUS_CODE_VALUES {
            let status_code_json = serde_json::json!(status_code);
            let parsed: super::StatusCode =
                serde_json::from_str(&status_code_json.to_string()).unwrap();

            assert_eq!(parsed, status_code);
        }
    }
}
