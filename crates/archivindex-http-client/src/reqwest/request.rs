//! Complete the submitted request and reconstruct its HTTP/1 message.

use http::{HeaderValue, Version, header};

use crate::reconstruct::reconstruct_request;
use crate::{Error, Request, request};

pub fn prepare(request: Request<'_>) -> Result<(reqwest::Request, Vec<u8>), Error> {
    let target = request::target(request.target);
    let url = reqwest::Url::parse(&target.to_string())?;
    // Reconstruct from the normalized URL that reqwest will send, including its authority.
    let sent_target = url.as_str().parse::<http::Uri>()?;
    let authority = sent_target.authority().ok_or(Error::MissingHost)?;
    let mut headers = request::headers(authority, request.headers);
    if let Some(body) = request.body {
        headers.append(header::CONTENT_LENGTH, HeaderValue::from(body.len()));
    }
    headers
        .entry(header::ACCEPT)
        .or_insert(HeaderValue::from_static("*/*"));

    let request_bytes = reconstruct_request(
        request.method,
        &sent_target,
        Version::HTTP_11,
        &headers,
        request.body,
    );
    let mut prepared = reqwest::Request::new(request.method.clone(), url);
    *prepared.headers_mut() = headers;
    *prepared.body_mut() = request.body.map(|body| reqwest::Body::from(body.to_vec()));

    Ok((prepared, request_bytes))
}

#[cfg(test)]
mod tests {
    use archivindex_http::message::RequestMetadata;
    use http::{HeaderMap, Method};

    use super::prepare;
    use crate::{Error, Request};

    /// The HTTP URI parser accepts an authority that the URL parser rejects as an IPv6 address.
    #[test]
    fn url_validation_keeps_the_parse_error_type() {
        let target = "http://[invalid]/".parse().unwrap();
        let result = prepare(Request {
            method: &Method::GET,
            target: &target,
            headers: &HeaderMap::new(),
            body: None,
        });

        assert!(matches!(
            result,
            Err(Error::InvalidUrl(url::ParseError::InvalidIpv6Address))
        ));
    }

    /// URL normalization applies to the default Host field as well as the request target.
    #[test]
    fn normalized_authority_and_explicit_host_are_preserved() {
        for host in [None, Some("virtual.example")] {
            let target = "http://user:password@EXAMPLE.COM:80/a/../b?q=1"
                .parse()
                .unwrap();
            let mut headers = HeaderMap::new();
            if let Some(host) = host {
                headers.insert("host", host.parse().unwrap());
            }
            let (request, request_bytes) = prepare(Request {
                method: &Method::GET,
                target: &target,
                headers: &headers,
                body: None,
            })
            .unwrap();
            let metadata = RequestMetadata::parse(&request_bytes).unwrap();

            assert_eq!(request.url().as_str(), "http://example.com/b?q=1");
            assert_eq!(metadata.target(), b"/b?q=1");
            assert_eq!(
                metadata.header("host"),
                Some(host.unwrap_or("example.com").as_bytes())
            );
            assert_eq!(metadata.header("authorization"), None);
            assert_eq!(request.headers()["host"], host.unwrap_or("example.com"));
        }
    }
}
