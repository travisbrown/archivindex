//! Parsed views of HTTP/1 messages.
//!
//! [`ResponseMetadata`] and [`RequestMetadata`] read a message's start line and field lines and
//! locate its body. They leave the input bytes unchanged and borrow from them, copying a field
//! value only to unfold its continuation lines.
//!
//! A message head consists of the start line, header field lines, and terminating empty line.
//! The header section contains the field lines alone.

use std::borrow::Cow;
use std::str::Utf8Error;

use crate::parsing::{Head, HeaderLine, is_token, scan_head};

/// Parsed fields and boundaries of a recorded HTTP response message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseMetadata<'a> {
    /// Status from the recorded response's first line.
    pub status: u16,
    /// Offset at which the recorded message body begins.
    pub body_offset: usize,
    headers: Fields<'a>,
}

impl<'a> ResponseMetadata<'a> {
    /// Parse an HTTP response beginning with a complete message head.
    #[must_use]
    pub fn parse(response: &'a [u8]) -> Option<Self> {
        let (head, headers) = parse_head(response)?;
        let mut parts = head.start_line.splitn(3, |&byte| byte == b' ');
        let version = parts.next()?;
        let code = parts.next()?;
        if !version.starts_with(b"HTTP/") || code.len() != 3 || !code.iter().all(u8::is_ascii_digit)
        {
            return None;
        }
        let status = code
            .iter()
            .fold(0, |value, &byte| value * 10 + u16::from(byte - b'0'));

        Some(Self {
            status,
            body_offset: head.body_offset,
            headers,
        })
    }

    /// Copy every borrowed field, so that the metadata can outlive the parsed message.
    #[must_use]
    pub fn into_owned(self) -> ResponseMetadata<'static> {
        ResponseMetadata {
            status: self.status,
            body_offset: self.body_offset,
            headers: self
                .headers
                .into_iter()
                .map(|(name, value)| {
                    (
                        Cow::Owned(name.into_owned()),
                        Cow::Owned(value.into_owned()),
                    )
                })
                .collect(),
        }
    }

    /// Return the first response header value, matched case-insensitively.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        header_value(&self.headers, name)
    }

    /// Return every response header value with this name, in the order received.
    ///
    /// Fields allowing comma-separated values, such as `Vary`, may be sent as several lines
    /// (RFC 9110 section 5.3). Other fields, such as `Set-Cookie`, must be kept separate.
    pub fn headers<'s>(&'s self, name: &'s str) -> impl Iterator<Item = &'s [u8]> {
        header_values(&self.headers, name)
    }

    /// Return the response header value with this name as one combined line.
    ///
    /// A single value is borrowed; multiple values are joined with a comma and a space
    /// (RFC 9110 section 5.3). Use this only for fields that allow comma-separated values, such as
    /// `Vary`, and not for `Set-Cookie`. Returns `Ok(None)` when the field is absent.
    ///
    /// # Errors
    ///
    /// Returns an error if any value is not valid UTF-8.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::borrow::Cow;
    ///
    /// use archivindex_http::message::ResponseMetadata;
    ///
    /// let response = b"HTTP/1.1 200 OK\r\nVary: Accept-Encoding\r\nVary: User-Agent\r\n\r\n";
    /// let metadata = ResponseMetadata::parse(response).unwrap();
    /// assert_eq!(
    ///     metadata.combined_header("vary").unwrap(),
    ///     Some(Cow::Owned("Accept-Encoding, User-Agent".to_owned()))
    /// );
    /// assert_eq!(metadata.combined_header("etag"), Ok(None));
    /// ```
    pub fn combined_header(&self, name: &str) -> Result<Option<Cow<'_, str>>, Utf8Error> {
        join_field_values(header_values(&self.headers, name), ',')
    }
}

/// Parsed fields and boundaries of a recorded HTTP request message.
///
/// The response's `Vary` header identifies request fields needed to match a later request against
/// the stored response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestMetadata<'a> {
    /// Offset at which the recorded message body begins.
    pub body_offset: usize,
    method: &'a str,
    target: &'a [u8],
    headers: Fields<'a>,
}

impl<'a> RequestMetadata<'a> {
    /// Parse an HTTP request beginning with a complete message head.
    ///
    /// The method must be a token and the target must be nonempty. The remaining part of the
    /// request line must begin with `HTTP/`; the version syntax is not fully validated.
    #[must_use]
    pub fn parse(request: &'a [u8]) -> Option<Self> {
        let (head, headers) = parse_head(request)?;
        let mut parts = head.start_line.splitn(3, |&byte| byte == b' ');
        let method = parts.next()?;
        let target = parts.next()?;
        let version = parts.next()?;
        if !is_token(method) || target.is_empty() || !version.starts_with(b"HTTP/") {
            None
        } else {
            Some(Self {
                body_offset: head.body_offset,
                // A token is ASCII by definition, so this cannot fail after the check above.
                method: std::str::from_utf8(method).ok()?,
                target,
                headers,
            })
        }
    }

    /// Return the request method, as it was sent.
    #[must_use]
    pub const fn method(&self) -> &'a str {
        self.method
    }

    /// Return the request target, as it was sent.
    ///
    /// RFC 9112 confines a request target to ASCII, but a recorded request may carry raw octets
    /// that a client sent unencoded, so this is exposed as bytes rather than as text.
    #[must_use]
    pub const fn target(&self) -> &'a [u8] {
        self.target
    }

    /// Return the first request header value, matched case-insensitively.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        header_value(&self.headers, name)
    }

    /// Return every request header value with this name, in the order received.
    pub fn headers<'s>(&'s self, name: &'s str) -> impl Iterator<Item = &'s [u8]> {
        header_values(&self.headers, name)
    }

    /// Return the request header value with this name as one combined line.
    ///
    /// Matching a request against a `Vary` field compares the selecting fields as combined values
    /// (RFC 9111 section 4.1). See [`combined_request_header`] for field-specific separators.
    /// Returns `Ok(None)` when the field is absent.
    ///
    /// # Errors
    ///
    /// Returns an error if any value is not valid UTF-8.
    pub fn combined_header(&self, name: &str) -> Result<Option<Cow<'_, str>>, Utf8Error> {
        combined_request_header(name, header_values(&self.headers, name))
    }
}

/// Combine request header values, using `; ` for `Cookie` and `, ` for other fields.
///
/// The field name is matched case-insensitively. Cookie pairs use a semicolon and a space
/// (RFC 6265 section 5.4 and RFC 9113 section 8.2.3). For other fields, use this only when their
/// grammar allows comma-separated values. A single value is borrowed. Returns `Ok(None)` for no
/// values.
///
/// # Errors
///
/// Returns an error if any value is not valid UTF-8.
pub fn combined_request_header<'a>(
    name: &str,
    lines: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Option<Cow<'a, str>>, Utf8Error> {
    let separator = if name.eq_ignore_ascii_case("cookie") {
        ';'
    } else {
        ','
    };

    join_field_values(lines, separator)
}

fn join_field_values<'a>(
    lines: impl IntoIterator<Item = &'a [u8]>,
    separator: char,
) -> Result<Option<Cow<'a, str>>, Utf8Error> {
    let mut lines = lines.into_iter();

    lines.next().map_or(Ok(None), |first| {
        let first = std::str::from_utf8(first)?;

        lines.next().map_or_else(
            || Ok(Some(Cow::Borrowed(first))),
            |second| {
                let mut combined = first.to_owned();

                for line in std::iter::once(second).chain(lines) {
                    combined.push(separator);
                    combined.push(' ');
                    combined.push_str(std::str::from_utf8(line)?);
                }

                Ok(Some(Cow::Owned(combined)))
            },
        )
    })
}

/// A field line's name, as sent, and its value.
///
/// Both borrow from the message, except for a value unfolded from continuation lines.
type Field<'a> = (Cow<'a, str>, Cow<'a, [u8]>);

/// The field lines of a message head, in the order received.
type Fields<'a> = Vec<Field<'a>>;

/// Split a message head into its start line, its body offset, and its unfolded field lines.
///
/// Accept `CRLF` and bare `LF` line endings. RFC 9112 section 2.2 permits recipients to accept a
/// bare `LF`.
///
/// Returns `None` when the head is unterminated or a field name is not an HTTP token.
fn parse_head(message: &[u8]) -> Option<(Head<'_>, Fields<'_>)> {
    let mut headers = Fields::new();
    let head = scan_head(message, |line| {
        match line {
            HeaderLine::Field {
                name,
                value,
                whitespace_before_colon: false,
            } => {
                let name = std::str::from_utf8(name).ok()?;
                headers.push((Cow::Borrowed(name), Cow::Borrowed(value.trim_ascii())));
            }
            HeaderLine::Continuation(content) => {
                let value = headers.last_mut()?.1.to_mut();
                value.push(b' ');
                value.extend_from_slice(content.trim_ascii());
            }
            _ => return None,
        }

        Some(())
    })?;

    Some((head, headers))
}

fn header_values<'h, 'n>(
    headers: &'h [Field<'h>],
    name: &'n str,
) -> impl Iterator<Item = &'h [u8]> + use<'h, 'n> {
    headers
        .iter()
        .filter(move |(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_ref())
}

/// The first value with this name, borrowed from `headers` alone so a caller may pass a temporary.
fn header_value<'h>(headers: &'h [Field<'h>], name: &str) -> Option<&'h [u8]> {
    header_values(headers, name).next()
}

#[cfg(test)]
mod tests;
