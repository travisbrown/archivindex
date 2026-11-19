//! Match stored responses to requests using `Vary`, and apply HTTP validators.

use std::borrow::Cow;
use std::str::Utf8Error;

use archivindex_http::message::ResponseMetadata;

/// Read all `Vary` lines from a recorded response as one comma-separated value.
///
/// An invalid UTF-8 value produces `*`, making the response
/// [`Unselectable`](Variance::Unselectable). Returns `None` when no `Vary` field is present.
/// Pass the result to [`Variance::declared`] or [`Variance::declared_without_request`].
#[must_use]
pub fn declared_vary(metadata: &ResponseMetadata<'_>) -> Option<String> {
    metadata.combined_header("vary").map_or_else(
        |_| Some(Variance::UNSELECTABLE.to_owned()),
        |value| value.map(Cow::into_owned),
    )
}

/// The request headers that must match before a stored response's validators can be reused.
///
/// For example, `Vary: User-Agent` prevents a request with a different user agent from reusing the
/// stored `ETag`. With an absent or empty `Vary`, any request for the same URI can reuse them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Variance {
    /// The response's `Vary` is absent or empty, so every request for the URI selects it.
    #[default]
    Invariant,
    /// The stored representation cannot be selected by request header fields.
    ///
    /// Either the response declared `Vary: *`, or it named selecting fields whose values in the
    /// originating request are unknown or unreadable. These cannot be matched, so state stored under
    /// this variance is never reused for revalidation.
    Unselectable,
    /// The response named selecting fields, recorded with the originating request's values.
    Selected(SelectingHeaders),
}

impl Variance {
    /// The `Vary` wildcard that prevents selection by request fields.
    const UNSELECTABLE: &'static str = "*";

    /// Record the variance a response declared, resolved against the request that produced it.
    ///
    /// `vary` is the response's `Vary` field value, absent when the response declares none. `field`
    /// returns the request's value for a lowercase field name, or `Ok(None)` when the request sent no
    /// such field; a field that was not sent is distinct from one sent empty. A field sent as
    /// several lines is resolved as their combined value, using `; ` for `Cookie` and `, ` for
    /// fields allowing comma-separated values. Values are compared as sent; differences in
    /// whitespace or list order prevent a match even when the values are equivalent.
    ///
    /// UTF-8 errors and values containing line breaks yield [`Variance::Unselectable`]. Callers
    /// must unfold recorded header values before passing them here.
    #[must_use]
    pub fn declared<V: AsRef<str>>(
        vary: Option<&str>,
        mut field: impl FnMut(&str) -> Result<Option<V>, Utf8Error>,
    ) -> Self {
        let Some(vary) = vary else {
            return Self::Invariant;
        };
        let mut entries = Vec::new();

        for name in vary.split(',') {
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            if name == Self::UNSELECTABLE {
                return Self::Unselectable;
            }
            let name = name.to_ascii_lowercase();
            let Ok(value) = field(&name) else {
                return Self::Unselectable;
            };
            let value = value.as_ref().map(AsRef::as_ref);
            if name.contains(['\r', '\n'])
                || value.is_some_and(|value| value.contains(['\r', '\n']))
            {
                return Self::Unselectable;
            }
            entries.push((name.into_boxed_str(), value.map(Box::from)));
        }

        if entries.is_empty() {
            return Self::Invariant;
        }
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        entries.dedup_by(|(left, _), (right, _)| left == right);

        Self::Selected(SelectingHeaders { entries })
    }

    /// Record the variance a response declared when the originating request is unavailable.
    ///
    /// Without the original request, revalidation is possible only if `Vary` is absent or empty.
    /// Otherwise the result is [`Variance::Unselectable`].
    #[must_use]
    pub fn declared_without_request(vary: Option<&str>) -> Self {
        match vary {
            Some(vary) if vary.split(',').any(|name| !name.trim().is_empty()) => Self::Unselectable,
            _ => Self::Invariant,
        }
    }

    /// Whether a request selects the representation this state was stored for.
    ///
    /// Send the stored validators only when this returns `true`.
    ///
    /// `field` follows the same contract as in [`Variance::declared`]. A UTF-8 error in any
    /// selecting field prevents a match.
    #[must_use]
    pub fn matches<V: AsRef<str>>(
        &self,
        mut field: impl FnMut(&str) -> Result<Option<V>, Utf8Error>,
    ) -> bool {
        match self {
            Self::Invariant => true,
            Self::Unselectable => false,
            Self::Selected(headers) => headers.entries.iter().all(|(name, value)| {
                field(name).is_ok_and(|candidate| {
                    candidate.as_ref().map(AsRef::as_ref) == value.as_deref()
                })
            }),
        }
    }
}

/// The values a request carried for the fields a response named in `Vary`.
///
/// Names are lowercased and sorted, so one selection compares equal to another regardless of the
/// order or case the server wrote its `Vary` field in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectingHeaders {
    entries: Vec<(Box<str>, Option<Box<str>>)>,
}

impl SelectingHeaders {
    /// Restore stored selecting fields. Names must already be lowercase, sorted, and unique.
    #[must_use]
    pub const fn from_entries(entries: Vec<(Box<str>, Option<Box<str>>)>) -> Self {
        Self { entries }
    }

    /// Iterate over normalized field names and their original values, including absent fields.
    pub fn iter(&self) -> impl Iterator<Item = (&str, Option<&str>)> {
        self.entries
            .iter()
            .map(|(name, value)| (name.as_ref(), value.as_deref()))
    }
}

/// Resolve a request field as a combined value, returning `Ok(None)` when absent.
///
/// Cookie pairs are joined with `; `; other fields use `, ` and must allow comma-separated values.
///
/// # Errors
///
/// Returns an error if any value is not valid UTF-8.
pub fn request_field<'a>(
    headers: &'a http::HeaderMap,
    name: &str,
) -> Result<Option<Cow<'a, str>>, Utf8Error> {
    archivindex_http::message::combined_request_header(
        name,
        headers
            .get_all(name)
            .iter()
            .map(http::HeaderValue::as_bytes),
    )
}

/// Validators of a previously obtained representation.
#[derive(Clone, Debug, Default)]
pub struct Validators {
    etag: Option<http::HeaderValue>,
    last_modified: Option<http::HeaderValue>,
}

impl Validators {
    /// Parse usable validators, ignoring values that cannot be sent as header fields.
    #[must_use]
    pub fn new(etag: Option<&str>, last_modified: Option<&str>) -> Option<Self> {
        let etag = etag.and_then(|value| http::HeaderValue::from_str(value).ok());
        let last_modified = last_modified.and_then(|value| http::HeaderValue::from_str(value).ok());
        (etag.is_some() || last_modified.is_some()).then_some(Self {
            etag,
            last_modified,
        })
    }

    /// Add conditional fields, replacing any caller-supplied value of the same name.
    pub fn apply(&self, headers: &mut http::HeaderMap) {
        if let Some(etag) = &self.etag {
            headers.insert(http::header::IF_NONE_MATCH, etag.clone());
        }
        if let Some(modified) = &self.last_modified {
            headers.insert(http::header::IF_MODIFIED_SINCE, modified.clone());
        }
    }
}

#[cfg(test)]
mod tests;
