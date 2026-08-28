//! Property-testing strategies for text and HTTP URLs.

use std::ops::RangeInclusive;

use proptest::prelude::*;
use proptest::sample::select;
use url::Url;

/// Strings of one to `max` characters drawn from `chars`.
pub fn string_of(chars: &'static [char], max: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(select(chars), 1..=max).prop_map(|chars| chars.into_iter().collect())
}

/// Strings of `range` tokens drawn from `tokens`.
pub fn tokens_of(
    tokens: &'static [&'static str],
    range: RangeInclusive<usize>,
) -> impl Strategy<Value = String> {
    proptest::collection::vec(select(tokens), range).prop_map(|tokens| tokens.concat())
}

/// The path and query tokens a URL is built from, including characters RFC 3986 forbids bare.
const URL_TOKENS: &[&str] = &[
    "a", "Z", "0", "-", "~", "|", "^", "[", "]", "{", "}", "`", "é", "日",
];

/// An HTTP URL, optionally with credentials, a query, and a fragment.
///
/// # Panics
///
/// Panics during generation if the URL tokens violate the URL parser's syntax.
pub fn http_url() -> impl Strategy<Value = Url> {
    (
        select(vec!["http", "https"]),
        proptest::option::of(select(vec!["user:s3cret-token", "user", ":s3cret-token"])),
        select(vec!["example.com", "example.org:8080"]),
        proptest::collection::vec(tokens_of(URL_TOKENS, 0..=8), 0..=3),
        proptest::option::of(tokens_of(URL_TOKENS, 0..=8)),
        proptest::option::of(tokens_of(URL_TOKENS, 0..=8)),
    )
        .prop_map(
            |(scheme, credentials, authority, segments, query, fragment)| {
                let credentials = credentials.map_or_else(String::new, |value| format!("{value}@"));
                let path = segments
                    .iter()
                    .fold(String::new(), |path, segment| path + "/" + segment);
                let query = query.map_or_else(String::new, |query| format!("?q={query}"));
                let fragment = fragment.map_or_else(String::new, |fragment| format!("#{fragment}"));

                Url::parse(&format!(
                    "{scheme}://{credentials}{authority}{path}{query}{fragment}"
                ))
                .expect("invariant violation: a generated URL parses")
            },
        )
}

#[cfg(test)]
mod tests {
    use proptest::property_test;

    #[property_test]
    fn string_of_respects_its_alphabet_and_length(
        #[strategy = super::string_of(&['a', 'b'], 3)] text: String,
    ) {
        assert!((1..=3).contains(&text.chars().count()));
        assert!(text.chars().all(|c| c == 'a' || c == 'b'));
    }

    #[property_test]
    fn tokens_of_concatenates_whole_tokens(
        #[strategy = super::tokens_of(&["ab", "%20"], 0..=2)] text: String,
    ) {
        let mut rest = text.as_str();
        let mut count = 0;
        while !rest.is_empty() {
            rest = rest
                .strip_prefix("ab")
                .or_else(|| rest.strip_prefix("%20"))
                .expect("a whole token");
            count += 1;
        }
        assert!(count <= 2);
    }
}
