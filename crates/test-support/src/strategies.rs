//! Property-testing strategies for text built from a fixed alphabet.

use std::ops::RangeInclusive;

use proptest::prelude::*;
use proptest::sample::select;

/// Strings of one to `max` characters drawn from `chars`.
pub fn string_of(chars: Vec<char>, max: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(select(chars), 1..=max).prop_map(|chars| chars.into_iter().collect())
}

/// Strings of `range` tokens drawn from `tokens`.
pub fn tokens_of(
    tokens: &'static [&'static str],
    range: RangeInclusive<usize>,
) -> impl Strategy<Value = String> {
    proptest::collection::vec(select(tokens), range).prop_map(|tokens| tokens.concat())
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    #[proptest]
    fn string_of_respects_its_alphabet_and_length(
        #[strategy(super::string_of(vec!['a', 'b'], 3))] text: String,
    ) {
        assert!((1..=3).contains(&text.chars().count()));
        assert!(text.chars().all(|c| c == 'a' || c == 'b'));
    }

    #[proptest]
    fn tokens_of_concatenates_whole_tokens(
        #[strategy(super::tokens_of(&["ab", "%20"], 0..=2))] text: String,
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
