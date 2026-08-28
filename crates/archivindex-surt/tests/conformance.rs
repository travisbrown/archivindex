//! Conformance with the reference implementations, checked against the fixtures under `data/`.
//!
//! Each fixture holds `url<TAB>expected` lines, where `expected` is `!error` when the reference
//! implementation rejects the URL; `#` lines and empty lines are ignored.

use archivindex_surt::Surt;
use archivindex_surt::url::{self, Canonicalizer};

/// One of the two key-rendering entry points, so the fixtures exercise both.
type Render = fn(&Canonicalizer, &str) -> Result<Surt<'static>, url::Error>;

fn check(name: &str, fixture: &str, canonicalizer: Canonicalizer, render: Render) {
    let failures = fixture
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (url, expected) = line
                .split_once('\t')
                .unwrap_or_else(|| panic!("{name}: `{line}` is not tab-separated"));
            let actual = render(&canonicalizer, url)
                .map_or_else(|_| "!error".to_owned(), |key| key.as_str().to_owned());

            (actual != expected.split(' ').next().unwrap_or(expected))
                .then(|| format!("{url}\n  expected: {expected}\n  got:      {actual}"))
        })
        .collect::<Vec<_>>();

    assert!(
        failures.is_empty(),
        "{name}: {} keys differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn matches_wayback_machine_urlkeys() {
    check(
        "wayback-urlkeys.tsv",
        include_str!("data/wayback-urlkeys.tsv"),
        Canonicalizer::WAYBACK,
        Canonicalizer::surt,
    );
}

#[test]
fn matches_python_surt() {
    check(
        "python-surt.tsv",
        include_str!("data/python-surt.tsv"),
        Canonicalizer::WAYBACK,
        Canonicalizer::surt,
    );
}

#[test]
fn matches_warcio() {
    check(
        "warcio.tsv",
        include_str!("data/warcio.tsv"),
        Canonicalizer::WARCIO,
        Canonicalizer::surt_ipv6_bracketed,
    );
}
