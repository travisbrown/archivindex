# archivindex-surt

[SURT][surt] keys and URL canonicalization for web archives.

A SURT reorders a URL's host so that keys for related pages sort together: `www.example.com`
becomes `com,example,www`, and everything under `example.com` shares the prefix `com,example`.

```rust
use archivindex_surt::Surt;

let key = Surt::from_url("https://www.Example.com:443/Movies/?b=2&a=1#top").unwrap();

assert_eq!(key.as_str(), "com,example)/movies?a=1&b=2");
assert_eq!(key.labels().collect::<Vec<_>>(), ["com", "example"]);
assert_eq!(key.url().to_string(), "https://example.com/movies?a=1&b=2");
```

`Surt::from_url` applies the Wayback Machine's rules for deciding which URLs share a key. For
example, it lowercases the URL and removes a leading `www` from the host. Use `url::Canonicalizer`
to choose the rules used by [warcio.js][warcio] or to configure individual rules yourself.
`Canonicalizer::COMMON_ONLY` applies only the common normalization rules. You can then use
`url::Url` to write the result as a SURT, a [Heritrix][heritrix] SURT, or an [SSURT][ssurt].
Heritrix SURT rendering is independent of Heritrix's configurable canonicalization policy.

Tests compare the results with fixtures from the Python [surt][surt-python] library,
[warcio.js][warcio], and the Wayback Machine's own `urlkey` values.

## Features

| Feature          | Default | Description                                           |
| ---------------- | ------- | ----------------------------------------------------- |
| `serde`          | yes     | Serialize and deserialize `Surt`                      |
| `bounded-static` | yes     | `ToBoundedStatic` and `IntoBoundedStatic` conversions |
| `fluent-uri`     | yes     | Conversions to and from `fluent_uri::Uri`             |
| `url`            | no      | Conversions to and from `url::Url`                    |

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[heritrix]: https://github.com/internetarchive/heritrix3
[mit]: https://opensource.org/license/mit
[ssurt]: https://github.com/iipc/urlcanon/blob/master/ssurt.rst
[surt]: http://crawler.archive.org/articles/user_manual/glossary.html#surt
[surt-python]: https://github.com/internetarchive/surt
[warcio]: https://github.com/webrecorder/warcio.js
