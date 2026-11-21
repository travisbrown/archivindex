# archivindex-surt

[SURT][surt] keys and URL canonicalization for web archives.

A SURT reorders a URL's host so that keys for related pages sort together: `www.example.com`
becomes `com,example,www`, and everything under `example.com` shares the prefix `com,example`.
These keys are used in archive indexes such as [CDX](../archivindex-cdx/).

The crate supports the Wayback Machine and [warcio.js][warcio] normalization conventions, with
configurable rules for URL equivalence. For example, the default Wayback rules map
`https://www.Example.com:443/Movies/?b=2&a=1#top` to `com,example)/movies?a=1&b=2`.

Rendering supports standard SURT, [Heritrix][heritrix] SURT, and [SSURT][ssurt]. Rendering and URL
normalization are separate choices; selecting Heritrix's key syntax does not select its
canonicalization policy.

## Integrations

| Feature      | Default | Support                               |
| ------------ | ------- | ------------------------------------- |
| `serde`      | yes     | Serialize and deserialize SURT keys   |
| `fluent-uri` | yes     | Convert to and from `fluent_uri::Uri` |
| `url`        | no      | Convert to and from `url::Url`        |

See the [Rustdoc reference](https://docs.rs/archivindex-surt) for parsing keys and configuring
canonicalization.

## License

This crate is licensed under either the [MIT License][mit] or the
[Apache License, Version 2.0][apache-2.0], at your option.

[apache-2.0]: https://www.apache.org/licenses/LICENSE-2.0
[heritrix]: https://github.com/internetarchive/heritrix3
[mit]: https://opensource.org/license/mit
[ssurt]: https://github.com/iipc/urlcanon/blob/master/ssurt.rst
[surt]: http://crawler.archive.org/articles/user_manual/glossary.html#surt
[warcio]: https://github.com/webrecorder/warcio.js
