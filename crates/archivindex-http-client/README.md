# archivindex-http-client

HTTP clients for archiving request and response messages, with capture fidelity reported for
each exchange. Captures include timing, protocol, and available connection metadata, and retain
partial responses when a size limit, timeout, or disconnect interrupts a transfer.

These clients serve the [Archivindex](../../README.md) archiving tools. Each fetch records one
request and response on a new connection. Redirects, retries, cookies, and decompression are left
to the calling application so it can account for every exchange.

## Capture options

| Client          | Protocols           | TLS       | Captured messages                            |
| --------------- | ------------------- | --------- | -------------------------------------------- |
| `Recorder`      | HTTP/1.1            | rustls    | Exact bytes                                  |
| `ReqwestClient` | HTTP/1.1            | rustls    | Reconstructed                                |
| `WreqClient`    | HTTP/1.1 and HTTP/2 | BoringSSL | Exact for HTTP/1.1, reconstructed for HTTP/2 |

`Recorder` preserves HTTP/1 header spelling, reason phrases, chunk framing, and trailers.
`ReqwestClient` uses reqwest's parsed messages, so some original formatting is lost. `WreqClient`
adds browser emulation and optional HTTP/2 support. All clients return HTTP/1 message captures;
HTTP/2 frames and HPACK state are not retained.

The API is synchronous and supports SOCKS5 proxies, custom certificate trust, response size limits,
and deadlines. Helpers cover request preparation, redirects, retry decisions, and conditional
requests. For automatic challenge handling and clearance cookies, see
[`archivindex-http-client-challenge`](../archivindex-http-client-challenge/).
For parsing stored messages, see [`archivindex-http`](../archivindex-http/).

## Integration

The `wreq` feature requires a native build toolchain and matching dependency forks with unreleased
capture hooks. See [build instructions and dependency patches](BUILDING.md). Browser emulation does
not guarantee access to a site.

This crate is not published to crates.io. See the
[workspace requirements](../../README.md#development) and
[build the Rustdoc reference](../../README.md#documentation) for API usage and examples.

## License

GPL-3.0-only. See [LICENSE](LICENSE).
