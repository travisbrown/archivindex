# archivindex-http-client-challenge

Challenge handling for the capture clients in
[`archivindex-http-client`](../archivindex-http-client/). A session answers recognized challenges,
retains clearance cookies, and returns every exchange, including challenge pages and verification
responses. This lets archiving tools preserve the sequence that led to a resource.

| Challenge  | Support                                                 |
| ---------- | ------------------------------------------------------- |
| Sucuri     | Decode the challenge's clearance cookie                 |
| Varnish    | Solve a SHA-256 proof and send trace and bypass cookies |
| Simply.com | Solve a SHA-256 proof and submit it for clearance       |

Recognition uses a limited set of script expressions without executing JavaScript. Work limits and
an optional overall deadline bound each sequence. Unsupported or truncated challenges are returned
as captured responses, so callers can inspect them or choose another approach.

Sessions support optional redirect following and hooks for request preparation and capture
processing. The recognizers can also be used in an application's own request loop.

## Scope

This is a companion to the Archivindex clients, not a browser automation engine. It handles only
the listed challenges and cannot answer compressed challenge pages. Its cookie store keeps
caller-supplied and clearance cookies for exact hosts; it does not implement a general browser
cookie store or process ordinary `Set-Cookie` responses. Successful fetching does not guarantee
that a site granted clearance.

The API is synchronous. This crate is not published to crates.io. See the
[workspace requirements](../../README.md#development) and
[build the Rustdoc reference](../../README.md#documentation) for usage and session behavior.

## License

GPL-3.0-only. See [LICENSE](LICENSE).
