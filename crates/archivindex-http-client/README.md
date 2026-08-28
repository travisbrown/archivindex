# archivindex-http-client

HTTP clients for the [Archivindex](https://github.com/travisbrown/archivindex) projects that capture
the request and response of each exchange. Every fetch makes one request on a new connection.
Clients do not follow redirects, retry, keep cookies, decode content, pool connections, or read
proxy settings from the environment.

The workspace also contains
[`archivindex-http-client-challenge`](../archivindex-http-client-challenge/README.md). Its `Session`
wraps a client, automatically answers Sucuri, Varnish, and Simply.com challenges, and retains
clearance cookies. It returns every exchange, including challenge and verification responses.

## Clients

All clients implement `Client` and return an `Exchange`. `Client::engine` names the HTTP
implementation behind a client, with its version and browser emulation profile where it has them.
An `Engine` is displayed in the `User-Agent` product syntax, such as `wreq/0.16.1 (chrome_136)`.

| Client          | Feature | Protocols           | TLS       | Captured messages                            |
| --------------- | ------- | ------------------- | --------- | -------------------------------------------- |
| `Recorder`      |         | HTTP/1.1            | rustls    | Exact bytes                                  |
| `ReqwestClient` |         | HTTP/1.1            | rustls    | Reconstructed                                |
| `WreqClient`    | `wreq`  | HTTP/1.1 and HTTP/2 | BoringSSL | Exact for HTTP/1.1, reconstructed for HTTP/2 |

Use `Recorder` when the original HTTP/1 bytes matter. `ReqwestClient` reconstructs messages from
parsed parts. `WreqClient` adds browser emulation and optional HTTP/2 support.

## Usage

```rust,no_run
use archivindex_http_client::recorder::Recorder;
use archivindex_http_client::{Client, Request};
use http::{HeaderMap, Method};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let captured = Recorder::new().fetch(Request {
        method: &Method::GET,
        target: &"https://www.example.com/".parse()?,
        headers: &HeaderMap::new(),
        body: None,
    })?;

    println!("{}", captured.status);
    println!("{} body bytes", captured.entity_body()?.len());
    Ok(())
}
```

`Request` borrows its method, target, headers, and body. `Client::fetch_with_deadline` also takes an
optional absolute `Instant` deadline. Passing `None` is equivalent to calling `fetch`; configured
connection and I/O timeouts still apply. Calls are synchronous. The reqwest and wreq clients run
each fetch on a scoped thread with its own Tokio runtime, so callers can already be inside a
runtime.

Clients add a missing `host` header and default HTTP/1.1 requests to `connection: close`. They
remove caller-supplied `content-length` and `transfer-encoding` fields and frame a supplied body
with its actual length. `Some(&[])` supplies an empty body; `None` supplies no body.

Targets must be absolute HTTP or HTTPS URIs. Validation happens before network activity. URI
userinfo is removed from the transport target; only an explicit `authorization` header supplies
origin credentials. `Exchange::target_uri` retains the URI supplied by the caller.

## Captured exchanges

The request and response are returned as HTTP/1 messages, even when the connection used HTTP/2.
`fidelity` distinguishes exact bytes from reconstructed messages; `http_protocol` records the
protocol used on the connection. Interim `1xx` responses are discarded, so the captured response
starts at the final status line.

`request_metadata()` parses the captured request head, borrowing from `request`, and exposes its
method, target, headers, and body offset.

`status` and `response_body_offset` are read from the response head. `response_metadata()` parses
the head again to expose its header values, borrowing from `response`. `message_body()` returns the
bytes after the head without changing them. `entity_body()` removes transfer coding and preserves
content coding. It returns an empty body for `HEAD`, `204`, and `304` responses, and an error when
a declared chunked body has incomplete framing or trailers.

`tls_version` is the negotiated TLS version when the client can observe it. It is absent for
plaintext exchanges and for reqwest's SOCKS connections. `ip_address` identifies the origin and
is absent for proxied exchanges. `started_at` records when network activity began; `fetch_time`
records its elapsed duration. `truncated` is absent for a complete response and otherwise records
`Length`, `Time`, or `Disconnect`.

### Reconstruction

The reqwest client reconstructs the request from the parts it submits, after URL normalization
and the addition of default headers. For example, `/a/../b` becomes `/b`. This describes the
expected request; reqwest does not expose the bytes written to the connection.

Reconstructed response headers use lowercase names and normalized whitespace. The original
reason phrase, repeated fields, and content-encoded data are preserved. A chunked body
gets one generated chunk per delivered body frame, followed by its trailers. The origin's chunk
boundaries and extensions are lost. Content decoding stays disabled even when another dependency
enables reqwest's compression features.

Exact HTTP/1 captures preserve header casing, duplicate fields, reason phrases, chunk extensions,
and trailers. The recorder serializes its own request and records the response verbatim. Wreq
observes plaintext transport reads and writes.

## HTTP policies and message parsing

`prepare` combines request headers, normalizes URL targets, and rejects or redacts embedded
credentials. `redirect` resolves HTTP locations and rewrites methods, bodies, and origin-specific
headers. The challenge crate's `Session` combines redirects and challenge answers in one sequence
when redirect following is enabled.

`retry` supplies bounded exponential delays, `Retry-After` parsing (including obsolete HTTP date
forms), and status, transport-error, and truncation classification. Callers decide whether a
request is safe to repeat and perform the wait. A configured response length bound is not treated
as a transient failure.

`conditional` parses combined `Vary` declarations, matches selecting request fields, and applies
ETag and Last-Modified validators. Missing and empty fields remain distinct. An unreadable `Vary`
declaration, unavailable selecting request, or non-UTF-8 selecting value makes the representation
unselectable. A non-UTF-8 selecting value in a later request prevents a match. Persistence and
application-specific representation identities belong to the caller.

The [`archivindex-http`](../archivindex-http/) crate provides message parsing and body extraction.
Its `body::entity_body` uses `Framing::Declared`, requiring complete chunk framing when chunked
encoding is declared. `body::entity_body_with_policy(message, Framing::Inferred)` also accepts a
body already dechunked under stale transfer headers, and a final zero-size chunk without its
trailers. Older Common Crawl ARC and WARC captures retained `Transfer-Encoding: chunked` after
removing chunk framing, as described in Common Crawl's
[archive conversion notes][common-crawl-headers]. Both policies preserve content coding and reject
unsupported transfer codings or incomplete chunk data. `body::message_body` returns the bytes after
the head unchanged, including any chunk framing and trailers.

[common-crawl-headers]: https://github.com/commoncrawl/arc2warc-conversion#required-rewriting-of-http-headers

## Limits and timeouts

Connection and I/O timeouts default to 30 seconds. The response capture limit defaults to 256 MiB.
Their setters accept `None` to remove a bound.

The response limit includes the final head and transfer framing. It counts captured bytes,
including generated framing for reconstructed responses. Each head has a separate 64 KiB bound.
Interim heads do not consume the final response limit. A response that ends exactly at the limit
is complete.

A failure before a complete final head is an error. Once the head arrives, a size limit,
disconnect, timeout, or expired deadline retains the captured prefix and sets `truncated`.
Malformed framing remains an error. I/O failures preserve their error kind, including
`ConnectionRefused` and `TimedOut`.

| Client          | Connection timeout | I/O timeout                                      | Deadline     |
| --------------- | ------------------ | ------------------------------------------------ | ------------ |
| `Recorder`      | Each address tried | Each socket read or write                        | Excludes DNS |
| `ReqwestClient` | DNS, TCP, and TLS  | Wait for the response head, then each body frame | Wall clock   |
| `WreqClient`    | DNS, TCP, and TLS  | Idle time after connecting                       | Wall clock   |

The recorder uses blocking DNS. Reqwest's first I/O wait includes connecting and sending the
request. Wreq measures HTTP/2 progress through outgoing request frames and decoded response data;
connection control traffic cannot keep a stalled exchange alive. A system DNS lookup already
running when an asynchronous client times out may finish in the background.

## TLS and proxies

The recorder and reqwest trust `webpki-roots` and explicitly select the `aws-lc-rs` crypto provider.
They do not depend on the operating system trust store or the process's default provider. Their
`tls_config` setters accept a replacement configuration, for example to trust a private
certificate authority. Both restrict ALPN to `http/1.1`. Wreq accepts custom roots through
`tls_cert_store`. Default configurations verify certificates and hostnames.

Every client accepts the same SOCKS5 proxy URIs:

```rust,no_run
use archivindex_http_client::recorder::Recorder;

let client = Recorder::new().proxy(Some("socks5h://127.0.0.1:1080"))?;
# Ok::<(), archivindex_http_client::InvalidProxy>(())
```

Use `socks5h://` for proxy DNS or `socks5://` for local DNS. The default port is 1080. Optional
username and password credentials are percent-encoded, as in
`socks5h://user:p%40ssword@host:1080`. Other schemes are rejected.

Proxy failures never fall back to direct connections. SOCKS negotiation is subject to transport
timeouts and deadlines, and is excluded from captured messages. Proxied exchanges omit the origin
IP because the socket peer is the proxy and SOCKS does not reliably identify the origin address.

## Optional wreq client

Enable the `wreq` feature for browser emulation. It requires Rust 1.98 or later, a C and C++
compiler, CMake, and libclang to build BoringSSL.

```rust,ignore
use archivindex_http_client::wreq::{WreqClient, parse_profile};

let client = WreqClient::new(parse_profile("chrome_136")?);
let http2_client = client.clone().http2(true);
let firefox = client.profile(parse_profile("firefox_136")?);
```

The profile supplies TLS settings, default headers, header ordering and casing, and HTTP/2
settings. Explicit request headers override profile values. Unknown profile names return an
error. Applications can also supply `wreq_util::Profile` directly by depending on the pinned
`wreq-util` version. Browser emulation does not guarantee access to a site.

HTTP/2 is disabled by default, restricting ALPN to `http/1.1`. With `.http2(true)`, the client
offers the profile's protocols and uses HTTP/2 when the server selects it, otherwise HTTP/1.1.
Restricting ALPN changes that part of the emulated browser's TLS handshake.

HTTP/2 messages are reconstructed as HTTP/1.1 with `Fidelity::Reconstructed` and
`HttpProtocol::Http2`. Finalized request headers come from the codec after defaults and removal of
connection-specific fields. Outgoing frames must establish one complete request; an additional
stream or connection fails the capture. Pseudo-headers become the request line, `host`, or response
status. Responses with bodies use generated chunked framing so trailers stay separate from the
initial headers. `HEAD`, `204`, and `304` keep their bodyless form and representation headers. The
returned messages omit binary frames, HPACK state, and original framing declarations. The codec also
bounds decoded response headers and trailers before reconstruction.

Each fetch owns its client, observer, runtime, and connection. Concurrent fetches cannot share
capture state. For HTTP/1, a codec or write failure invalidates an unfinished capture. An already
complete or truncated response is retained; a later shutdown failure does not invalidate it.

### Dependency patches

The observer hooks are not released upstream. The crate uses a pinned wreq fork and matching TLS
revisions. Cargo does not propagate dependency patches. A consuming workspace that enables `wreq`
must copy these entries into its root manifest, including when using a local path dependency:

```toml
[patch.crates-io]
wreq = { git = "https://github.com/travisbrown/wreq", rev = "108701e00f33b40132e78d28abce8b4f6e3a6b19" }
btls = { git = "https://github.com/0x676e67/btls", rev = "9d859deefab0183e2fccf91204c818c8d1805b27" }
btls-sys = { git = "https://github.com/0x676e67/btls", rev = "9d859deefab0183e2fccf91204c818c8d1805b27" }
tokio-btls = { git = "https://github.com/0x676e67/btls", rev = "9d859deefab0183e2fccf91204c818c8d1805b27" }
```

The manifest pins matching versions of `wreq`, `wreq-proto`, and `wreq-util`. Publication is
disabled while the optional client depends on these patches.

## Development

This crate contains the HTTP clients, their tests, and their benchmarks. The workspace root
manifest defines shared package metadata, dependencies, lints, and dependency patches. The sibling
`archivindex-http-client-challenge` crate contains the challenge recognizers, bounded proof
solvers, cookie jar, and automatic session.

`framing` locates response boundaries and applies limits. `archivindex-http` parses HTTP/1
messages and extracts entity-bodies. `reconstruct` serializes parsed parts into HTTP/1 messages.
Transport setup, request preparation, and error classification stay private.

```sh
cargo +nightly fmt --all -- --check
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo test --workspace --locked --no-default-features
cargo test --workspace --locked --all-features
cargo +1.98.0 check --workspace --locked --all-targets --all-features
cargo bench -p archivindex-http-client --locked --bench response_capture -- --test
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --locked --all-features --no-deps
```

All clients run shared conformance and proxy tests against local HTTP, TLS, and SOCKS servers.
Separate suites check exact capture, HTTP/2, and TLS configuration. Tests cover framing across
read boundaries, every cap through chunked trailers, deadlines, disconnects, authentication,
profile overrides, concurrent calls, and calls inside Tokio. The in-memory benchmarks exercise
short responses, long headers, and long chunk extensions at different read sizes.

CI checks both feature configurations on Linux, baseline clients on macOS and Windows, Rust 1.98
compatibility, and fresh compatible dependencies on stable Rust. It also checks formatting,
documentation, and the dependency policy.

## License

GPL-3.0-only. See [LICENSE](LICENSE).
