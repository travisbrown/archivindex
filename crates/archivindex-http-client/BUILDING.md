# Building the HTTP clients

The baseline clients need the workspace's Rust toolchain. The optional `wreq` feature also needs
C and C++ compilers, CMake, and libclang to build BoringSSL.

## Using the wreq feature in another workspace

The capture hooks are not released upstream. Copy these patches into your workspace's root
manifest, even when depending on Archivindex through a local path. Keep the revisions matched to
the Archivindex checkout you use.

```toml
[patch.crates-io]
wreq = { git = "https://github.com/travisbrown/wreq", rev = "54d02b68b764fe5b39ec9c1256d50e8a6fd08f1f" }
wreq-util = { git = "https://github.com/travisbrown/wreq-util", rev = "69af0555e88cb2882de7a4250a40a828d7eb9a61" }
btls = { git = "https://github.com/0x676e67/btls", rev = "3c1dd080ba617d78c63cd09285d2ce31abcdcbf9" }
btls-sys = { git = "https://github.com/0x676e67/btls", rev = "3c1dd080ba617d78c63cd09285d2ce31abcdcbf9" }
tokio-btls = { git = "https://github.com/0x676e67/btls", rev = "3c1dd080ba617d78c63cd09285d2ce31abcdcbf9" }
http2 = { git = "https://github.com/0x676e67/http2", rev = "ddb99c6db7222b1c3b9c72d1286b22b1dfb32918" }
netty = { git = "https://github.com/0x676e67/netty", rev = "abaa73d5964af776424f7f5bbd351af9bf52a555" }
```

## Validation

See the [workspace development instructions](../../README.md#development) and
[CI workflow](../../.github/workflows/ci.yml) for build and validation commands. To run the response
capture benchmarks:

```console
cargo bench -p archivindex-http-client --locked --bench response_capture
```
