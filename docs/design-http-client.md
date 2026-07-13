# may_minihttp Client — Design Document

## Status (2026-07-14)

**Phase 1 shipped in-tree:** native HTTP/1.1 client under `may_minihttp::client`, feature-gated
(`client`). Drop-in compatible with `may_http::client::{HttpClient, Request, Response}` — BRRTRouter
migrates here; the abandoned `may_http` crate is retired.

The client supports absolute `http://` and `https://` URLs. HTTPS is implemented with rustls, the
platform certificate verifier, and an explicit ring crypto provider. A cloneable high-level
`Client` now adds a replay-aware `RequestBuilder`, bounded host/TLS-keyed pooling, bounded buffered
responses, and an opt-in redirect policy. The low-level `HttpClient` remains available for streaming
and `may_http` compatibility.

The concurrency and policy design is specified in
[`design-strict-may-client.md`](./design-strict-may-client.md).

---

## Why a client in may_minihttp?

`may_http` (~6 years unmaintained) is a thin wrapper around `may::net::TcpStream`. `may_minihttp` is
actively maintained (same author: Xudong Huang) and already owns HTTP parsing for the server side.

Adding a **native** client here:

1. One crate for may HTTP server + client (Microscaler fork already in BRRTRouter / Sesame-IDAM)
2. Fixes like DELETE/PUT/PATCH `write_head` live in the maintained crate, not a dead fork
3. Server types (`request.rs`, `response.rs`) stay unchanged — client is a separate module
4. Feature-gated (`client`) so server-only builds are unchanged

## Current may_minihttp API

### Server (existing, unchanged)

```rust
HttpServer<Service>::start(addr) -> JoinHandle<()>
Request::method() / path() / headers() / body()
Response::status_code() / header() / body()
```

### Low-level client (`feature = "client"`)

```rust
use may_minihttp::client::{HttpClient, Request, Response};

let mut client = HttpClient::connect(("127.0.0.1", 8080))?;
client.set_timeout(Some(Duration::from_secs(30)));

// Shortcuts
let rsp = client.get("/health".parse()?)?;
let rsp = client.post("/api".parse()?, body_bytes)?;

// URL-aware connection (port and Host header derived from the URL)
let mut secure = HttpClient::from_url("https://identity.example.com")?;
let rsp = secure.get("/idam/v1/.well-known/jwks.json".parse()?)?;

// Full control (DELETE, PUT, PATCH, custom headers)
let mut req = client.new_request(Method::DELETE, "/fleet/42".parse()?);
// req.headers_mut().insert(...);
drop(req); // writes head on Drop
let rsp = client.send_request(req)?; // or get_rsp after drop(req)
```

API mirrors `may_http::client` so BRRTRouter `proxy.rs` / `fetch.rs` need only import path changes.

### Pooled client

```rust
use may_minihttp::client::{Client, RedirectPolicy};

let client = Client::builder()
    .max_connections(64)
    .max_connections_per_origin(8)
    .redirect_policy(RedirectPolicy::SameOrigin { max_hops: 5 })
    .build()?;
let rsp = client.get("https://identity.example.com/health")?.send()?;
```

`Client` is `Clone + Send + Sync`. It checks out one exclusive HTTP/1.1 connection per request,
buffers the response up to `max_response_body`, and only checks a transport back in when framing and
persistence rules make reuse safe. Pool capacity waits use `may::sync::Condvar`; the pool mutex is
not held during DNS, connect, TLS, writes, or reads.

## Architecture

```
may_minihttp/src/
├── lib.rs
├── http_server.rs          # existing server
├── request.rs              # existing server request (httparse)
├── response.rs             # existing server response
└── client/                 # feature = "client"
    ├── mod.rs
    ├── buffer.rs           # BufferIo over the selected transport
    ├── body/               # BodyReader, BodyWriter
    ├── client_impl.rs      # HttpClient
    ├── request.rs          # outgoing streaming Request (http 0.2)
    ├── response.rs         # incoming streaming Response (httparse decode)
    ├── shared.rs           # Send-capable, may-Mutex transport plumbing
    ├── multipart.rs        # replayable text/byte multipart encoding
    └── rich.rs             # Client, pool, RequestBuilder, redirects, buffered response
```

**No `may_http` dependency.** The transport is either a plain `may::net::TcpStream` or a rustls
`StreamOwned<ClientConnection, TcpStream>`, both behind `BufferIo`. HTTPS uses the operating
system trust configuration by default. Callers can inject an `Arc<rustls::ClientConfig>` for a
private CA or mTLS.

### DELETE / PUT / PATCH

`Request::write_head` handles all methods. Non-GET/HEAD/POST with no `Content-Length` assume an empty
body (no `Transfer-Encoding: chunked` for those methods). Call `set_content_length` before writing
when the body size is known.

Regression tests in `client/request.rs`.

## Dependencies

| Crate | Scope | Notes |
|-------|-------|-------|
| `http = "0.2"` | `client` feature only | Uri, Method, StatusCode, HeaderMap |
| `rustls` (ring provider) | `client` feature only | TLS 1.2/1.3 transport; AWS-LC disabled |
| `rustls-platform-verifier` | `client` feature only | Operating-system certificate verification |
| `httparse`, `bytes`, `log`, `may` | always | shared with server |

Server continues to use `httparse` directly — no type mixing between server and client modules.

## Evolution Plan

### Phase 1: Core client (this PR) ✅

- Native `HttpClient`, `Request`, `Response` — `may_http`-compatible API
- GET, POST, DELETE, PUT, PATCH via `new_request` / `send_request`
- Feature gate `client`
- BRRTRouter migration off `may_http`
- Absolute HTTP/HTTPS URL connection, default ports, and automatic `Host` header
- Platform certificate verification plus injected TLS configuration for private CA/mTLS

### Phase 2: Rich API

- `RequestBuilder` and bounded `BufferedResponse` ✅
- Domain `Error` enum remains future work; current errors use `io::Error`
- JSON request/response helpers (`json` feature) ✅
- Multipart/form-data text/byte encoder with exact length and direct request streaming ✅
- File/reader multipart parts without blocking a may scheduler thread
- PUT/DELETE/PATCH convenience methods on `HttpClient`
- Middleware trait

### Phase 3: Robustness

- Host/TLS-keyed connection pooling with bounded per-origin and total connections ✅
- Redirect handling with opt-in policy, hop limit, loop detection, and cross-origin credential stripping ✅
- Separate connect, read/write, and total request deadlines ✅
- Retries with idempotency rules, plus streaming bodies for large responses

### Phase 4: Production

- TLS in the `client` feature ✅
- Metrics, compression, HTTP/2 if `may` supports it

## Resolved Questions

1. **Separate crate vs in-tree?** → In `may_minihttp`, feature-gated.
2. **Replace `may_http`?** → Yes for Microscaler consumers (BRRTRouter). Fork archived after migration.
3. **`http` version?** → `0.2` in client module only; workspace `http 1.0` for BRRTRouter server types unchanged.
4. **Body buffering?** → Buffer via `Read` on `Response` (same as `may_http`).

## Open Questions

1. Upstream contribution to Xudong-Huang/may_minihttp — proceed locally; upstream ping optional.
2. Rich client API (Phase 2) vs minimal compat layer — compat first, richness when Sesame-IDAM needs it.

## Capability gap register

The remaining direct `reqwest` use in BRRTRouter test tooling exposed the following gaps. This is a
classification of the existing evolution plan, not part of the HTTPS delivery slice.

| Capability | Classification | Current position | Acceptance for closure |
|---|---|---|---|
| JSON request/response helpers | Ergonomic | Delivered behind the optional `json` feature | Correct content type; encode/decode errors; unit and in-process integration tests |
| Multipart/form-data | Functional | Text and in-memory byte parts delivered; non-blocking file/reader source remains | Generated boundary; injection-safe metadata; exact content length; direct-to-request streaming; file-source design must preserve may scheduling |
| Redirects | Functional and security-sensitive | Delivered on pooled `Client` | Disabled by default; bounded hops; relative `Location`; loop detection; credential stripping; downgrade rejection |
| Connection pooling | Functional/performance | Delivered on pooled `Client` | Keyed by scheme/host/port/TLS identity; bounded per-origin/total entries; idle/lifetime expiry |
| Connect vs request deadline | Functional/operational | Delivered on pooled `Client` | Independent connect, I/O, pool-wait, and total deadlines |
| Async/Tokio API | Deliberate non-goal | Sync call surface over coroutine-aware `may::net` I/O | Keep core runtime-neutral from Tokio; use an adapter only when an external async harness requires it |

Ordinary functional tests should use `Client` or `HttpClient` so their HTTP parsing, header behavior,
TLS, and coroutine scheduling match production. A test-only client remains reasonable only where an
external harness genuinely requires an async/Tokio API, provided the exception is documented.
