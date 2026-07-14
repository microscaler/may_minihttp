# may_minihttp Client — Design Document

## Status (2026-07-14)

**Phase 1 shipped in-tree:** native HTTP/1.1 client under `may_minihttp::client`, feature-gated
(`client`). Drop-in compatible with `may_http::client::{HttpClient, Request, Response}` — BRRTRouter
migrates here; the abandoned `may_http` crate is retired.

The client supports absolute `http://` and `https://` URLs. HTTPS is implemented with rustls, the
platform certificate verifier, and an explicit ring crypto provider. A cloneable high-level
`Client` now adds a replay-aware `RequestBuilder`, bounded host/TLS-keyed pooling, buffered and
lease-owning streaming responses, an opt-in redirect policy, typed error classification, and
operational counters. It also supports sanitized request lifecycle observers and bounded resolver
adapters for cached DNS or push-updated service registries. An optional cooperative token safely
cancels may-aware waits and I/O with a typed, observable outcome. A bounded request metadata
provider injects rotating service credentials and trace context without adding JWT or tracing
policy to the transport. Immutable rustls configuration snapshots add generation-safe mTLS
identity and trust rotation. The low-level `HttpClient` remains available for direct streaming and
`may_http` compatibility.

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
let rsp = client.send_request(req)?; // explicitly finishes the request and propagates write errors
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

`Client` is `Clone + Send + Sync`. It checks out one exclusive HTTP/1.1 connection per request. The
default path buffers up to `max_response_body`; `send_streaming` keeps the lease until EOF. A
transport is checked back in only when framing and persistence rules make reuse safe. Early drops,
cancellation, and read errors discard it without blocking drain I/O. Pool capacity waits use
`may::sync::Condvar`; the pool mutex is not held during DNS, connect, TLS, writes, or reads.

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
    ├── cancellation.rs     # cloneable cooperative request cancellation
    ├── metadata.rs         # bounded rotating request metadata provider
    ├── tls.rs              # immutable rustls snapshots and rotation policy
    ├── observer.rs         # sanitized request lifecycle events
    ├── resolver.rs         # system, bounded cache, and service registry resolvers
    └── rich.rs             # Client, pool, redirects, buffered/streaming responses
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

### Phase 2: Rich API (current delivery complete)

- `RequestBuilder` and bounded `BufferedResponse` ✅
- Backwards-compatible `ClientError`/`ClientErrorKind` classification over retained `io::Error` ✅
- JSON request/response helpers (`json` feature) ✅
- Multipart/form-data text/byte encoder with exact length and direct request streaming ✅
- Explicit bounded `blocking_reader`/`blocking_file` preload boundary ✅
- Single-use streaming request reader and lease-owning streaming response ✅

PUT/DELETE/PATCH one-line shortcuts and a middleware abstraction are optional future API
conveniences, not requirements of the current IDAM delivery.

### Phase 3: Robustness

- Host/TLS-keyed connection pooling with bounded per-origin and total connections ✅
- Redirect handling with opt-in policy, hop limit, loop detection, and cross-origin credential stripping ✅
- Separate connect, read/write, and total request deadlines ✅
- One stale-idle retry for idempotent requests with replayable bodies ✅
- Cancellation-safe RAII pool accounting and incomplete-body discard ✅
- Strict response framing, bounded headers/chunks/trailers, and explicit request finalisation ✅

### Phase 4: Production

- TLS in the `client` feature ✅
- Monotonic connection, wait, retry, and redirect counters ✅
- Lock-free-callback request lifecycle observation with sanitized event data ✅
- Bounded positive/negative resolver caching, single-flight refresh, and address rotation ✅
- Push-updated service resolution with no request-path DNS I/O ✅
- Cooperative token cancellation with typed and observable outcomes ✅
- Bounded default/request/provider header precedence and rotating metadata injection ✅
- Generation-keyed TLS identity/trust rotation with explicit last-known-good policy ✅
- Compression and HTTP/2 remain optional future capabilities if `may` supports them

## Resolved Questions

1. **Separate crate vs in-tree?** → In `may_minihttp`, feature-gated.
2. **Replace `may_http`?** → Yes for Microscaler consumers (BRRTRouter). Fork archived after migration.
3. **`http` version?** → `0.2` in client module only; workspace `http 1.0` for BRRTRouter server types unchanged.
4. **Body buffering?** → Buffer via `Read` on `Response` (same as `may_http`).

## Open Questions

1. Upstream contribution to Xudong-Huang/may_minihttp — proceed locally; upstream ping optional.
2. Upstream API shape for a future breaking 0.2 rename (`HttpClient` connection vs pooled `Client`).

## Capability gap register

Direct `reqwest` use in BRRTRouter test tooling exposed the following gaps. The production client
now closes them without introducing Tokio, Hyper, reqwest, or AWS-LC into its normal feature graph.

| Capability | Classification | Current position | Acceptance for closure |
|---|---|---|---|
| JSON request/response helpers | Ergonomic | Delivered behind the optional `json` feature | Correct content type; encode/decode errors; unit and in-process integration tests |
| Multipart/form-data | Functional | Delivered with replayable text/bytes and explicit bounded blocking preload helpers | Generated boundary; injection-safe metadata; exact content length; direct-to-request streaming; blocking filesystem boundary is unmistakable |
| Redirects | Functional and security-sensitive | Delivered on pooled `Client` | Disabled by default; bounded hops; relative `Location`; loop detection; credential stripping; downgrade rejection |
| Connection pooling | Functional/performance | Delivered on pooled `Client` | Keyed by scheme/host/port/TLS identity; bounded per-origin/total entries; idle/lifetime expiry; cancellation-safe lease accounting |
| Connect vs request deadline | Functional/operational | Delivered on pooled `Client` | DNS and address attempts share the connect budget; I/O, pool wait, and total request deadlines are finite |
| Large request/response streaming | Functional/performance | Delivered with single-use readers and pool-aware streaming responses | No replay of reader bodies; EOF permits reuse; early drop/error discards without drain-on-drop I/O |
| Error/operations surface | Operational | Delivered without breaking `io::Result` callers | Typed classification retains source error; monotonic counters expose creates, reuse, discard, waits, retries, redirects |
| Request observation | Operational | Delivered through an optional `ClientObserver` | Stable request IDs and phase/outcome events; no paths, queries, headers, or bodies; callbacks outside client locks; cooperative cancellation emits after cleanup |
| Service resolution | Functional/operational | Delivered through `CachingResolver` and `ServiceResolver` | Bounded TTL/cache/address counts; single-flight cold lookup; address rotation; explicit invalidation; logical Host/SNI retained |
| Cooperative cancellation | Functional/operational | Delivered through `CancellationToken` | May-scoped request race; typed `Cancelled`; prompt may-I/O wakeup; incomplete transport discard; one terminal event |
| Request metadata | Security/operational | Delivered through `RequestMetadataProvider` | Per-attempt refresh; bounded headers; request precedence; transport-owned framing; redacted typed failure; redirect credential stripping |
| TLS identity/trust rotation | Security/operational | Delivered through `TlsConfigProvider` | Immutable per-request snapshot; generation-keyed pool; retired idle discard; redacted fail-closed or explicit last-known-good policy; rustls/ring only |
| Async/Tokio API | Deliberate non-goal | Sync call surface over coroutine-aware `may::net` I/O | Keep core runtime-neutral from Tokio; use an adapter only when an external async harness requires it |

Ordinary functional tests should use `Client` or `HttpClient` so their HTTP parsing, header behavior,
TLS, and coroutine scheduling match production. A test-only client remains reasonable only where an
external harness genuinely requires an async/Tokio API, provided the exception is documented.
