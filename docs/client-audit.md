# HTTP Client Audit: Inter-Service Fitness

Branch: `integration/microscaler-fork`

Date: 2026-07-14

Scope: coroutine-native east-west HTTP communication for Microscaler services

## Decision

The client is complete enough for bounded, secure HTTP/1.1 inter-service communication. It should
not aim for feature parity with curl, browsers, or `reqwest`. Request-level observation, strict
service-resolution primitives, cooperative request cancellation, bounded rotating request
metadata, and generation-safe TLS identity/trust rotation are now delivered. Further client work
is discovery-gated rather than required for the current inter-service baseline.

HTTP/2 and response decompression are discovery-gated optimisations. Proxy discovery, cookie jars,
WebSockets, URL credentials, HSTS, and a Tokio-native pool are not core client requirements.

Implementation-ready candidate stories are indexed in
[`client-audit/README.md`](./client-audit/README.md).

## Client Architecture Summary

The feature-gated client currently contains 8,517 lines across 16 modules:

| Module | Lines | Purpose |
|---|---:|---|
| `client_impl.rs` | 538 | Low-level, single-connection `HttpClient` |
| `rich.rs` | 4,291 | Pooled `Client`, builders, redirects, deadlines, leases, cancellation, metadata/TLS rotation, and event emission |
| `request.rs` | 606 | Request framing and body serialization |
| `response.rs` | 462 | Response parsing and body dispatch |
| `body_reader.rs` | 478 | Sized, chunked, EOF, and empty readers |
| `body_writer.rs` | 285 | Sized, chunked, and empty writers |
| `body/mod.rs` | 5 | Body reader/writer module boundary |
| `shared.rs` | 163 | Plain/TLS shared transport plumbing |
| `buffer.rs` | 214 | Buffered transport I/O |
| `multipart.rs` | 304 | Bounded multipart encoding and preload boundaries |
| `observer.rs` | 127 | Sanitized request lifecycle event API |
| `cancellation.rs` | 138 | Cloneable cooperative cancellation token and typed I/O marker |
| `resolver.rs` | 700 | System, bounded cache, and push-updated service resolution |
| `metadata.rs` | 106 | Policy-neutral, redacted request metadata provider API |
| `tls.rs` | 65 | Immutable rustls snapshot and rotation-provider API |
| `mod.rs` | 35 | Module layout and public exports |

There are 114 client-module unit test functions and 22 dedicated client integration tests, in
addition to doctests and the wider server/performance suites.

## Evaluation Criteria

Capabilities are prioritised when they improve at least one of:

1. predictable latency under partial failure;
2. service discovery and endpoint rotation;
3. workload identity, credential safety, or TLS trust;
4. bounded resource use and cancellation;
5. request-level diagnostics without leaking sensitive data;
6. throughput demonstrated by a representative Microscaler workload.

Compatibility with arbitrary public websites or interactive command-line usage is not sufficient
justification by itself.

## Delivered Inter-Service Baseline

| Area | Delivered behaviour |
|---|---|
| Protocol | Strict HTTP/1.0 and HTTP/1.1 response parsing; bounded headers, chunks, trailers, and bodies; ambiguous framing rejected |
| Requests | Standard methods, immutable bytes, optional JSON, bounded multipart, and single-use reader bodies |
| Responses | Bounded buffering by default and lease-owning streaming when requested |
| TLS | rustls TLS 1.2/1.3 with ring, platform verification, static private CA/mTLS configuration, and generation-safe identity/trust rotation |
| Pooling | Scheme/host/port/TLS-generation-keyed pool with global/per-origin bounds, idle/lifetime expiry, retired-generation discard, and coroutine-aware waiting |
| Deadlines | Connect, I/O, pool wait, and total request budgets with checked arithmetic |
| Failure safety | Cancellation-safe RAII leases, explicit cooperative cancellation, partial-body discard, and one stale-idle retry for idempotent replayable requests |
| Redirects | Disabled by default; bounded same/cross-origin policies with credential stripping and downgrade protection |
| Errors | Backwards-compatible `io::Result` plus stable `ClientErrorKind` classification |
| Operations | Monotonic connection, reuse, discard, wait, retry, and redirect counters |
| Observation | Optional sanitized lifecycle events for starts, waits, resolution, connection/reuse/discard, responses, redirects/retries, cancellation, abandonment, and terminal outcomes |
| Resolution | Compatible injected `Resolver`; bounded positive/negative cache with single-flight refresh; push-updated registry with no request-path DNS; address rotation and invalidation |
| Request metadata | Optional per-attempt provider plus default headers; bounded precedence; redacted typed failures; redirect credential suppression; no JWT/OAuth/tracing policy |
| Runtime boundary | No Tokio, Hyper, reqwest, or AWS-LC in the normal client feature graph |

## Corrections to the Previous Audit

The earlier reqwest comparison contained several incorrect or misleading findings:

- `308 Permanent Redirect` is handled alongside 307 and requires a replayable body.
- 301/302 do **not** silently downgrade POST to GET. They are followed only for GET/HEAD; this is the
  safer service-client behaviour. 303 performs the explicit GET conversion.
- `Client` is cloneable and shares its pool through `Arc<ClientInner>`.
- resolved addresses are already attempted within a single connect budget; exponential backoff
  between addresses is not a missing HTTP feature.
- a process-local pool using may synchronization is the intended architecture, not a deficiency.
- cancelling the owning may coroutine releases pool capacity safely; the delivered cooperative
  token adds a safe cross-coroutine API and typed/observable outcome.
- HTTP/1.0 responses are reusable only when they explicitly advertise keep-alive; the client does
  not claim to inject `Connection: close` into every HTTP/1.0 exchange.
- `RequestBuilder::reader` controls request streaming. `send_streaming` independently controls
  whether the response is buffered.
- multipart files are bounded, explicit blocking preloads; filesystem reads are not hidden inside
  the request coroutine.

## Prioritised Work

| Priority | Candidate | Status | Why it matters | Story |
|---|---|---|---|---|
| P0 | Request lifecycle observation | Delivered | Distributed diagnosis needs request timing and outcome data, not only pool totals | [CA-01](./client-audit/stories/CA-01-request-observation.md) |
| P0 | May-aware resolver and service discovery | Delivered | The default OS resolver can block; internal endpoints and addresses rotate | [CA-02](./client-audit/stories/CA-02-service-discovery-resolver.md) |
| P1 | Cooperative cancellation | Delivered | Shutdown, abandoned upstream requests, and request races need a safe abort path | [CA-03](./client-audit/stories/CA-03-cooperative-cancellation.md) |
| P1 | Request metadata provider | Delivered | Rotating service credentials and trace context are applied consistently without transport-level JWT policy | [CA-04](./client-audit/stories/CA-04-request-metadata-provider.md) |
| P1 | TLS identity rotation | Delivered | Certificate/trust rotation cannot reuse connections created under an obsolete TLS identity | [CA-05](./client-audit/stories/CA-05-tls-identity-rotation.md) |
| P2 discovery | Bounded decompression | Evidence required | May reduce bandwidth for large payloads, but only if workloads justify complexity and risk | [CA-06](./client-audit/stories/CA-06-bounded-decompression.md) |
| P2 discovery | HTTP/2 feasibility | Evidence required | Multiplexing may help high-concurrency origins, but must fit strict may architecture and measured demand | [CA-07](./client-audit/stories/CA-07-http2-feasibility.md) |

The delivered CA-01 through CA-05 items are transport primitives, not service-discovery,
authorization, token-acquisition, certificate-issuance, secret-store, or observability-backend
policy. Only evidence-gated P2 discovery items remain.

## Capabilities That Belong Above the Transport

The following are useful to service communication but should live in BRRTRouter, a typed service
client, or a dedicated resilience layer:

- application retry policy, backoff, retry budgets, and `Retry-After` interpretation;
- circuit breaking and endpoint health scoring;
- load balancing, hedging, and failover across logical service instances;
- JWT/bearer acquisition, refresh, audience selection, and authorization policy;
- mapping HTTP statuses and response bodies into domain errors;
- deciding whether an application operation is safe to retry.

The transport should expose replayability, typed failures, deadlines, observations, and metadata
hooks needed by those layers. It should not silently retry application responses.

## Conditional or Egress-Only Capabilities

| Capability | Position |
|---|---|
| Explicit HTTP/HTTPS proxy | Add only for a known egress or corporate-network requirement; do not silently honour environment variables for east-west traffic |
| TCP keepalive options | Potentially useful after measurement; HTTP/1.1 has no portable application-level ping |
| `.text()` / `.error_for_status()` | Low-risk ergonomics, but not a production-readiness gap; domain clients often need the original error body |
| Generic middleware chain | Prefer the bounded observer and metadata-provider interfaces first; add a chain only if concrete consumers need more composition |
| Certificate pinning | Use only for a defined trust model; private CA or mTLS configuration and rotation are usually better service-identity mechanisms |

## Explicit Non-Goals

- cookie or browser-session management;
- WebSocket support;
- URL-embedded credentials;
- HSTS browser policy;
- transparent environment proxy discovery;
- a Tokio/async compatibility pool;
- reqwest API compatibility;
- curl-style output, interactive authentication, or arbitrary public-site compatibility;
- following redirects by default.

These are deliberate exclusions, not an unfinished curl-compatibility backlog:

| Excluded surface | Why it is not useful to the may inter-service client |
|---|---|
| Arbitrary public-site compatibility | East-west callers have controlled contracts, trust, payload bounds, and redirect policy |
| Curl-style CLI, output formatting, uploads, and interactive auth | These are human tooling concerns, not transport primitives used by service coroutines |
| Browser state and policy (`CookieJar`, HSTS, credential URLs) | Service identity and authorization are explicit; hidden ambient state is unsafe |
| Automatic environment proxy discovery | East-west routing must not change because of process environment; explicit egress can be added for a concrete deployment |
| WebSockets and generic upgrade handling | No current Microscaler service contract requires them; they introduce a different connection lifecycle |
| Reqwest/Tokio API parity | It would duplicate another runtime and weaken the strict may dependency and scheduling boundary |

## Non-Functional Requirements for Future Client Work

All accepted stories must preserve these invariants:

1. no Tokio, Hyper, reqwest, or AWS-LC in the normal dependency graph;
2. no hidden blocking I/O on may scheduler workers;
3. no pool lock held during DNS, connect, TLS, observer callbacks, request I/O, or response I/O;
4. one total request deadline remains authoritative across every phase and retry;
5. request/response bodies, decompressed data, metadata, queues, and caches are bounded;
6. tokens, cookies, authorization values, full query strings, and bodies are not logged by default;
7. cancellation and callback failures cannot leak pool capacity or permit an incomplete connection
   to return to the pool;
8. new behaviour is disabled or conservative by default unless it is required for correctness.

## Revised Bottom Line

`may_minihttp` is already a capable HTTP/1.1 transport for Microscaler service calls. It now has
bounded strict-path service resolution, request-level observation, cooperative cancellation, and
rotation-safe request metadata, and generation-safe TLS identity/trust rotation. HTTP/2 and
decompression should proceed only after workload evidence. Proxy, cookie, WebSocket,
browser-policy, arbitrary-public-site, and curl-parity features are outside the core remit.
