# Strict may client architecture

## Status

Implemented direction, 2026-07-14. HTTPS, replay-aware requests, buffered and streaming responses,
redirects, bounded pooling, and cancellation-safe leases follow this design without an async
runtime or hidden blocking worker pool.

## Runtime invariants

1. Network I/O uses `may::net::TcpStream`; TLS wraps that socket with rustls.
2. Waiting between coroutines uses `may::sync::{Mutex, Condvar, Semphore}` or may channels.
3. Timers and deadlines use may's I/O timeout and timer facilities.
4. No Tokio executor, async trait, `reqwest`, or hidden OS-thread-per-request adapter enters the
   client feature graph.
5. A pool lock is held only while inspecting or mutating pool metadata. DNS, connect, TLS, request
   writes, and response reads occur after releasing it.
6. Responses can stream. A connection is reusable only after its framing boundary is fully consumed;
   partial bodies are discarded without performing I/O from `Drop`.
7. System DNS is an explicit possible blocking boundary. Strict deployments inject a
   push-updated `ServiceResolver`, or a `CachingResolver` around an application-owned may-aware
   resolver. Cache waiters use a may condition variable and honour the connect deadline.
8. Observer callbacks run synchronously after releasing pool and transport locks. Built-in events
   contain origins and operational outcomes, never paths, queries, headers, or bodies.

The `client` feature explicitly enables `may/io_timeout`; it must compile with the crate's default
features disabled.

## Layering

### Connection layer

The existing `HttpClient` remains the single-connection HTTP/1.1 primitive during the 0.1 line. It
owns one plain or TLS transport and preserves response streaming and connection reuse. The new
cloneable `Client` is the policy-and-pool handle without forcing the breaking 0.2 rename early.

For the breaking 0.2 API, `HttpClient` can be renamed to internal `HttpConnection` and `Client` can
take the primary name. This avoids making a single HTTP/1.1 connection look concurrently
multiplexable.

### Request layer

`RequestBuilder` owns replayable request metadata and a body enum:

- empty;
- immutable bytes;
- optional JSON serialized to immutable bytes;
- multipart text/byte parts with a known encoded length;
- a single-use streaming reader explicitly marked non-replayable.

Redirect and stale-connection retry logic may replay only bodies marked replayable. A streaming body
must fail with a typed `BodyNotReplayable` result before a second network attempt.

### Pool layer

The pool stores idle transports, not live response objects and not concurrently shared
`HttpConnection` handles. Its key is:

```text
(scheme, canonical host, effective port, TLS profile identity)
```

The state is protected by `may::sync::Mutex`; capacity waiters use a may condition variable or
semaphore with the request deadline. Limits are required globally and per origin. Idle eviction is
lazy on checkout/check-in, so no background reaper thread is necessary.

Checkout reserves capacity under the lock, releases the lock, and only then connects. Check-in is
allowed when:

- the response body reached its framing boundary;
- neither side requested `Connection: close`;
- the HTTP version permits persistence;
- no read, write, parse, or TLS error marked the transport unhealthy;
- the idle and lifetime limits have not expired.

Stale idle sockets may be replaced once for idempotent, replayable requests. They must never cause
an automatic retry of a non-idempotent request after bytes may have reached the peer.

### Resolution layer

`SystemResolver` remains the compatibility default and may block in the operating-system resolver.
`CachingResolver` bounds positive and negative TTLs, entries, and addresses; coalesces one cold
lookup; rotates address order; and supports explicit invalidation. Its scheduler safety is inherited
from the wrapped resolver. `ServiceResolver` is the strict request path: discovery code pushes
bounded address sets into it, and requests perform no DNS network I/O. Both preserve the logical URL
host for the HTTP `Host` header and rustls server name while connecting to the selected socket
address.

### Observation layer

An optional `ClientObserver` receives request start, pool wait, resolution, connect/reuse, response,
redirect/retry, and terminal outcome events. Early abandonment of a streaming response is reported.
May cancellation currently unwinds through `Drop`, where calling arbitrary consumer code can
re-enter may's cancellation machinery; a cancellation event is therefore deferred to the explicit
cooperative mechanism in CA-03. Event origins contain only scheme, host, and effective port.
Callback latency and panic policy belong to the consumer; callbacks cannot alter request control
flow.

## Redirect policy

Redirect following is disabled by default. The opt-in policy contains a maximum hop count and an
origin rule. It resolves relative `Location` values, detects loops, and can be restricted to
same-origin targets. Status-specific method and body rules remain mandatory.

If cross-origin redirects are enabled, `Authorization`, `Cookie`, `Proxy-Authorization`, and caller-
configured sensitive headers are stripped before the redirected request is sent. HTTPS-to-HTTP
downgrades are rejected unless a separate explicit policy permits them. Status handling follows:

- 303: change to GET and discard the body;
- 307/308: preserve method and body only when replayable;
- 301/302: preserve GET/HEAD; other methods require explicit compatibility policy.

## Body helpers

JSON is optional through the `json` feature. The compatibility API retains `io::Error`; callers that
need stable categories use `ClientError`/`ClientErrorKind` without losing the source error.
Multipart text and byte parts compute exact `Content-Length` and write directly into the request
body without creating a second encoded body. Multipart metadata is validated before output to
prevent CR/LF header injection.

File multipart support does not hide blocking filesystem reads in the request coroutine.
`MultipartForm::blocking_file` and `blocking_reader` are explicit, bounded preload boundaries that
produce replayable bytes; callers invoke them before latency-sensitive coroutine work. Direct
request readers are single-use and never retried.

## Acceptance criteria

### JSON and multipart

- `cargo check --no-default-features --features client` passes.
- `cargo test --features json` covers JSON headers, serialization, deserialization, and an in-process
  request round trip.
- Multipart length equals bytes written, metadata injection is rejected before output, and an
  in-process server receives text and byte parts.

### Redirects

- Disabled by default and bounded when enabled.
- Relative, same-origin, loop, hop-limit, downgrade, and cross-origin credential tests are local and
  deterministic.
- Non-replayable bodies are never resent.

### Pooling

- Bounds are enforced under concurrent may coroutines without an OS-thread wait.
- The pool key separates HTTP, HTTPS, ports, and TLS profiles.
- Locks are demonstrably not held during network I/O.
- Fully consumed persistent responses reuse a connection; close/error/incomplete responses do not.
- Idle and lifetime expiry are deterministic under an injectable clock in unit tests.
- Coroutine cancellation and partial streaming-response drop release capacity without drain I/O.
- A stale idle socket is retried once only for idempotent requests with replayable bodies.

### Dependency boundary

The normal `client` and `json` feature graphs contain no Tokio, reqwest, hyper, or AWS-LC packages.

### Resolution and observation

- Positive/negative cache expiry and invalidation are deterministic under an injected instant.
- Concurrent cold cache lookups are coalesced and a waiting coroutine honours its connect deadline.
- Push-updated entries are bounded, replaceable, removable, and rotate their first address.
- The logical `Host` is preserved when connecting to a registry-provided address.
- New, reused, redirected, retried, failed, abandoned, buffered, and streaming request
  lifecycles have deterministic observer tests with sanitized event payloads.
