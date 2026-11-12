# CA-01: Request Lifecycle Observation

Priority: P0

Status: Delivered 2026-07-14

## Outcome

Consumers can measure and trace each service request without coupling `may_minihttp` to an
OpenTelemetry implementation or exposing sensitive request data.

## Functional Requirements

- [x] `ClientBuilder` accepts an optional `Arc<dyn ClientObserver>`.
- [x] Events cover request start, pool wait, DNS, connection/TLS establishment, connection reuse,
      connection discard, response headers, redirect, stale retry, terminal success, abandonment,
      and terminal failure.
- [x] Terminal events correlate to a start event carrying method and sanitized origin, and contain
      status or `ClientErrorKind`, total duration,
      and phase durations where available.
- [x] Events identify whether a connection was created, reused, or discarded.
- [x] Observer callbacks are invoked after releasing pool and transport locks.
- [x] The absent-observer path performs no observer allocation and remains the default.
- [x] Aggregate `ClientStats` remains available and backwards compatible.

## Non-Functional Requirements

- [x] Core features do not depend on OpenTelemetry, tracing, Tokio, or a metrics exporter.
- [x] Default events exclude bodies, authorization values, cookies, full query strings, and raw
      certificate material.
- [x] Observer code cannot change request control flow; callbacks return no transport result.
- [x] Callback panics or latency are documented as caller responsibility.
- [x] Event variants are `#[non_exhaustive]` and fields are suitable for BRRTRouter and Sesame
      adapters.

## Acceptance Criteria

1. Local deterministic tests verify event ordering for new, reused, redirected, retried, failed,
   abandoned, buffered, and streaming requests.
2. No callback runs while the pool mutex or shared transport mutex is held.
3. A redaction test proves secrets and bodies are absent from all built-in event structures.
4. The no-observer path passes the existing regression suite. This repository has no dedicated
   HTTP client performance benchmark, so no numeric throughput claim is made.
5. Existing `Client::stats()` callers compile unchanged.

## Out of Scope

- exporting spans or metrics;
- choosing trace sampling policy;
- propagating vendor-specific headers;
- logging full URLs or payloads.

## Cancellation Boundary

Direct may coroutine cancellation is delivered as an unwind. Invoking an arbitrary observer from a
cancellation `Drop` can re-enter may's cancellation machinery and trigger another panic, so that
legacy path emits no callback. CA-03's explicit `CancellationToken` emits `RequestCancelled` from
the parent only after scoped unwind cleanup is complete.

## Verification

- `cargo test --lib --features json`
- `cargo test --test client_integration --features json`
- `cargo clippy --lib --features json -- -D warnings`
- normal dependency graph contains no Tokio, Hyper, reqwest, or AWS-LC

## Delivery Evidence

- deterministic unit tests cover new/reused connections, pool waiting, redirect, stale retry,
  failure, and partial/full streaming outcomes;
- the existing coroutine-cancellation test proves pool capacity is released without invoking
  observer code from unwind cleanup;
- redaction tests assert that a secret query and authorization value never enter recorded events;
- discard and terminal callbacks run only after releasing pool and transport state;
- existing `Client::stats()` tests and API callers remain unchanged.
