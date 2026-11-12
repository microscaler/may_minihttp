# Epic 01: Core Client

## Overview

Native HTTP/1.1 client for `may_minihttp`: connection management, request/response types, and
`may_http::client`-compatible API. No dependency on the abandoned `may_http` crate.

**Status:** CORE AND RICH CLIENT DONE (compat layer 2026-07-09; hardened rich client 2026-07-14)

**Target Milestone:** Release 0.2.0 (Microscaler fork)

**Dependencies:** None

---

## Stories

| Story | Title | Status |
|-------|-------|--------|
| [01.1](stories/01.1-project-setup.md) | Project Setup & Cargo.toml | DONE |
| [01.2](stories/01.2-error-types.md) | Error Types | DONE (classified wrapper preserves `io::Error`) |
| [01.3](stories/01.3-http-client.md) | HttpClient: Connection & Configuration | DONE |
| [01.4](stories/01.4-request-builder.md) | RequestBuilder: GET/POST with Headers | DONE (rich client) |
| [01.5](stories/01.5-response.md) | Response: Status, Headers, Body, JSON | DONE (buffered and streaming) |
| [01.6](stories/01.6-integration-tests.md) | Integration Tests with Mock Server | DONE |

---

## Definition of Done (Epic-level)

- [x] Native client module under `src/client/`, feature-gated
- [x] `cargo check --features client` passes
- [x] DELETE/PUT/PATCH do not panic in `Request::Drop`
- [x] BRRTRouter migrated off `may_http`
- [x] Integration test against in-process `HttpServer`
- [x] `docs/design-http-client.md` reflects shipped architecture and known gaps
- [x] Server-side code unchanged without `client` feature

---

## Risks

| Risk | Mitigation |
|------|-----------|
| Breaking server API | Client in `src/client/`, feature-gated |
| `http 0.2` vs `1.0` conflict | `http 0.2` only in `client/` module |
| Upstream may_minihttp drift | Microscaler fork branch `integration/microscaler-fork` |

## Capability closure

The detailed register and closure criteria live in
[`docs/design-http-client.md`](../../design-http-client.md#capability-gap-register). JSON, multipart,
redirect policy, bounded host-keyed pooling, shared connect deadlines, streaming, typed error
classification, and operational counters are delivered. A Tokio-native API remains a deliberate
non-goal: the synchronous call surface is backed by coroutine-aware `may::net` I/O.
