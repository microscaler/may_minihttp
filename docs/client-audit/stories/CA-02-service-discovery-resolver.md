# CA-02: May-Aware Resolver and Service Discovery

Priority: P0

Status: Delivered 2026-07-14

## Outcome

Internal service names resolve without hidden scheduler blocking, and address changes are honoured
within bounded, observable time.

## Functional Requirements

- [x] Provide a push-updated `ServiceResolver` whose request path performs no network lookup.
- [x] Cache positive results for a configurable bounded TTL.
- [x] Cache negative results for a shorter configurable TTL.
- [x] Bound cache entries and purge expired entries deterministically.
- [x] Rotate the first attempted address so one unhealthy endpoint does not receive every attempt.
- [x] Refresh an expired or explicitly invalidated service entry before connection.
- [x] Permit consumers to invalidate a service name after deployment or discovery events.
- [x] Preserve SNI and HTTP `Host` from the logical URL when connecting to a resolved address.
- [x] Resolution, cache waits, all address attempts, and TLS share the existing connect and total
      request deadlines.
- [x] Emit CA-01 observation events for resolution source, duration, address count, and
      refresh outcome without logging sensitive query data.

## Non-Functional Requirements

- [x] No hidden OS-thread-per-resolution adapter.
- [x] No resolver or cache lock is held during wrapped resolution or TCP connect.
- [x] Cache memory, TTL, in-flight names, and address count per record are bounded.
- [x] IPv4 and IPv6 `SocketAddr` results are supported; round-robin first-address rotation is
      documented.
- [x] Existing injected `Resolver` implementations remain source compatible through default trait
      methods.

## Acceptance Criteria

1. An injectable fake clock proves positive TTL, negative TTL, expiry, and invalidation behavior.
2. Concurrent coroutines resolving one cold name do not produce an unbounded lookup stampede.
3. A local test rotates across two addresses and succeeds when the first endpoint is unavailable.
4. A slow resolution consumes the connect deadline and returns `ClientErrorKind::Timeout`.
5. Host and TLS identity tests prove that connecting by resolved IP does not replace logical SNI or
   the `Host` header.
6. Scheduler-worker tests show no blocking system DNS call on the strict resolver path.

## Out of Scope

- a global load balancer;
- endpoint health scoring or circuit breaking;
- Kubernetes-specific policy in the core crate;
- exponential backoff inside one request.

## Verification

- deterministic local DNS/resolver tests with no public network dependency
- `cargo check --no-default-features --features client`
- `cargo clippy --lib --features json -- -D warnings`

## Delivery Evidence

- injected-instant tests cover positive/negative expiry, cache hits, and invalidation;
- concurrent may coroutines prove one cold lookup and deadline-bounded coalesced waiting;
- a local service test falls through an unavailable first address to the live endpoint;
- an in-process rustls test proves a registry IP does not replace logical SNI or `Host`;
- `ServiceResolver` tests cover update, replacement, removal, entry/address bounds, canonical host
  keys, and address rotation without public DNS.
