# CA-05: TLS Identity and Trust Rotation

Priority: P1

Status: Delivered 2026-07-14

## Outcome

Long-running services can rotate mTLS identities and trust bundles without process restart or
accidental reuse of connections established under obsolete TLS material.

## Functional Requirements

- [x] Define an injectable TLS configuration provider that returns a configuration snapshot and a
      stable generation/profile identity.
- [x] Pool keys include the TLS generation so connections from different identities never mix.
- [x] A new generation is used for new connections without mutating active rustls sessions.
- [x] Idle connections from retired generations are discarded or expire within a configured bound.
- [x] Rotation supports private CA bundles, client certificates, and client private keys.
- [x] Rotation failure leaves the last known-good generation usable according to explicit policy.
- [x] CA-01 events expose generation changes and TLS setup failures without certificate or key data.

## Non-Functional Requirements

- [x] Private key material is never cloned into logs, errors, or observation events.
- [x] No pool lock is held while loading configuration or performing a TLS handshake.
- [x] Rotation is atomic from the perspective of a new request.
- [x] Existing static `tls_config` callers retain a straightforward migration path.
- [x] rustls with the ring provider remains the only normal TLS implementation.

## Acceptance Criteria

1. Deterministic local-CA tests rotate from client identity A to B and prove new connections use B.
2. A request using generation B never checks out an idle generation-A connection.
3. Invalid replacement material does not destroy the last known-good configuration.
4. Retired idle connections are removed within the documented bound.
5. The normal dependency graph contains no OpenSSL or AWS-LC.

## Out of Scope

- HSTS;
- generic browser certificate pinning;
- certificate issuance or secret-store implementation;
- service authorization decisions based on the peer certificate.

## Delivered Semantics

`TlsConfigProvider` returns an immutable `TlsConfigSnapshot` containing an
`Arc<rustls::ClientConfig>` and a non-zero, monotonically increasing generation. The provider must
retain the generation while its effective identity and trust material are unchanged. Generation
zero is reserved for non-TLS pool keys. Returning an older or equal generation cannot replace the
client's accepted snapshot, which prevents a concurrent stale provider result from rotating the
client backwards.

The client loads an initial known-good snapshot during construction. It then calls the provider
once when each logical request first encounters HTTPS and retains that snapshot across redirect
hops and the stale-connection retry. Provider work occurs before pool checkout and its latency
consumes the total request deadline. Existing `.tls_config(Arc<ClientConfig>)` callers continue to
use one static generation; static configuration and a provider are deliberately mutually
exclusive.

The pool key is `(scheme, host, port, TLS generation)`. Accepting a newer generation atomically
updates the active snapshot and immediately removes every idle connection from retired HTTPS
generations. A request already using an older snapshot may complete on its existing rustls session,
but its lease cannot check that connection back into the pool after rotation. New rustls sessions
receive the new snapshot without mutation of existing sessions.

`TlsConfigFailurePolicy::FailRequest` is the conservative default and returns a redacted typed TLS
error before DNS or connect. `UseLastKnownGood` explicitly permits the request to continue with the
last accepted snapshot. Provider error text, certificates, and private keys are never retained in
errors, `Debug`, or events. `TlsConfigCompleted` reports generation/fallback/error state, while
`TlsGenerationChanged` reports only generation numbers and the count of retired idle connections.
The normal graph contains neither the `openssl`/`openssl-sys` TLS implementation nor AWS-LC;
`rustls-platform-verifier` may retain its existing `openssl-probe` trust-path discovery helper.

## Acceptance Evidence

- `tls_rotation_uses_the_new_mtls_client_identity`
- `tls_generation_rotation_retires_idle_connections`
- `tls_provider_failure_can_use_last_known_good_snapshot`
- `tls_provider_failure_fails_closed_before_connect_by_default`
- `pool_key_separates_scheme_port_and_tls_generation`
