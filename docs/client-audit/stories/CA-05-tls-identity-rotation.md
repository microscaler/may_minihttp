# CA-05: TLS Identity and Trust Rotation

Priority: P1

Status: Proposed

## Outcome

Long-running services can rotate mTLS identities and trust bundles without process restart or
accidental reuse of connections established under obsolete TLS material.

## Functional Requirements

- [ ] Define an injectable TLS configuration provider that returns a configuration snapshot and a
      stable generation/profile identity.
- [ ] Pool keys include the TLS generation so connections from different identities never mix.
- [ ] A new generation is used for new connections without mutating active rustls sessions.
- [ ] Idle connections from retired generations are discarded or expire within a configured bound.
- [ ] Rotation supports private CA bundles, client certificates, and client private keys.
- [ ] Rotation failure leaves the last known-good generation usable according to explicit policy.
- [ ] CA-01 events expose generation changes and TLS setup failures without certificate or key data.

## Non-Functional Requirements

- [ ] Private key material is never cloned into logs, errors, or observation events.
- [ ] No pool lock is held while loading configuration or performing a TLS handshake.
- [ ] Rotation is atomic from the perspective of a new request.
- [ ] Existing static `tls_config` callers retain a straightforward migration path.
- [ ] rustls with the ring provider remains the only normal TLS implementation.

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
