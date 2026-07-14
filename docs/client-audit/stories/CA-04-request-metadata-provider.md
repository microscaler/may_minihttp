# CA-04: Request Metadata Provider

Priority: P1

Status: Delivered 2026-07-14

## Outcome

Services consistently attach rotating credentials, correlation identifiers, and trace context
without embedding JWT, OAuth, or OpenTelemetry policy in the HTTP transport.

## Functional Requirements

- [x] `ClientBuilder` accepts optional default headers and/or an `Arc<dyn RequestMetadataProvider>`.
- [x] The provider receives sanitized method/origin context and returns headers for one attempt.
- [x] Request-specific headers have a documented precedence over defaults.
- [x] `Host`, `Content-Length`, and `Transfer-Encoding` remain transport-owned and cannot be
      overridden by a provider.
- [x] Provider-generated sensitive headers join the cross-origin redirect stripping set.
- [x] Metadata is refreshed for a new logical request; retry and redirect refresh behavior is
      explicit and tested.
- [x] Provider failures map to a stable error classification before request bytes are written.
- [x] CA-01 events report provider success/failure without reporting header values.

## Non-Functional Requirements

- [x] No bearer-token, JWT, JWE, OAuth, or vendor tracing dependency enters the core crate.
- [x] Provider callbacks run without pool or transport locks held.
- [x] Secret header values are never included in debug output or observer events.
- [x] Header count and aggregate encoded size remain subject to explicit limits.

## Acceptance Criteria

1. Tests cover default headers, request override, forbidden framing headers, and provider failure.
2. Cross-origin redirects strip built-in and provider-declared credentials.
3. A rotating fake provider proves that a later logical request receives fresh credentials.
4. No provider callback occurs after request head bytes have been written for that attempt.
5. BRRTRouter or Sesame can implement trace and service-token injection without modifying
   `may_minihttp` internals.

## Out of Scope

- acquiring or refreshing a token;
- choosing JWT algorithms, claims, audience, or scopes;
- distributed-tracing sampling and export;
- application retry policy.

## Delivered Semantics

Header precedence is `request-specific > provider > client defaults`. Precedence replaces all
values for a matching name; unrelated multi-value headers are preserved. `Host`, `Content-Length`,
and `Transfer-Encoding` are rejected from all three inputs because the URL and body encoder own
them.

The provider runs immediately before every intended wire attempt: the initial attempt, each
redirect hop, and the single safe stale-connection retry. `attempt` starts at one for each logical
request and increases across those paths. `redirect_hop` and `stale_retry` let a provider choose a
fresh trace value or signature without exposing the URL path, query, body, or existing headers.
The callback runs before pool checkout, so it holds neither pool capacity nor a transport lock.
Its latency consumes the existing total request deadline. Implementations own their blocking,
latency, and panic policy.

Built-in credential headers, client-configured sensitive names, and provider-declared sensitive
names form one set for the logical request. After a cross-origin redirect, that complete set stays
suppressed for all remaining hops. A caller that deliberately needs credentials for the target
origin must start a new logical request rather than carrying ambient authority through a redirect.

Provider-returned errors and invalid provider metadata are redacted to
`ClientErrorKind::Metadata`; their messages and header values are not retained. The
`RequestMetadataCompleted` event reports only the sanitized origin, counters, duration, and stable
error classification. Provider and merged headers are bounded by configurable field-count and
encoded-size limits (defaults: 64 fields and 16 KiB).

## Acceptance Evidence

- `request_metadata_precedence_and_rotation_are_deterministic`
- `transport_owned_and_bounded_request_headers_are_enforced`
- `metadata_provider_failure_is_redacted_classified_and_pre_connect`
- `stale_connection_retry_refreshes_attempt_metadata`
- `cross_origin_redirect_strips_credentials`
