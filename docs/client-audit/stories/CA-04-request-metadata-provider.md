# CA-04: Request Metadata Provider

Priority: P1

Status: Proposed

## Outcome

Services consistently attach rotating credentials, correlation identifiers, and trace context
without embedding JWT, OAuth, or OpenTelemetry policy in the HTTP transport.

## Functional Requirements

- [ ] `ClientBuilder` accepts optional default headers and/or an `Arc<dyn RequestMetadataProvider>`.
- [ ] The provider receives sanitized method/origin context and returns headers for one attempt.
- [ ] Request-specific headers have a documented precedence over defaults.
- [ ] `Host`, `Content-Length`, and `Transfer-Encoding` remain transport-owned and cannot be
      overridden by a provider.
- [ ] Provider-generated sensitive headers join the cross-origin redirect stripping set.
- [ ] Metadata is refreshed for a new logical request; retry and redirect refresh behavior is
      explicit and tested.
- [ ] Provider failures map to a stable error classification before request bytes are written.
- [ ] CA-01 events report provider success/failure without reporting header values.

## Non-Functional Requirements

- [ ] No bearer-token, JWT, JWE, OAuth, or vendor tracing dependency enters the core crate.
- [ ] Provider callbacks run without pool or transport locks held.
- [ ] Secret header values are never included in debug output or observer events.
- [ ] Header count and aggregate encoded size remain subject to explicit limits.

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
