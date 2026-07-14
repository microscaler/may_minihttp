# CA-03: Cooperative Request Cancellation

Priority: P1

Status: Proposed

## Outcome

A caller can safely abandon an in-flight request from another coroutine without unsafe coroutine
access, pool-capacity leakage, or connection reuse after partial I/O.

## Functional Requirements

- [ ] `RequestBuilder` accepts an optional cloneable cancellation handle/token.
- [ ] Cancellation is distinguishable from deadline expiry in `ClientErrorKind`.
- [ ] Cancellation is checked before pool wait, DNS, connect, TLS, request write, redirect/retry,
      response buffering, and streaming reads.
- [ ] Blocking waits are woken promptly when cancellation is requested.
- [ ] A cancelled checked-out connection is discarded unless the implementation can prove no bytes
      were exchanged and the response state is clean.
- [ ] Cancellation emits one terminal CA-01 event.
- [ ] Dropping a cancellation handle without cancelling has no effect.

## Non-Functional Requirements

- [ ] The public API contains no `unsafe` requirement.
- [ ] Cancellation performs no response-body drain and no network I/O from `Drop`.
- [ ] Races between completion, timeout, and cancellation have one deterministic terminal outcome.
- [ ] Cancellation cannot return an incomplete connection to the pool.
- [ ] Existing callers without a token retain current behavior and cost.

## Acceptance Criteria

1. Deterministic tests cancel during pool wait, connect, response wait, buffered body read, and
   streaming body read.
2. Pool capacity is immediately available after cancellation.
3. A cancellation/completion race produces exactly one terminal event and no panic.
4. A subsequent request never consumes bytes from the cancelled exchange.
5. Request deadlines remain authoritative when no token is supplied.

## Dependencies

CA-02 should define whether and how resolver waits are interruptible. Phase-boundary cancellation
may ship first only if its limitations are explicit.

## Out of Scope

- hedged-request policy;
- retrying a cancelled operation;
- cancelling arbitrary user-provided blocking readers that do not cooperate.
