# CA-03: Cooperative Request Cancellation

Priority: P1

Status: Delivered 2026-07-14

## Outcome

A caller can safely abandon an in-flight request from another coroutine without unsafe coroutine
access, pool-capacity leakage, or connection reuse after partial I/O.

## Functional Requirements

- [x] `RequestBuilder` accepts an optional cloneable cancellation handle/token.
- [x] Cancellation is distinguishable from deadline expiry in `ClientErrorKind`.
- [x] Cancellation races the complete request lifecycle, covering pool wait, resolution, connect,
      TLS, request write, redirect/retry,
      response buffering, and streaming reads.
- [x] May-aware blocking waits and I/O are woken promptly when cancellation is requested.
- [x] A cancelled checked-out connection is discarded unless the implementation can prove no bytes
      were exchanged and the response state is clean.
- [x] Cancellation emits one terminal CA-01 event after unwind cleanup.
- [x] Dropping a cancellation handle without cancelling has no effect.

## Non-Functional Requirements

- [x] The public API contains no `unsafe` requirement.
- [x] Cancellation performs no response-body drain and no network I/O from `Drop`.
- [x] Races between completion, timeout, and cancellation have one deterministic terminal outcome.
- [x] Cancellation cannot return an incomplete connection to the pool.
- [x] Existing callers without a token retain their direct, allocation-free execution path.

## Acceptance Criteria

1. Deterministic tests cancel during pool wait, connect, response wait, buffered body read, and
   streaming body read.
2. Pool capacity is immediately available after cancellation.
3. A cancellation/completion race produces exactly one terminal event and no panic.
4. A subsequent request never consumes bytes from the cancelled exchange.
5. Request deadlines remain authoritative when no token is supplied.

## Dependencies

CA-02 provides deadline-aware may condition-variable cache waits and the no-network-wait
`ServiceResolver`. The whole request runs in the cancellable child, so may-aware resolver waits are
interrupted with the other phases.

## Out of Scope

- hedged-request policy;
- retrying a cancelled operation;
- cancelling arbitrary user-provided blocking readers that do not cooperate.

## Architecture

Token-bearing requests use may's scoped completion queue to race the request coroutine against the
token's may condition-variable wait. When cancellation wins, the scope cancels and joins the request
child first. RAII releases pool accounting and drops incomplete transports without observer
callbacks from unwind `Drop`; the parent then emits exactly one `RequestCancelled` event and returns
an `Interrupted` `io::Error`, classified as `ClientErrorKind::Cancelled`.

Requests without a token do not create the completion-queue coroutines. `SystemResolver` and an
arbitrary request-body reader remain explicit blocking boundaries if their implementation does not
cooperate with may cancellation.

## Delivery Evidence

- token state, clone sharing, idempotence, waiter wakeup, and drop-without-cancel tests;
- deterministic cancellation during may-aware resolution, an injected connect phase, pool wait,
  buffered response wait, and streaming body read;
- cancelled checked-out connections release capacity and cannot feed stale bytes to a subsequent
  request;
- completion/cancellation races repeatedly produce one terminal event and no panic;
- no-token deadline and pool tests continue to pass unchanged.
