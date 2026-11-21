# CA-07 evidence plan: pooled HTTP/1.1 baseline and HTTP/2 decision

This document records the evidence required before adding HTTP/2 to the strict may client. It is
an experiment plan, not an implementation commitment.

## Baseline to capture

Run the pooled `client::Client` against a local or representative service endpoint. The
`client_pool_audit` example provides a repeatable baseline; capture one result per concurrency
level and payload shape:

```text
cargo run --example client_pool_audit --features client -- \
  http://127.0.0.1:8080/health 16 100 8
```

| Field | Meaning |
|---|---|
| concurrency | Number of may request coroutines |
| requests | Completed requests in the measurement window |
| errors | Requests that did not complete successfully |
| p50/p95/p99 (µs) | End-to-end request latency percentiles (divide by 1,000 for milliseconds) |
| connections_created | New transport connections reported by `ClientStats` |
| connections_reused | Keep-alive checkouts reported by `ClientStats` |
| pool_waits / pool_wait_time | Queue pressure from the per-origin bound |
| response_bytes | Buffered response payload bytes observed by the probe; capture wire bytes separately when comparing encodings |
| cpu | Process CPU time, or the platform-specific proxy used when CPU time is unavailable |

The endpoint, payload, duration, runtime configuration, and client limits must be recorded with
the result. A short warm-up must be excluded from percentile calculations. Results are not
comparable across machines unless the machine and runtime details are recorded.

## Peer and deployment inventory

For every deployed east-west peer and ingress hop, record:

- whether TLS ALPN advertises `h2`;
- whether clear-text HTTP/2 (h2c) is supported or intentionally forbidden;
- negotiated protocol in the deployed path, including service-mesh sidecars;
- maximum concurrent streams and connection-draining behavior, if an HTTP/2 endpoint exists;
- whether HTTP/1.1 fallback is guaranteed during rollout and failure.

Do not infer support from a public endpoint or a browser. The inventory must cover the actual
Microscaler service path.

## Go/no-go review

HTTP/2 may enter implementation only when the baseline demonstrates a material tail-latency,
connection-count, or throughput gain at a concurrency/payload shape that occurs in production,
and the peer inventory confirms that the protocol is available where the gain matters. The review
must also identify a may-compatible implementation and bounded semantics for streams, frames,
header tables, flow control, cancellation, deadlines, draining, and fallback.

If increasing the bounded HTTP/1.1 per-origin pool provides the same benefit with less operational
risk, retain HTTP/1.1 and close CA-07 as no-go.
