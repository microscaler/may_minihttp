# CA-07: Strict-May HTTP/2 Feasibility

Priority: P2 discovery

Status: Proposed discovery

## Outcome

Determine whether HTTP/2 multiplexing provides enough benefit for Microscaler east-west traffic to
justify a may-native implementation and its protocol complexity.

## Discovery Requirements

- [ ] Measure connection counts, queue time, throughput, tail latency, and CPU for representative
      high-concurrency single-origin workloads on the existing HTTP/1.1 pool.
- [ ] Confirm which deployed peers, ingress components, and service meshes negotiate HTTP/2.
- [ ] Evaluate may-compatible HTTP/2 implementations without introducing Tokio or Hyper.
- [ ] Define ALPN, TLS, flow-control, stream cancellation, connection draining, and pool semantics.
- [ ] Define bounded limits for concurrent streams, frame sizes, header tables, queued requests, and
      per-connection memory.
- [ ] Compare operational complexity with increasing the bounded HTTP/1.1 per-origin pool.
- [ ] Produce a go/no-go architecture decision before implementation.

## Go Criteria

Implementation proceeds only if all are true:

1. a representative workload shows a material tail-latency, connection-count, or throughput gain;
2. production peers actually support the negotiated protocol;
3. a strict-may implementation path exists without Tokio, Hyper, reqwest, or AWS-LC;
4. cancellation, deadlines, backpressure, and bounded-memory behavior can be tested deterministically;
5. HTTP/1.1 fallback remains available and secure.

## Non-Goals

- HTTP/2 server push;
- browser parity;
- implementing HTTP/2 solely to match reqwest feature lists;
- weakening current framing, timeout, or pool-safety guarantees.
