# CA-06: Bounded Response Decompression

Priority: P2 discovery

Status: Proposed discovery (measurement probe added)

## Outcome

Determine whether opt-in response decompression materially improves representative service traffic
and, only if justified, define a safe bounded implementation.

## Discovery Requirements

- [ ] Measure payload sizes, compressibility, bandwidth, CPU, and latency for representative JSON
      and bulk responses in Sesame-IDAM, BRRTRouter, and Hauliage.
- [ ] Compare no compression with candidate encodings supported by actual service peers.
- [ ] Specify whether limits apply to compressed bytes, decompressed bytes, and expansion ratio.
- [ ] Define behavior for unsupported, repeated, or malformed `Content-Encoding` values.
- [ ] Evaluate buffered and streaming decompression without blocking the may scheduler.
- [ ] Produce a go/no-go decision and dependency review before implementation.

The deterministic probe at [`../../../examples/compression_audit.rs`](../../../examples/compression_audit.rs)
reports plain and gzip wire sizes plus p50/p95 encode/decode wall-clock samples. Run it with the
default service-shaped fixtures, then repeat with captured Sesame-IDAM, BRRTRouter, and Hauliage
payloads supplied as positional file arguments. These results are evidence only; the client does
not negotiate or decode compression until the go/no-go gate is approved.

The first Hauliage fixture run is recorded in
[`../evidence/CA-06-2026-07-14.md`](../evidence/CA-06-2026-07-14.md).

## Implementation Acceptance Criteria if Approved

1. Decompression is opt-in and sends an explicit `Accept-Encoding` value.
2. Decompressed bytes are bounded independently of wire bytes and cannot bypass
   `max_response_body`.
3. Streaming decompression retains deadline and early-drop pool semantics.
4. Truncated streams, checksum failures, expansion-limit breaches, and unsupported encodings return
   typed failures and discard the connection when required.
5. Compression libraries add no Tokio, Hyper, reqwest, OpenSSL, or AWS-LC dependency.
6. Benchmarks demonstrate a material benefit for an approved Microscaler workload.

## Non-Goals

- transparent decoding of every format supported by browsers;
- enabling compression by default without workload evidence;
- compressing secrets merely to imitate general-purpose web clients.
