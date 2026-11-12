# Inter-Service Client Candidate Stories

This directory breaks the actionable findings from
[`docs/client-audit.md`](../client-audit.md) into independently reviewable stories. These are
independently tracked backlog items. CA-01 through CA-05 are delivered; the remaining stories are
not commitments in the current delivery.

## Ordering

| Story | Priority | Title | Status | Depends on |
|---|---|---|---|---|
| [CA-01](./stories/CA-01-request-observation.md) | P0 | Request lifecycle observation | Delivered 2026-07-14 | — |
| [CA-02](./stories/CA-02-service-discovery-resolver.md) | P0 | May-aware resolver and service discovery | Delivered 2026-07-14 | — |
| [CA-03](./stories/CA-03-cooperative-cancellation.md) | P1 | Cooperative request cancellation | Delivered 2026-07-14 | CA-02 for cancellable DNS semantics |
| [CA-04](./stories/CA-04-request-metadata-provider.md) | P1 | Request metadata provider | Delivered 2026-07-14 | CA-01 event/redaction model |
| [CA-05](./stories/CA-05-tls-identity-rotation.md) | P1 | TLS identity and trust rotation | Delivered 2026-07-14 | CA-01 observations |
| [CA-06](./stories/CA-06-bounded-decompression.md) | P2 discovery | Bounded response decompression | Measurement probe added | CA-01 measurements |
| [CA-07](./stories/CA-07-http2-feasibility.md) | P2 discovery | Strict-may HTTP/2 feasibility | Evidence plan documented | CA-01 measurements |

CA-07's measurement fields, peer inventory, and go/no-go gate are captured in the
[evidence plan](./CA-07-evidence-plan.md). It remains discovery-only until a deployed-path
workload justifies implementation.

CA-06 measurement results are kept under [evidence](./evidence/).

The initial CA-07 pooled baseline is also recorded under [evidence](./evidence/); it is not a
production go/no-go result.

## Delivery Rule

P0 and P1 stories may enter implementation after API review. P2 discovery stories must first
produce workload evidence and a go/no-go decision; they do not imply implementation.

Retries, circuit breakers, load balancing, bearer/JWT policy, and domain status mapping remain
above the HTTP transport and are therefore not implementation stories in this directory.
