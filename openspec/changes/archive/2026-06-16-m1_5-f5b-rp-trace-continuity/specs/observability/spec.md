# observability (delta) — m1_5-f5b-rp-trace-continuity

## ADDED Requirements

### Requirement: RelayPoint exports real distributed traces behind the trace seam
With an OTLP collector configured, RP services SHALL export spans whose `trace_id` continues the
request's inbound `traceparent`; with none configured they remain log-only (fail-open). Local nested
spans use a local parent; only cross-process/NATS continuations are remote.

#### Scenario: obs.otlp-exporter-behind-seam
With no OTLP endpoint, `StartSpan` mints a trace context and logs only (no panic, idempotent end);
with an exporter wired, it exports a span — call sites unchanged.

#### Scenario: obs.trace-spans-nats-hops
A `StartSpan` under a trace seeded from an inbound `traceparent` (a `.cmd`/`.log` hop) exports a span
that CONTINUES that `trace_id` and is marked a remote continuation; a locally-nested `StartSpan` uses
the in-process span as a LOCAL (non-remote) parent and shares its `trace_id`.

#### Scenario: obs.sampling-config
The sampler ratio is read from `OTEL_TRACES_SAMPLER_ARG` (default 1.0), parent-based.
