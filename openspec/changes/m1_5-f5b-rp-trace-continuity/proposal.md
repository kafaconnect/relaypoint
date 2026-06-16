# Change: m1_5-f5b-rp-trace-continuity

Milestone **M1.5 (production foundation)**, story **F5 — observability collection (RelayPoint side)**,
the companion to desk's `m1_5-f5a-observability-collection`. Wire the OTLP trace exporter behind RP's
`obs.StartSpan` seam so RP services export real spans, and (follow-up task) carry the W3C `traceparent`
across RP's internal `.log`/agent-feed hops so a desk request is ONE trace end-to-end.

This is an **active proposal** on the relaypoint repo (the cross-repo half of F5, per the ADR-0011
two-`obs`-copies-in-lockstep + the shared-infra process rule).

## Charter
Advances operability (queryable distributed traces); ADR-0011 governs the shared `obs` contract +
field schema. `go.opentelemetry.io/*` is Apache-2.0 (OSS).

## Review tier
**T2 (core)** — telemetry is fail-open, never on the critical path.

## From (the gap)
RP's `internal/obs` carried the foundation (canonical logs, traceparent parse/inject, the StartSpan
seam) but no OTLP exporter — `StartSpan` only logged. The router already extracts the inbound
`traceparent` from `.cmd`, but the `.log` fact (`signaling.LogStore.Append`) carries no header, so the
projector + agent-feed start fresh traces (the cross-hop gap).

## To
- **OTLP exporter behind the seam** (`internal/obs/otel.go`, the twin of desk's): `InitTracer` (no-op
  when `OTEL_EXPORTER_OTLP_ENDPOINT` unset, fail-open) + `StartSpan` mints a real span that continues
  the inbound `trace_id` (local nesting stays local, only cross-process/NATS is remote). Wired in
  `cmd/{router,projector,authcallout}`.
- **(remaining task)** thread `ctx`+`traceparent` through `signaling.Append` → the `.log` publish, and
  the projector `Deliver` (extract) → `FeedSink.Publish` (inject), closing the `.log`/feed continuity.

## Impact
- `internal/obs/otel.go` + `trace.go` (StartSpan branch); `go.mod`; `cmd/{router,projector,authcallout}`
  InitTracer. **Remaining (V2-01 todo):** the `.log`/feed `traceparent` threading (a signature change on
  `LogStore.Append` + projector) — a focused follow-up.
