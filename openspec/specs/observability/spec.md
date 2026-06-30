# Observability Specification

## Purpose

The health/readiness surface on every RP binary plus the wired OTLP export + metrics surface that
make the realtime plane's failure modes (DLQ growth, lease flaps, roster 5xx, fan-out lag) probeable
and alertable rather than log-only. Builds on the trace seam from `m1_5-f5b-rp-trace-continuity`
(`obs.InitTracer` is a no-op unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set). Materialized from the
`rp-realtime-hardening` change (RH-06, RH-09).

## Requirements

### Requirement: Every RP binary exposes a health/readiness surface

`cmd/{router,projector,authcallout}` MUST each expose a small HTTP health listener (e.g.
`:8222/healthz`) reporting **liveness** (the process is up, NATS connected, JetStream reachable). The
projector MUST additionally report **readiness** = lease-held: readiness MUST FAIL when the worker is
wedged or has lost/paused its lease, so a non-active or stalled projector is taken out of rotation.
The health surface MUST read its signals from the owned ports (no concrete NATS imported into the
core).

#### Scenario: Liveness reports NATS and JetStream reachability
- **id:** `obs.health.liveness-nats-js`
- **GIVEN** an RP binary (router, projector, or auth-callout) running its health listener
- **WHEN** a liveness probe hits `/healthz`
- **THEN** it reports healthy only while the process is up with NATS connected and JetStream reachable, and unhealthy otherwise

#### Scenario: Projector readiness reflects whether it holds the lease
- **id:** `obs.health.readiness-reflects-lease`
- **GIVEN** the projector with its health listener
- **WHEN** a readiness probe hits it while the projector is a non-holding standby, has lost its lease, or is paused on an overdue renew
- **THEN** readiness FAILS (the wedged/non-active worker is taken out of rotation), and SUCCEEDS only while it actively holds the lease and is processing

### Requirement: OTLP export is wired in deployment so the desk→RP trace waterfall continues

The three RP Deployments MUST set `OTEL_EXPORTER_OTLP_ENDPOINT` (+ `_INSECURE`, sampler args) and
`OBS_ENV`, mirroring desk (which exports to `alloy.infra:4317`), so a span started under an inbound
`traceparent` (a `.cmd`/`.log` hop) actually EXPORTS and continues the desk-originated trace across
the RP hop, and RP logs carry the real environment label instead of a default `env=dev`. With no
endpoint configured the services remain log-only (fail-open), unchanged.

#### Scenario: With the OTLP endpoint injected, RP exports continuing spans
- **id:** `obs.otlp.export-wired`
- **GIVEN** the RP Deployments with `OTEL_EXPORTER_OTLP_ENDPOINT`/`_INSECURE`/sampler + `OBS_ENV` set (mirroring desk)
- **WHEN** the router/projector handle a fact seeded by an inbound `traceparent`
- **THEN** they export spans that CONTINUE that `trace_id` to the collector and label logs with the real `OBS_ENV` — closing the desk→RP waterfall break (and remain log-only when no endpoint is set)

### Requirement: RP emits metrics for the realtime plane's failure modes

The RP binaries MUST expose a metrics surface (an OTel meter or `promhttp` handler on the health
port) instrumenting at least: DLQ routes, `Nak`s, publish retries, lease-renew retries, roster
errors, and fan-out latency. These signals MUST be scrapeable/alertable — not visible only as log
lines — so DLQ growth, lease flaps, roster 5xx, and fan-out lag can drive alerts.

#### Scenario: Realtime failure-mode counters and latencies are exposed
- **id:** `obs.metrics.surface`
- **GIVEN** an RP binary with its metrics surface on the health port
- **WHEN** a scrape reads it
- **THEN** it exposes counters for DLQ routes, Naks, publish retries, lease-renew retries, and roster errors, plus a fan-out latency metric — each alertable, not log-only
