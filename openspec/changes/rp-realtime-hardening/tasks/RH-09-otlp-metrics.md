---
id: RH-09
slice: RH
title: MED — wire OTLP export in deploy plus a metrics surface
status: todo
specs: [obs.otlp.export-wired, obs.metrics.surface]
---

## Goal
OTLP export is dormant in deploy: every RP main calls `obs.InitTracer` but it is a no-op unless
`OTEL_EXPORTER_OTLP_ENDPOINT` is set (otel.go:30), and the RP Deployments set no `OTEL_*`/`OBS_ENV`
(desk exports to `alloy.infra:4317`) → the desk→RP trace waterfall breaks at the RP hop and all RP
logs are labelled `env=dev`. And there are NO metrics anywhere in RP (`internal/obs` is logging+
tracing only) → DLQ growth, lease flaps, roster 5xx, fan-out lag are only log lines, not alertable.

## Success criteria (test-first)
- A test that a span under an inbound `traceparent` exports a CONTINUING `trace_id` when an endpoint
  is configured (and stays log-only when unset); a test that the metrics surface exposes the named
  counters/latency.
- Inject `OTEL_EXPORTER_OTLP_ENDPOINT`/`_INSECURE`/sampler + `OBS_ENV` into the 3 RP Deployments
  (mirror desk).
- Add a metrics handler (OTel meter or `promhttp`) on the health port instrumenting DLQ routes,
  Naks, publish-retries, lease-renew-retries, roster errors, and fan-out latency.

## Files
- `internal/obs/otel.go` + a new `internal/obs/metrics.go` (meter/handler)
- `internal/projector/projector.go`, `internal/signaling/router.go` (emit the counters/latency)
- `cmd/{router,projector,authcallout}/main.go` (mount the metrics handler on the health port)
- CROSS-REPO follow-up (desk): the 3 RP Deployments' `OTEL_*`/`OBS_ENV` env — tracked, not edited here.

## Spec
`// @spec:obs.otlp.export-wired`, `// @spec:obs.metrics.surface`

## Log
- todo
