---
id: RH-09
slice: RH
title: MED — wire OTLP export in deploy plus a metrics surface
status: done
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
- DONE. OTLP export was already code-wired (`obs.InitTracer` builds the otlptracegrpc exporter +
  parent-based sampler when `OTEL_EXPORTER_OTLP_ENDPOINT` is set; `startOTelSpan` seeds a REMOTE parent
  from the inbound traceparent so the trace_id continues). This task LOCKS that behaviour with a spec
  test and adds the metrics surface.
- Metric library: `github.com/prometheus/client_golang` (promhttp) — chosen over the OTel meter because
  the OTel→Prometheus exporter bridge is not in the module cache (offline) whereas client_golang is;
  promhttp mounts a `/metrics` handler directly. New file `internal/obs/metrics.go` owns a private
  registry + `MetricsHandler()`.
- Named series (own registry, low-cardinality, no per-tenant labels):
  `relaypoint_projector_dlq_routes_total`, `relaypoint_projector_naks_total`,
  `relaypoint_publish_retries_total` (shared: projector feed-publish retries + router OCC log-append
  re-folds), `relaypoint_projector_lease_renew_retries_total`, `relaypoint_projector_roster_errors_total`,
  `relaypoint_projector_fanout_latency_seconds` (histogram).
- Mounted on the EXISTING RH-06 health listener (`:8222/metrics`) — `health.Handler`/`health.Serve` took
  an optional `http.Handler` (nil = probes only, keeping health decoupled from obs); the 3
  `cmd/*/main.go` pass `obs.MetricsHandler()`. No second port, no new required env.
- Emission call sites: projector.go — `nak()` wrapper (all 6 Nak sites), `dlqOrNak` (DLQ route),
  `publishWithRetry` (retry), `renewWithRetry` (lease-renew retry), `resolveRoster` (roster error),
  `fanout` (latency); router.go — both OCC conflict re-fold loops (publish retry).
- New tests: `TestInitTracerWiresExportWhenEndpointSet` (`@spec:obs.otlp.export-wired` — endpoint set ⇒
  exporter wired + inbound trace_id continues; unset ⇒ tracer nil, log-only, trace_id still continues),
  `TestMetricsSurfaceExposesNamedSeries` (`@spec:obs.metrics.surface` — scrape exposes all 6 series).
- CROSS-REPO FOLLOW-UP (desk, NOT done here): inject `OTEL_EXPORTER_OTLP_ENDPOINT` (+`_INSECURE`,
  sampler arg) and `OBS_ENV` into the 3 RP Deployments (router/projector/authcallout), mirroring desk's
  export to `alloy.infra:4317`, to actually close the desk→RP trace waterfall and replace `env=dev`
  labels in prod. RP code is ready; only the deploy env is outstanding.
