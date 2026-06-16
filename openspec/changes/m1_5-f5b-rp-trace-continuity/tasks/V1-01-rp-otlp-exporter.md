---
id: V1-01
slice: V1
title: OTLP exporter behind obs.StartSpan (twin of desk f5a) + cmd inits + tests
status: done
issue:
specs: [observability]
---
## Log
- 2026-06-15 done: internal/obs/otel.go (InitTracer no-op/fail-open; startOTelSpan continues trace_id,
  local-child stays local) + trace.go StartSpan branch; InitTracer wired in cmd/{router,projector,
  authcallout}; go.mod otel deps. Tests otel_test.go (`// @spec:obs.otlp-exporter-behind-seam`,
  `// @spec:obs.trace-spans-nats-hops`, `// @spec:obs.sampling-config`). build + obs tests + check-logging green.
