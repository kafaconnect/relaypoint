---
id: RH-06
slice: RH
title: HIGH — health/readiness surface on each binary plus a least-privilege projector NATS user
status: todo
specs: [obs.health.liveness-nats-js, obs.health.readiness-reflects-lease, deploy.nats.projector-user]
---

## Goal
No RP deployment has liveness/readiness probes and no binary exposes a health surface; the NATS user
model is diverged — `nats-server.conf` defines `router`/`authsvc`/`client` with NO `projector` user,
yet the deployment sets `NATS_USER=projector` and `cmd/projector` defaults to `router`/`router-dev`.
It works only because infra NATS is currently anonymous → enabling auth darks fan-out.

## Success criteria (test-first)
- A health-handler unit test first: `/healthz` reports healthy only with NATS connected + JetStream
  reachable; the projector's readiness FAILS when wedged / lease lost / paused.
- Each `cmd/{router,projector,authcallout}` exposes a small health listener (e.g. `:8222/healthz`)
  reading liveness from the owned ports; readiness/liveness probes wired in the chart.
- A `projector` NATS user (`.log` read + `AGENT_FEED`/DLQ write + KV lease perms) added to the
  authoritative model; the infra-NATS-anonymous posture documented; stale "auth_callout ENABLED"
  comments removed; the wrong `cmd/projector` `NATS_USER` default fixed.

## Files
- `cmd/router/main.go`, `cmd/projector/main.go`, `cmd/authcallout/main.go` (health listener; fix projector NATS_USER default)
- `internal/projector/projector.go` / ports (expose lease-held for readiness)
- `deploy/nats/nats-server.conf` (add `projector` user; document anonymous posture)
- `deploy/docker-compose.yml` (healthchecks)
- CROSS-REPO follow-up (desk): `deploy/helm/desk/templates/relaypoint.yaml` + `deploy/k8s/50-52-rp-*.yaml` probe wiring + production `projector` user in Helm values — tracked, not edited here.

## Spec
`// @spec:obs.health.liveness-nats-js`, `// @spec:obs.health.readiness-reflects-lease`, `// @spec:deploy.nats.projector-user`

## Log
- todo
