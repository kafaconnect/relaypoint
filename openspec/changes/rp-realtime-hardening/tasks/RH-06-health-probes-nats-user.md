---
id: RH-06
slice: RH
title: HIGH — health/readiness surface on each binary plus a least-privilege projector NATS user
status: done
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
- DONE. Added `internal/health` (liveness/readiness `/healthz` + `/readyz`, distroless self-probe
  `-healthcheck`, defaulted `:8222` — no new env). Each `cmd/{router,projector,authcallout}` now
  serves it: liveness = NATS connected + JetStream `AccountInfo` reachable (authcallout = connected
  only, it uses no JS); the projector's readiness reads its leader lease via a new `Projector.Ready()`
  backed by the existing `fence` (FAILS while standby/wedged, paused on a renew stall, or lease-lost)
  — exposed through the projector's own type, no new global. Health port is a `projector.Config`
  field (`HealthAddr`, defaulted).
- Fixed the diverged `cmd/projector` NATS default (`router`/`router-dev` -> `projector`/`projector-dev`).
  Added a least-privilege `projector` NATS user to `deploy/nats/nats-server.conf` (`.log` READ-only +
  publish-deny on `.log`, AGENT_FEED/DLQ write, `$KV.projector-{lease,snapshot}` + RP-scoped JS API);
  added it to callout-exempt `auth_users`; documented the infra-NATS-anonymous posture (ADR-0004).
  Wired `PROJECTOR_PASSWORD` into compose + `.env.example`; added distroless self-probe healthchecks
  to the router + authcallout compose services.
- Tests: `internal/health` TestLivenessHealthyOnlyWhenNATSAndJetStreamReachable,
  TestReadinessIndependentOfLiveness (@spec:obs.health.liveness-nats-js); `internal/projector`
  TestReadyReflectsLeaseState (@spec:obs.health.readiness-reflects-lease); `cmd/projector`
  TestProjectorNATSUserDefaultIsProjector, TestNATSConfDefinesLeastPrivilegeProjectorUser
  (@spec:deploy.nats.projector-user).
- CROSS-REPO FOLLOW-UP (desk, NOT done here): wire k8s liveness/readiness probes against `:8222`
  `/healthz` + `/readyz` in `deploy/helm/desk/templates/relaypoint.yaml` and
  `deploy/k8s/50-52-rp-*.yaml`, and add the production `projector` NATS user/password to the desk
  Helm values.
