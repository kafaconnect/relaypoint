# Deploy Specification

## Purpose

The authoritative NATS user model and deploy-definition hygiene for the RP services. The local
`deploy/nats/nats-server.conf` is the dev/integration reference (it intentionally retains the dev
`client` user per ADR-0004); the PRODUCTION user model + Deployments live in the desk repo
(`deploy/helm/desk/...`, `deploy/k8s/50-52-rp-*.yaml`) and the desk-side edits are tracked as a
cross-repo follow-up. Materialized from the `rp-realtime-hardening` change (RH-06 adds the
least-privilege `projector` identity; RH-10 removes the duplicated mutable-tag deploy definitions).

## Requirements

### Requirement: A least-privilege projector NATS identity exists in the authoritative user model

The NATS user model MUST define a `projector` user with EXACTLY the permissions the fan-out service
needs — read `tenant.*.interaction.*.log` (the `INTERACTION_LOGS` source), write
`tenant.<tid>.agent.*.feed.>` + `tenant.<tid>.agent.dlq.feed` (`AGENT_FEED` + DLQ), and the KV
lease/snapshot subjects — and the projector Deployment MUST use it (the wrong
`cmd/projector` default `NATS_USER=router`/`NATS_PASSWORD=router-dev` MUST be fixed). Today the
deployment sets `NATS_USER=projector` but no `projector` user is defined, so enabling auth on the
shared bus would dark fan-out; the dev `nats-server.conf` reference MUST document the
infra-NATS-currently-anonymous posture, and stale "auth_callout ENABLED" comments MUST be removed.

#### Scenario: The projector connects as a defined least-privilege user
- **id:** `deploy.nats.projector-user`
- **GIVEN** the authoritative NATS user model (dev `nats-server.conf` reference + production desk Helm values)
- **WHEN** the projector connects
- **THEN** a `projector` user is defined granting only `.log` read + `AGENT_FEED`/DLQ write + KV lease perms, the projector binary defaults no longer fall back to `router`/`router-dev`, and the anonymous-infra posture is documented (no stale "auth_callout ENABLED" comment) — so enabling auth does not dark fan-out

### Requirement: No deploy definition ships a mutable image tag with IfNotPresent

The deploy definitions MUST be single-sourced on the sha-traceable Helm chart. The stale
`deploy/k8s/50-52-rp-*.yaml` (mutable non-sha tags like `rp-router:signal-test`,
`rp-projector:roster`, `rp-authcallout:m17` with `IfNotPresent`, unreferenced by any kustomization)
MUST be deleted (Helm is canonical) or generated from the chart, and no Deployment MUST ship a
mutable tag with `IfNotPresent`.

#### Scenario: Image references are sha-traceable, never mutable-tag + IfNotPresent
- **id:** `deploy.images.immutable-tags`
- **GIVEN** the RP deploy definitions
- **WHEN** an operator resolves the image for any RP service
- **THEN** it comes from the sha-traceable Helm chart, the stale duplicated `deploy/k8s/50-52-rp-*.yaml` are removed (or chart-generated), and no Deployment pairs a mutable tag with `IfNotPresent`
