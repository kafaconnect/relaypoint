---
id: RH-10
slice: RH
title: MED — enable HA replicas (queue group / KV lease) plus delete duplicated mutable-tag deploy defs
status: todo
specs: [projector.ha.warm-standby-replicas, deploy.images.immutable-tags]
---

## Goal
HA replicas are unused: rp-router is `replicas:1` despite being stateless + `QueueSubscribe(...,
"router", ...)`; rp-projector is `replicas:1`+`Recreate` despite the KV-lease single-active design
(warm standby intended). And two divergent deploy defs exist: `deploy/k8s/50-52-rp-*.yaml` use
mutable non-sha tags (`rp-router:signal-test`, `rp-projector:roster`, `rp-authcallout:m17`) +
`IfNotPresent` + stale "auth_callout ENABLED" comments, unreferenced by any kustomization, while Helm
uses sha-traceable tags.

## Success criteria
- A test/assertion that the single-active fold + one-command-delivery invariants hold under >= 2
  replicas (lease election + queue group).
- rp-router → replicas >= 2 (`RollingUpdate`; the queue group guarantees one delivery); rp-projector
  → replicas 2 relying on the lease for single-active (warm-standby crash failover).
- Delete the stale `deploy/k8s/50-52-rp-*.yaml` (Helm canonical) or generate from the chart; never
  ship a mutable tag with `IfNotPresent`.

## Files
- CROSS-REPO (desk): `deploy/helm/desk/templates/relaypoint.yaml` (replicas + strategy), delete `deploy/k8s/50-52-rp-*.yaml`
- `internal/projector/projector_test.go` (single-active-under-replicas invariant test)

## Spec
`// @spec:projector.ha.warm-standby-replicas`, `// @spec:deploy.images.immutable-tags`

## Log
- todo
