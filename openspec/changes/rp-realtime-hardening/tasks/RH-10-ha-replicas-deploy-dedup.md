---
id: RH-10
slice: RH
title: MED — enable HA replicas (queue group / KV lease) plus delete duplicated mutable-tag deploy defs
status: done
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
- DONE: RP-repo single-active-under-replicas test AND the desk-side deploy are both complete. The
  desk-side deploy (rp-router replicas>=2 + RollingUpdate; rp-projector replicas 2 warm-standby;
  delete the stale `deploy/k8s/50-52-rp-*.yaml`) lands in the **kafaconnect/desk** repo (the
  authoritative production Helm/k8s tree) — tracked and committed there, not in this repo.
- Added `TestIntegration_TwoReplicasSingleActiveWarmStandbyFailover`
  (`internal/projector/projector_integration_test.go`, `// @spec:projector.ha.warm-standby-replicas`):
  TWO projector instances share ONE JetStream + ONE lease bucket (`kvLeaseName`) — the deployed
  `replicas: 2` warm standby. Proves under >=2 replicas: (1) single-active — exactly one instance is
  leader (`Ready()==nil`), the other is fenced in `Acquire` and never goes live / never fans out;
  (2) warm-standby failover — on the holder's lease Release the standby acquires and resumes the fold
  from the durable ack floor; (3) exactly-once across the handover — every command on the feed once,
  no skip / no double-delivery.
- No production seam added: reused the existing public `Projector.Ready()` (RH-06) as the
  leader-vs-standby observable + the existing integration harness (`runProjector`, `connectJS`,
  `freshStreams`, `waitUntil`). Lease transitions are driven explicitly (stop leader -> deferred
  `Release` deletes the leader key), not by sleeping out the 5s TTL.
- Documented coverage boundary: a hard-CRASH TTL-lapse takes the SAME `Acquire/Create` path,
  exercised deterministically here via explicit Release; and the feed `Nats-Msg-Id` dedup makes a
  rogue standby publish invisible at the feed, so single-active is asserted at the lease/`Ready()`
  seam while end-to-end exactly-once is asserted by feed command counts across the real handover.
- Verify (real JetStream NATS): `go build ./...` `go vet ./...` `gofmt -l .` (empty)
  `go test ./...` `go test -count=1 -p 1 -tags integration ./...` — ALL PASS (7 pkgs each).
- CROSS-REPO (desk, LANDED, NOT this repo): rp-router replicas>=2 (RollingUpdate), rp-projector
  replicas 2 in `deploy/helm/desk/templates/relaypoint.yaml`, and the stale
  `deploy/k8s/50-52-rp-*.yaml` (mutable-tag/`IfNotPresent`) deleted — `// @spec:deploy.images.immutable-tags`.
