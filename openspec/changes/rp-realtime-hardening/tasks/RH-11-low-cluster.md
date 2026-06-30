---
id: RH-11
slice: RH
title: LOW cluster — assigned emit/drop, stream ceiling, fail-loud password, fanout config, plus contract/comment/doc notes
status: done
specs: [router.cluster.assigned-emit-or-drop, signaling.stream.retention-ceiling, router.config.fail-loud-password, projector.config.fanout-concurrency]
---

## Goal
A cluster of low-severity correctness/operability/contract gaps. The verifiable ones carry a spec id;
the doc/comment-only ones are tracked here and closed in prose/runbook.

## Success criteria
- (a) `interaction.assigned` is recognized (router.go:426-432) but never emitted (handleParticipation
  :465-485): EMIT it for assign (distinct from a transfer's `joined`) OR drop it from the recognized
  set + document. `// @spec:router.cluster.assigned-emit-or-drop`
- (c) `EnsureLogStream` returns the `UpdateStream` err when `AddStream` AND `UpdateStream` both fail
  (today returns the `AddStream` err); the `INTERACTION_LOGS` stream gains a `MaxBytes`/`MaxAge`
  ceiling + alert; per-subject discard is never enabled on it.
  `// @spec:signaling.stream.retention-ceiling`
- (h) `cmd/router/main.go:33` + `cmd/projector/main.go:38` default `NATS_PASSWORD="router-dev"` →
  fail-loud (`mustEnv`) like authcallout. `// @spec:router.config.fail-loud-password`
- (i) `fanoutConcurrency=32` hardcoded (projector.go:28) → surfaced on `Config` with a default (no
  env var). `// @spec:projector.config.fanout-concurrency`
- NOTES (no spec id; doc/comment/runbook only):
  - (b) Participation command payload is ad-hoc JSON (router.go:434-458) in a protobuf-everywhere
    wire — define a protobuf participation payload OR document the JSON schema as a contract.
  - (d) `visitor_token.go:186-214` JWKS fail-closed-after-TTL availability cliff — document the
    operational dependency + consider a bounded stale-grace window for exp-valid tokens (runbook).
  - (e) `visitor_token.go:57,65,92-93` "per-kid cooldown" comment is actually a global throttle — fix
    the comment.
  - (f) `visitor_token.go:130-137` no clock-skew leeway on `exp`/`nbf` — note only (secure as-is).
  - (g) `GOWORK=off` undocumented for local submodule builds — document in README/AGENTS.

## Files
- `internal/signaling/router.go` (assigned emit/drop), `internal/signaling/store.go` (EnsureLogStream err + stream ceiling)
- `cmd/router/main.go`, `cmd/projector/main.go` (fail-loud password)
- `internal/projector/projector.go` (`Config.FanoutConcurrency` + default)
- `internal/authcallout/visitor_token.go` (comment fix; runbook notes), `README.md`/`AGENTS.md` (GOWORK)

## Spec
`// @spec:router.cluster.assigned-emit-or-drop`, `// @spec:signaling.stream.retention-ceiling`, `// @spec:router.config.fail-loud-password`, `// @spec:projector.config.fanout-concurrency`

## Log
- **(a) DROP** `interaction.assigned` — it is a DOMAIN verb (assign vs a transfer's joined), not a delivery
  STRUCTURE; RP carries membership-open as one structural fact (`participant.joined`). Removed from the
  recognized set (`isParticipationFact`, router.go) AND the fold (`ParticipationView.ApplyFact`,
  participation.go). Assign still emits `participant.joined`. Safe for live data: the type was never
  emitted, so no `.log` contains it. Tests: new `TestParticipation_AssignedFactDropped`
  (`@spec:router.cluster.assigned-emit-or-drop`); updated `TestParticipation_AssignedAndRejoin` and
  `TestRouter_DirectParticipationFactRejected` to use the structural fact.
- **(c)** `ensureStream` helper now returns the **UpdateStream** err when both create+reconcile fail
  (was the AddStream err). `INTERACTION_LOGS` gained ceilings: **MaxAge = 365d** (> any realistic
  open-interaction lifetime), **MaxBytes = 50 GiB** (caps a runaway before a single-node disk fills).
  Both are named code-level defaults, NO env. `Discard` stays `DiscardOld` (whole-stream);
  `MaxMsgsPerSubject` stays `-1`; **DiscardNewPerSubject is never set** (per-subject discard would drop an
  open interaction's head and corrupt replay). Alert-before-ceiling is a runbook doc note (in-code).
  Tests: `TestLogStreamRetentionCeiling`, `TestEnsureStreamReturnsUpdateErrWhenBothFail`
  (`@spec:signaling.stream.retention-ceiling`).
- **(h)** `NATS_PASSWORD` is now `mustEnv` in `cmd/router/main.go` (added a `mustEnv` mirroring
  authcallout exactly) and `cmd/projector/main.go` (reused its existing `mustEnv`; removed the
  `defaultNATSPassword` const). Test: `TestNATSPasswordFailLoud` in both cmds — a subprocess re-exec
  with `NATS_PASSWORD` stripped asserts a non-zero exit (`@spec:router.config.fail-loud-password`).
  Updated `TestProjectorNATSUserDefaultIsProjector` (dropped the removed-const assertion).
  **CROSS-REPO DEPLOY REQUIREMENT (orchestrator, companion desk PR):** the desk Helm values for BOTH
  `rp-router` and `rp-projector` MUST now set a non-empty `NATS_PASSWORD` env or the pods crash-loop at
  startup. Dev infra NATS is anonymous so any non-empty value works, but the var MUST be present.
  RP `deploy/docker-compose.yml` already passes `NATS_PASSWORD: ${ROUTER_PASSWORD:-router-dev}` to
  router (always non-empty) and has no projector service, so no compose/.env change was needed.
- **(i)** `fanoutConcurrency` const → `Config.FanoutConcurrency` (default 32 via `defaultFanoutConcurrency`
  in `withDefaults`, NO env); `fanout()` uses `p.cfg.FanoutConcurrency`. Test:
  `TestConfigFanoutConcurrencyDefault` (`@spec:projector.config.fanout-concurrency`).
- **NOTES closed in prose/comment:** (b) documented `participationData` as the trusted-backend JSON
  participation-command contract (deliberate protobuf exception; protobuf payload is a TODO).
  (d) added an OPERATIONAL-DEPENDENCY/runbook note at the JWKS fail-closed site (availability cliff past
  TTL; bounded stale-grace flagged as a deliberate non-goal, security-over-availability). (e) FIXED the
  "per-kid cooldown" comment → it is a GLOBAL one-shared-timer throttle. (f) noted the deliberate
  zero clock-skew leeway on exp/nbf (secure as-is). (g) documented `GOWORK=off` for local submodule
  builds in `AGENTS.md` + clarified the comment-rule scope split (DEFAULT-ZERO = production; tests looser).
- Verify (GOWORK=off): build OK, vet OK, `gofmt -l .` clean, unit `go test ./...` ok (~4.7s), integration
  `go test -count=1 -p 1 -tags integration ./...` ok (~15.5s; external-NATS:14222 signaling tests SKIP — expected).
