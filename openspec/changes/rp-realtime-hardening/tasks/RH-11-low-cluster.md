---
id: RH-11
slice: RH
title: LOW cluster — assigned emit/drop, stream ceiling, fail-loud password, fanout config, plus contract/comment/doc notes
status: todo
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
- todo
