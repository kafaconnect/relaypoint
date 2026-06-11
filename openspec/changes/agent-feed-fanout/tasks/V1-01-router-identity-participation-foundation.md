---
id: V1-01
slice: V1
title: Router write/participation foundation — identity-from-suffix, privileged participant commands, ParticipationView
status: done
specs:
  - signaling.feed.cmd-identity-pinned
  - signaling.feed.cmd-nonparticipant-denied
  - signaling.feed.cmd-wildcard-no-reconnect
  - signaling.feed.privileged-assign-to-fact
  - signaling.feed.privileged-actor-guarded
  - signaling.feed.privileged-transfer-ordering
  - signaling.feed.participation-from-facts
---

First code slice — the only part unit-testable without NATS auth-callout config or the fan-out
service. In `internal/signaling/`:

- **Identity-from-suffix.** `.cmd` subject is `tenant.<tid>.interaction.<iid>.cmd.<identity>`; the
  router parses the identity from the LAST subject token (the ACL-pinned author), rejects a payload
  `actor_id` that disagrees with `reason: actor_mismatch`. Dev fallback: until the auth-callout
  mints a per-connection identity, the suffix is the dev identity source (advisory — the participant
  gate is not enforced under the shared-`client` posture).
- **Privileged participant commands.** `participant.assign` / `unassign` / `transfer`, role-gated to
  a trusted-backend identity (agent-role REJECTED); they write `participant.joined` / `participant.left`
  facts with audit fields (`commanded_by`, `reason`, `request_id`). `transfer` emits
  `participant.joined`(new) BEFORE `participant.left`(old).
- **ParticipationView.** A shared fold over an interaction's `.log` facts → per-agent membership
  intervals `[join_seq, left_seq)`; reusable function/port; the router authorizes agent-role commands
  with it (an agent may only command an interaction it currently participates in). The fan-out
  projector will reuse it.

Acceptance: unit tests (fake LogStore, no live NATS) tagged `// @spec:` for each scenario; full
`gofmt -l . / go vet / go build / go test ./...` green; integration suite green on an ephemeral
NATS with the migrated `cmd.*` ACL.

## Log
- 2026-06-11 done: `participation.go` (Interval/ParticipationView/FoldParticipation), router
  identity-from-suffix + `actor_mismatch` + server-side participant authz + privileged
  participation-command→fact path (transfer joined-before-left, audit fields); additive proto
  Event audit fields (commanded_by/reason/request_id); `.cmd` subject migrated to `…cmd.<identity>`
  (router subscribes `*.cmd.*`; dev `client` ACL → `cmd.*`). 9 new unit tests + existing suite
  green; `go vet`/`build`/`gofmt` clean; integration green on ephemeral NATS (port 24222).
