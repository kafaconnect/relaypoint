# Cross-review: capability-only subscriber cutover, round 2

## Scope

- Change: `substrate-boundary-invariant-rp`
- Task: `SBI-RP-03`
- Reviewer: Gemini CLI (`agy`)
- Builder: Codex
- Mode: independent read-only review after round 1 resolution

## Verdict

`PASS`

## Findings

No BLOCKER, HIGH, MEDIUM, or LOW findings.

## Verified behavior

- Subscriber authorization uses only the exact `agent-feed` and
  `agent-feed-admin-operation-observer` capabilities.
- Legacy `RoleAgent` authorization fails closed.
- Subscriber grants are tenant-pinned and treat the subscriber identifier as opaque.
- Visitor and trusted-backend roles reject capability claims.
- Durable participation gates all non-start subscriber commands.
- Allowed-connection logs include the authorizing capability.
- Specs, tasks, and scenario mappings describe the implemented boundary.

## Evidence

- `go test ./internal/authcallout -count=1`: PASS
- Reviewer verdict: T1 zero open findings.
