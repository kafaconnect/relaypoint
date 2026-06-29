---
id: RH-03
slice: RH
title: HIGH — partially-applied participant.transfer must re-drive idempotently (subset, not exact-equality)
status: todo
specs: [router.transfer.partial-apply-idempotent]
---

## Goal
A transfer drives `joined(new)` then `left(old)`; if `joined` commits but `left` fails (OCC
exhaustion / poison), a retry with the same `command_id` rebuilds a 1-elem recorded set vs a 2-elem
want and is rejected `command_id reused with a different payload` → transfer stuck (new joined, old
still a member = over-delivery) recoverable only by a fresh `command_id`.

## Success criteria (test-first)
- A FAILING test for the partial-apply retry path first (join committed, leave failed, same
  `command_id` retried).
- Replace the `sameSet(recorded, want)` exact-equality precheck with a SUBSET check (every recorded
  sub-id ∈ want): a divergent payload still mismatches and is rejected, but a partial apply re-drives
  the missing `participant.left` while the committed `participant.joined` dedups on its `command_id`.
- Transfer completes after retry without a fresh `command_id`.

## Files
- `internal/signaling/router.go` (`sameSet` → subset check at the transfer reconcile, ~:498-529)
- `internal/signaling/router_unit_test.go` (partial-apply retry test + divergent-payload still-rejected test)

## Spec
`// @spec:router.transfer.partial-apply-idempotent`

## Log
- todo
