---
id: RH-04
slice: RH
title: HIGH — gate publish/tombstone Nak on MaxDeliver to DLQ, roster failure retries unbounded
status: todo
specs: [projector.delivery.exhausted-to-dlq, projector.roster.unbounded-retry]
---

## Goal
`process` Naks on fan-out fail (:309), tombstone fail (:316), and roster fail (:292) WITHOUT checking
`Delivered(f) >= MaxDeliver` (unlike `poison()` :382 which DLQs+acks). `MaxDeliver=5` exhausts in
~1.75s, so a longer blip either silently terminates the fact (at-least-once violated, no DLQ, no
alert) or wedges the single-active consumer (`MaxAckPending=1` → total stall).

## Success criteria (test-first)
- A FAILING fail-ALL-deliveries test first (existing `failFor` tests recover on redelivery and never
  exhaust): a publish/tombstone failing every delivery up to `MaxDeliver` must DLQ + ack, not Nak.
- Publish + tombstone Nak branches gate on `Delivered >= MaxDeliver` → route to DLQ like `poison()`
  (reason + source `event_id`/`sequence`), then ack.
- Roster failure prefers UNBOUNDED retry/backoff — never DLQ a fact for a transient roster outage.

## Files
- `internal/projector/projector.go` (`process` publish/tombstone branches → `Delivered>=MaxDeliver` DLQ; roster branch unbounded retry)
- `internal/projector/projector_test.go` (fail-all-deliveries → DLQ test; roster-error unbounded-retry test)

## Spec
`// @spec:projector.delivery.exhausted-to-dlq`, `// @spec:projector.roster.unbounded-retry`

## Log
- todo
