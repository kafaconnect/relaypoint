---
id: RH-04
slice: RH
title: HIGH — gate publish/tombstone Nak on MaxDeliver to DLQ, roster failure retries unbounded
status: done
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
- done: `poison`→`dlqOrNak` (one MaxDeliver-gated DLQ+ack helper, reads `cfg.MaxDeliver`); the publish/tombstone Nak branches now route through it (exhausted → DLQ+ack, else Nak), the roster-error branch stays a direct unbounded Nak (never DLQ). Tests: `TestExhaustedDeliveryToDLQ` (@spec:projector.delivery.exhausted-to-dlq) + `TestRosterErrorRetriesUnboundedNeverDLQ` (@spec:projector.roster.unbounded-retry); fake source gained an opt-in `redeliverCap` modelling the broker terminating at MaxDeliver. build/vet/vet-integration/race green; RDL-01/02/03 + failFor tests preserved.
- cross-review follow-up: a plain `Nak` on roster failure still counts toward `MaxDeliver=5`, so "roster retries unbounded" was false (a >5-redelivery desk outage still terminated a valid fact). Fixed: roster resolution moved into `resolveRoster`, which retries IN-PROCESS holding the delivery via `msg.InProgress()` (new `LogSource.InProgress` port — extends the ack deadline without consuming the delivery budget) for a bounded window (`Config.RosterRetryWindow`, default 90s ~ a few × AckWait), only falling back to `Nak` after the cap. Briefly holds the `MaxAckPending=1` consumer, acceptable for a short desk blip and far better than dropping a valid fact. `dlqOrNak` (exhausted publish/tombstone → DLQ+ack) unchanged. Renamed `TestRosterErrorRetriesUnboundedNeverDLQ` → `TestRosterErrorHeldViaInProgressThenBoundedNakNeverDLQ` (keeps `@spec:projector.roster.unbounded-retry`): asserts InProgress is used + never DLQ/ack-drop; `TestTenantRosterErrorRecoversInProcessNoNak` shows a transient error recovers in-process with no Nak.
