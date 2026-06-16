# Design: router-occ

Closes a pre-existing concurrency/HA gap in the router's log append. Mechanics only; the
log-authoritative model itself (router-assigned dense sequence, fold-from-log rebuild,
`command_id` dedup, conflict/illegal-transition rejection, sole-writer) is unchanged.

## Two sequences, kept distinct

- **`sequence`** — the dense, gapless, per-interaction number the router assigns each fact
  (1, 2, 3, …). It is application-meaningful (ordering of an interaction's facts) and lives in the
  fact payload.
- **last-subject STREAM sequence** — JetStream's own per-subject message sequence on the
  `INTERACTION_LOGS` stream. It is the OCC token only; the router never interprets its value, it
  just echoes the last-seen one back on the next append.

`Replay` returns both the folded facts and the last STREAM sequence it saw (`m.Metadata().Sequence.Stream`
of the final delivered message, 0 if the subject is empty). `rebuild` records it on
`interactionState.streamSeq`.

## Append under OCC

`LogStore.Append(subject, data, dedupID, expectedLastSubjSeq)` publishes with both
`nats.MsgId(dedupID)` (idempotency, unchanged) **and**
`nats.ExpectLastSequencePerSubject(expectedLastSubjSeq)`. JetStream commits only if the subject's
current last STREAM sequence equals the expectation; otherwise it returns an `*nats.APIError` with
`ErrorCode == JSErrCodeStreamWrongLastSequence` (10071), which the adapter maps to the sentinel
`ErrOCCConflict`. Any other publish error is returned as-is (fail closed). Dedup is evaluated by
the broker first, so a genuine `Nats-Msg-Id` retry still returns `duplicate=true` and never
trips OCC.

## Retry in HandleCommand

The append now sits in a bounded loop (`attempt` 0 then 1). Each attempt re-checks the
`command_id` binding and legality against the current fold (a re-fold can reveal a concurrent
writer committed this command_id, or ended the interaction). On `ErrOCCConflict`:

1. `rebuild` from the log (the true tail), updating `seq`/`streamSeq`/`status`/`results` in place.
2. If the rebuild failed, or this was already the retry attempt, poison + evict the state and
   return a retryable rejection (`lost concurrent append — retry`) — never append behind a stale
   sequence.
3. Otherwise loop once more with the fresh token.

On a clean commit the in-memory `seq` and `streamSeq` each advance by exactly one (one fact
committed). The dup-append reconcile path and the lost-ack reconcile path are unchanged except
they now also carry `streamSeq` forward from the fresh fold.

## Why one retry, not unbounded

The per-router `interactionState.mu` already serializes a single router's commands on one
interaction, so the only contenders on a subject are *distinct* router instances. With a small,
fixed instance count the loser wins on its retry; a still-failing second attempt is surfaced as
retryable so the client/transport (not an unbounded server loop) decides whether to re-issue. This
keeps the server's worst-case work per command bounded.

## Loose coupling

The OCC contract lives on the `LogStore` **port** (the `expectedLastSubjSeq` parameter, the
returned `lastSubjSeq`, and `ErrOCCConflict`), not on NATS. The router core depends only on the
port; the JetStream adapter is the only place that knows the `ExpectLastSequencePerSubject` header
and the wrong-last-sequence error code. The in-memory fake in the unit tests models the same OCC
(a per-subject counter) with no NATS, so the core stays unit-testable.

## Verification

A live-NATS integration test (`router_occ_integration_test.go`) runs two routers over one stream
(an HA pair) and fires concurrent commands on one interaction: the durable log holds exactly one
fact per sequence (no duplicate sequence, dense and ordered), and the head-to-head loser re-folds
and lands the next sequence rather than being rejected. The test is load-bearing — with OCC
removed it fails on a duplicate sequence.
