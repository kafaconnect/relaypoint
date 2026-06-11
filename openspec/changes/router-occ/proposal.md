# Change: router-occ

## From

The router is the sole authoritative writer of every `.log` fact and assigns each fact a dense,
monotonic per-interaction `sequence` from state it rebuilds (folds) from the durable log. But
`internal/signaling/store.go` `Append` enforces only **idempotency** (`Nats-Msg-Id` dedup); it
does **not** enforce JetStream's per-subject optimistic concurrency
(`Nats-Expected-Last-Subject-Sequence`). So two router instances — an HA pair, or one router with
a stale rebuilt state — can both append a fact to the same `tenant.<t>.interaction.<id>.log`
subject carrying the **same** router-assigned `sequence`. That breaks the log-authoritative
monotonic ordering invariant under concurrency: the in-node state guard
(`signaling.cmd.concurrent-interaction-guard`) serializes commands **within** one process but
cannot arbitrate **across** processes. This gap predates the protobuf wire change.

## To

`LogStore.Append` publishes under **per-subject optimistic concurrency**: it carries an
**expected last-subject-sequence** token and uses `nats.ExpectLastSequencePerSubject(...)`, so the
broker rejects the append unless the subject is still at the expected last STREAM sequence. A
wrong-expected-sequence rejection is distinguished (`ErrOCCConflict`, retryable) from any other
error. `Replay` additionally returns the subject's current last STREAM sequence — the OCC token,
**distinct** from the dense per-interaction `sequence`.

`HandleCommand` obtains that token during the fold/getState, passes it to `Append`, and on an OCC
conflict **re-folds (rebuilds) once and retries**; a second loss returns a retryable rejection
(`lost concurrent append — retry`). All existing semantics are preserved unchanged: `command_id`
dedup, divergent-payload conflict, illegal-transition rejection, sole-writer, the dup-append
reconcile, and the poison/evict paths.

## Reason

The log must stay authoritative under the HA the platform targets: at most one fact per
router-assigned sequence, no matter how many router instances (or a single router holding a stale
fold) race on one interaction. OCC at the broker is the cheapest correct mechanism — the broker
already orders the stream, so it can reject a stale-sequence append atomically; the loser re-folds
and retries against the true tail.

## Impact

- `internal/signaling/store.go` (`LogStore` port: `Append`/`Replay` signatures + `ErrOCCConflict`),
  `internal/signaling/router.go` (`HandleCommand` OCC retry, `interactionState.streamSeq`),
  `internal/signaling/router_unit_test.go` (fakes track a per-subject stream sequence + enforce
  OCC), and a new `internal/signaling/router_occ_integration_test.go`.
- **Subjects/streams:** unchanged (`tenant.*.interaction.*.log`, `INTERACTION_LOGS`); only the
  publish header set changes (adds `Nats-Expected-Last-Subject-Sequence` alongside `Nats-Msg-Id`).
- No contract/proto change; the desk repo is untouched.
