# Delta for Signaling Core — OCC/dedup ordering is broker-agnostic

Hardens the per-subject OCC behaviour added by `router-occ`
(`router.occ.expected-subject-seq`): the router MUST NOT depend on JetStream evaluating
`Nats-Msg-Id` dedup before the expected-last-subject-sequence (OCC) check, because that ordering is
broker-dependent (a single-server R1 broker checks expected-subject FIRST).

## ADDED Requirements

### Requirement: OCC-vs-dedup ordering is broker-agnostic — a conflict re-checks command_id dedup

The router MUST NOT assume the broker evaluates `Nats-Msg-Id` dedup before the
per-subject expected-last-sequence (OCC) check; that ordering is broker-dependent (on a
single-server R1 JetStream the expected-subject check runs FIRST). Consequently, a genuine retry of
an already-committed `command_id` MAY surface as an optimistic-concurrency conflict rather than a
duplicate. On such a conflict the router MUST re-fold (rebuild from the durable log) and re-check
`command_id` dedup against the fresh fold; if the fresh fold shows the command was already
committed, the router MUST replay the original cached accepted result (same `caused_by`) and MUST
NOT append a second fact. The `LogStore` port contract MUST document this ordering as
broker-dependent rather than as a guaranteed dedup-before-OCC evaluation.

#### Scenario: Retry of an already-committed command surfaces as OCC conflict and replays accepted
- **id:** `router.occ.dedup-ordering-agnostic`
- **GIVEN** an interaction whose `command_id` is already committed to the durable log, and a router whose in-memory fold is one sequence behind the true tail (it has not yet seen that committed fact)
- **WHEN** the router retries that same `command_id` and the broker (single-server R1 ordering: expected-subject check before dedup) returns the append as an optimistic-concurrency conflict rather than a duplicate
- **THEN** the router re-folds from the durable log, recognises the already-committed `command_id`, and returns the original cached accepted `CommandResult` (status accepted, same `caused_by`) — not a spurious rejection
- **AND** the router appends NO second fact (the re-fold's dedup hit satisfies the retry); the `LogStore` port documents the OCC/dedup ordering as broker-dependent, so callers treat a conflict as "rebuild + re-check command_id dedup"
