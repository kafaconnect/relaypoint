# Delta for Signaling Core — router OCC

Extends the router-authoritative command plane: the log append now holds across router
instances, not only within one process. This is additive to
`signaling.cmd.concurrent-interaction-guard` (which serializes commands within a single node) —
it closes the cross-instance/stale-fold gap.

## ADDED Requirements

### Requirement: Log append is guarded by per-subject optimistic concurrency

The router MUST append a `.log` fact under JetStream per-subject optimistic concurrency: every
append MUST carry the expected last STREAM sequence for that interaction's subject
(`Nats-Expected-Last-Subject-Sequence`), and the broker MUST reject the append unless the subject
is still at that sequence. The expected last-subject STREAM sequence is OBTAINED during the
fold/replay of the interaction's state and is DISTINCT from the router-assigned dense per-interaction
`sequence`.

A wrong-expected-sequence rejection MUST be distinguished from other publish errors and treated as
a retryable optimistic-concurrency conflict. On such a conflict the router MUST re-fold (rebuild
state from the durable log) and retry the append ONCE; if it still loses, the router MUST return a
retryable rejection and MUST NOT append behind a stale sequence. As a result, under concurrent
writers (two router instances, or one router with a stale rebuilt state) racing on ONE interaction,
the durable log holds EXACTLY ONE fact per router-assigned `sequence` — no duplicate sequence, the
ordering stays dense and monotonic.

All other command-plane semantics are PRESERVED unchanged: `command_id` idempotent dedup, the
divergent-payload conflict rejection, illegal-transition rejection, and sole-writer (only the
router writes the log).

#### Scenario: Concurrent writers cannot duplicate a router-assigned sequence
- **id:** `router.occ.expected-subject-seq`
- **GIVEN** one interaction on the live `INTERACTION_LOGS` stream and two router instances (an HA pair, or one router holding a stale fold) that both believe the subject is at the same last STREAM sequence
- **WHEN** they concurrently issue commands that each fold to the same next router-assigned `sequence`
- **THEN** the append carries `Nats-Expected-Last-Subject-Sequence` and the broker commits exactly one of the racing facts, so the durable log holds exactly ONE fact per `sequence` (no duplicate sequence; the log stays dense, ordered, monotonic)
- **AND** the loser observes the optimistic-concurrency conflict, re-folds from the durable log ONCE, and retries — landing the NEXT sequence (an accepted result), never a duplicate; a still-losing retry returns a retryable rejection rather than appending behind a stale sequence
