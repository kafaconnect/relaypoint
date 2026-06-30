# Signaling Core Specification

## Purpose

The router write plane on the SHARED `INTERACTION_LOGS` stream (`tenant.*.interaction.*.log`).
Materialized from the `rp-realtime-hardening` change (RH-01, RH-03, RH-07, RH-11), additive to the
`router-occ` OCC mechanism — it corrects the OCC **token source**, the partial-transfer retry, the
rebuild fetch tail + in-process state lifetime, and a cluster of low-severity contract/config gaps.
No subject/stream rename; the stream gains a size ceiling.

## Requirements

### Requirement: The OCC token is the broker-committed stream sequence, not a guess

On a clean append the router MUST set its per-subject OCC token to the **stream sequence the broker
committed** for that append, NOT to `previous + 1`. Because `INTERACTION_LOGS` is a SHARED stream,
`Nats-Expected-Last-Subject-Sequence` is evaluated against the GLOBAL stream sequence of the
subject's last message; another interaction interleaving on the stream advances that value by more
than one, so a `++`-guessed token is stale and produces a SPURIOUS optimistic-concurrency conflict.
`LogStore.Append` MUST therefore return the committed message's stream sequence
(`ack.Sequence`), and the router MUST record that returned value as `streamSeq` after a clean
commit; the dup and error paths continue to self-correct via `Replay`/rebuild. Under concurrent load
across DISTINCT interactions on the shared stream, a correctly-folded append MUST NOT raise a
spurious conflict, and the single retry budget MUST remain available to arbitrate a GENUINE
same-subject race (so a coincident real concurrent write is no longer wrongly rejected). The dense
per-interaction `sequence` is unaffected — it stays monotonic and gap-free.

#### Scenario: Interleaved interactions on the shared stream raise no spurious OCC conflict
- **id:** `router.occ.committed-stream-seq`
- **GIVEN** two distinct interactions `A` and `B` on the live `INTERACTION_LOGS` stream appending facts ALTERNATELY (so the global stream sequence on each subject advances by more than one between that subject's own appends)
- **WHEN** the router appends successive facts to `A` and to `B` under per-subject OCC
- **THEN** each clean append records `streamSeq` from the broker-committed `ack.Sequence` (not `prev+1`), so NO append raises a spurious `ErrOCCConflict`, the durable log stays dense and monotonic per interaction, and the single retry budget is consumed ONLY by a genuine same-subject race (a coincident real concurrent write on one subject is still arbitrated, not pre-empted by a phantom conflict)

### Requirement: A partially-applied participation transfer re-drives idempotently

A `participant.transfer` writes the new agent's `participant.joined` BEFORE the old agent's
`participant.left` (no-gap). If the join commits but the leave fails (OCC exhaustion / poison), a
retry with the SAME `command_id` MUST be able to re-drive the still-missing fact. The router MUST
therefore reconcile the recorded result against the wanted set with a **subset** check (every
recorded sub-id ∈ the wanted set), NOT an exact-equality precheck: a divergent payload still
mismatches (rejected `command_id reused with a different payload`), but a partially-applied transfer
re-drives the missing `participant.left` (the already-committed `participant.joined` dedups on its
`command_id`). A partial apply MUST NOT leave the transfer permanently un-retryable (new joined, old
still a member = over-delivery recoverable only by a fresh `command_id`).

#### Scenario: Partial transfer retry completes the missing leave, not rejects
- **id:** `router.transfer.partial-apply-idempotent`
- **GIVEN** a `participant.transfer{from: alice, to: bob}` whose `participant.joined{bob}` committed but whose `participant.left{alice}` failed, retried with the SAME `command_id`
- **WHEN** the router reconciles the recorded result (1-element: the committed join) against the wanted set (2-element: join + leave) using a subset check
- **THEN** it does NOT reject as `command_id reused with a different payload`; it re-drives `participant.left{alice}` while the already-committed `participant.joined{bob}` dedups on its `command_id`, so the transfer completes (alice no longer a member) without needing a fresh `command_id`
- **AND** a genuinely divergent payload for that `command_id` still fails the subset check and is rejected

### Requirement: Router rebuild uses a no-wait fetch and bounds in-process state

The router's `Replay` MUST NOT pay a fixed wait tail on a drained or under-full subject: it MUST
issue a **no-wait** fetch (or an equivalent direct/last-message read) so a subject with no further
messages returns IMMEDIATELY rather than blocking to a `MaxWait` expiry — this fetch is on the
first-access, every-OCC-conflict, and every-dup path, so its latency compounds. The router MUST also
**bound its in-process state**: the per-interaction state map and the per-`command_id` results cache
MUST be idle-evictable (TTL or LRU), since both are rebuildable from the durable log; long-lived open
interactions MUST NOT grow router memory without bound at multi-tenant scale. Eviction MUST be
transparent — an evicted interaction rebuilds from the log on next access with identical semantics.

#### Scenario: A drained subject rebuild returns without a wait tail
- **id:** `router.rebuild.no-wait-fetch`
- **GIVEN** an interaction whose `.log` subject has been fully drained (no further messages)
- **WHEN** the router rebuilds state from the log (first access, an OCC conflict, or a dup reconcile)
- **THEN** the fetch returns immediately via a no-wait/direct read rather than blocking to a `MaxWait` expiry, and the rebuilt state is identical to a waiting fetch's

#### Scenario: Idle interaction state is evicted and rebuilds on next access
- **id:** `router.state.idle-evict`
- **GIVEN** an interaction whose in-process state and `command_id` results cache have been idle past the eviction threshold
- **WHEN** the eviction runs and the interaction is later accessed again
- **THEN** the idle state is removed (bounding router memory) and the next access rebuilds it from the durable log with identical sequence/dedup semantics

### Requirement: Low-severity router contract and config hardening

The router MUST close the following correctness/operability gaps:

- `interaction.assigned` MUST either be EMITTED on assign (distinct from a transfer's `joined`) or
  REMOVED from the recognized event set — the recognized-but-never-emitted state MUST NOT persist.
- `INTERACTION_LOGS` MUST carry a `MaxBytes`/`MaxAge` ceiling with an alert; per-subject `discard`
  MUST NOT be enabled on this stream (it would silently drop a live interaction's facts).
- The router (and projector) NATS password MUST be **fail-loud** (`mustEnv`), like the auth-callout —
  a missing credential MUST abort startup, not silently default to `router-dev`.
- `EnsureLogStream` MUST return the `UpdateStream` error when both `AddStream` and `UpdateStream`
  fail (today it returns the `AddStream` error, masking the real cause).

#### Scenario: interaction.assigned is emitted on assign or removed from the recognized set
- **id:** `router.cluster.assigned-emit-or-drop`
- **GIVEN** the router recognizes `interaction.assigned` in its event set
- **WHEN** an assignment (not a transfer) is processed
- **THEN** the router EITHER writes a distinct `interaction.assigned` fact OR `interaction.assigned` is removed from the recognized set and documented — it is never recognized-but-unreachable

#### Scenario: The shared log stream has a retention ceiling and never per-subject discards
- **id:** `signaling.stream.retention-ceiling`
- **GIVEN** the `INTERACTION_LOGS` stream config
- **WHEN** it is ensured
- **THEN** it declares a `MaxBytes`/`MaxAge` ceiling (with an alert hook) and does NOT enable per-subject discard, so a single live interaction's facts are never silently dropped

#### Scenario: A missing NATS password aborts startup loudly
- **id:** `router.config.fail-loud-password`
- **GIVEN** the router or projector started without its NATS password env set
- **WHEN** it boots
- **THEN** it aborts with a fatal "missing required env" rather than silently connecting with a `router-dev` default
