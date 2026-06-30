# Projector (Participation/Fan-out) Specification

## Purpose

The single-active leased fan-out worker that tails the SHARED `tenant.*.interaction.*.log`
(`INTERACTION_LOGS`) and writes `tenant.<tid>.agent.<aid>.feed.>` (`AGENT_FEED`) + the DLQ
`tenant.<tid>.agent.dlq.feed`, holding a single-active KV leader **lease**. Owned ports
(`LogSource`, `FeedSink`, `LeaseStore`, `Roster`, `SnapshotStore`) only — no concrete NATS in the
core. Materialized from the `rp-realtime-hardening` change (RDL-01/02/03, RH-02, RH-04, RH-05,
RH-10, RH-11); anchors the previously-dangling `RDL-01`/`RDL-02` concurrent-fan-out ids.

## Requirements

### Requirement: A fact is fanned to its recipient feeds concurrently, acked only after all succeed

The worker MUST publish a fact to its N recipient feeds **concurrently** (a bounded errgroup), not
N sequential `PublishMsg` RTTs, so a fact bound for N agents costs ~one RTT under the serial
`MaxAckPending=1` consumer. Semantics MUST be preserved exactly: the source `.log` fact is acked
ONLY after EVERY intended recipient publish is acknowledged (ack-after-publish); the per-`(agent,
interaction, sequence)` dedup id (`Nats-Msg-Id = <tid>.<aid>.<iid>.<sequence>`) makes any
re-publish a no-op (at-most-once per feed); and ANY single publish failure leaves the source
un-acked. This locks the deployed concurrent-fan-out perf commit behind a spec (it was previously
tagged in tests but defined in no requirement).

#### Scenario: Every recipient feed gets the fact exactly once and the source acks once
- **id:** `RDL-01`
- **GIVEN** a fact at `sequence` N bound for many recipient agents of a tenant
- **WHEN** the worker fans it out concurrently (bounded by `fanoutConcurrency`)
- **THEN** EVERY recipient feed stores the fact exactly once (the bounded errgroup drops none) and the source `.log` fact is acked exactly once

#### Scenario: One recipient failing all retries Naks the whole fact, redelivery dedups
- **id:** `RDL-02`
- **GIVEN** a concurrently-fanned fact at `sequence` N where ONE recipient fails every publish retry on its first delivery
- **WHEN** the fan-out returns that failure
- **THEN** the worker does NOT ack — it Naks the whole fact; on redelivery it re-publishes, the already-published feeds dedup to a single copy, the previously-failed recipient now lands, and the source is acked exactly once (no drop, at-most-once per feed)

### Requirement: Lease renewal is fenced within the TTL budget; an overdue renew pauses the data path

The lease-renew path MUST keep the worker's fenced (lease-held) window strictly containing its
data-path activity. The renew adapter MUST honour `ctx` (it MUST NOT discard it and ride the NATS
default request timeout), and the caller MUST bound each attempt with a per-attempt `ctx` timeout
such that the TOTAL retry time is strictly less than `(TTL − renewInterval)` — the attempt count and
timeouts MUST be DERIVED from the configured lease TTL, not hardcoded, so a TTL change cannot
silently re-open the window. On an OVERDUE renew (one that did not conclusively succeed within the
budget) the worker MUST pause `process`/`Deliver` IMMEDIATELY (stop-the-world), BEFORE the moment a
standby could re-acquire the expired lease — NOT after a fixed number of failed attempts. As a
result, two holders MUST NOT both Deliver/fan-out or both write the participation snapshot
(`kv.Put("latest", …)`) across a renewal stall, so the last-writer-wins snapshot can never be
corrupted by a stale former holder.

#### Scenario: Renew budget stays under the lease window and an overdue renew stops the world
- **id:** `RDL-03`
- **GIVEN** a configured lease `TTL` and `renewInterval`, with the worker actively Delivering/fanning-out
- **WHEN** the broker stalls the renew
- **THEN** the renew honours its per-attempt `ctx` timeout so the total retry time is `< (TTL − renewInterval)` (attempts/timeouts derived from the TTL), and the instant the renew is overdue the worker pauses `process`/`Deliver` BEFORE a standby could re-`Create` the lease
- **AND** because the data path stops the moment the lease can no longer be proven held, a former holder and a new holder never both fan out or both write the snapshot (no last-writer-wins snapshot corruption)

### Requirement: Exhausted publish and tombstone deliveries are DLQ'd, not silently Nak'd

The `process` publish-failure and tombstone-failure branches MUST gate on `Delivered(f) >=
MaxDeliver` exactly like the poison path: a transient failure (`Delivered < MaxDeliver`) Naks for
redelivery, but an EXHAUSTED failure (`Delivered >= MaxDeliver`) MUST route the fact to the DLQ
(`tenant.<tid>.agent.dlq.feed`) with the failure reason and the source `event_id`/`sequence`, then
ack — NOT a silent terminal `Nak`. An ungated `Nak` on a longer outage otherwise either drops the
fact (at-least-once violated, no DLQ, no alert) or wedges the single active consumer
(`MaxAckPending=1` → total stall).

#### Scenario: A publish that fails past max_deliver is dead-lettered, not dropped or wedged
- **id:** `projector.delivery.exhausted-to-dlq`
- **GIVEN** a fact whose recipient feed publish (or whose `feed.revoked` tombstone) fails on every delivery up to `MaxDeliver`
- **WHEN** the delivery limit is reached
- **THEN** the worker routes the fact to `tenant.<tid>.agent.dlq.feed` with the failure reason + source `event_id`/`sequence` and acks the source (the consumer is not wedged), rather than Nak-ing it terminally (which would silently drop the fact or stall the `MaxAckPending=1` consumer)

### Requirement: A roster outage retries unbounded; an empty roster soft-fails and is never cached

A roster lookup is the recipient source for tenant-shared fan-out; a desk blip MUST NOT cost a fact
its delivery. A roster ERROR MUST be retried with backoff **unbounded** (it MUST NOT be DLQ'd — the
fact is valid, only the recipient source is momentarily unavailable). An EMPTY roster (HTTP 200,
empty agents) MUST be treated as a SOFT failure — `Nak`/retry, not ack-and-drop — unless a tenant
legitimately has zero agents; and the roster cache MUST store ONLY non-empty results (an empty
success MUST NOT be cached), mirroring the existing "errors are not cached" intent, so a stale empty
cache cannot dark a tenant for the cache window.

#### Scenario: A roster error retries unbounded, never DLQ
- **id:** `projector.roster.unbounded-retry`
- **GIVEN** a fact to fan out while the roster source returns an error (desk briefly down)
- **WHEN** the worker resolves recipients
- **THEN** it Naks for redelivery and keeps retrying with backoff (unbounded) — it does NOT DLQ the fact for a transient roster outage and does NOT fan out to a stale/empty set

#### Scenario: An empty roster soft-fails and is not cached
- **id:** `projector.roster.empty-not-cached`
- **GIVEN** the roster returns HTTP 200 with an empty agent list for a tenant
- **WHEN** the worker resolves recipients and the cache is consulted
- **THEN** the worker Naks/retries (soft fail) rather than acking-and-dropping the fact, and the roster cache stores ONLY non-empty results so the next lookup re-fetches rather than serving a cached-empty set for the cache window

#### Scenario: A genuinely zero-agent tenant does not loop forever
- **id:** `projector.roster.empty-soft-fail`
- **GIVEN** a tenant the operator has confirmed legitimately has zero agents
- **WHEN** the projector resolves an empty roster for it
- **THEN** the empty result is handled by the configured zero-agent path (not an unbounded retry loop on a permanently-empty tenant), keeping the soft-fail bounded to genuine transient emptiness

### Requirement: The projector runs HA replicas relying on the KV lease for single-active

The projector Deployment MUST run >= 2 replicas as a WARM STANDBY relying on the KV leader lease for
single-active election (so a crash fails over without a cold restart), with a `RollingUpdate`
strategy — NOT `replicas: 1` + `Recreate`. The router (stateless, `QueueSubscribe(..., "router",
...)`) MUST likewise run >= 2 replicas with `RollingUpdate`; the queue group guarantees exactly one
delivery per command. The lease + queue-group invariants (single-active fold, one command delivery)
MUST hold under the added replicas.

#### Scenario: Two projector replicas, one active via the lease
- **id:** `projector.ha.warm-standby-replicas`
- **GIVEN** the projector deployed with >= 2 replicas and the router with >= 2 replicas (RollingUpdate)
- **WHEN** the active projector crashes
- **THEN** a standby acquires the KV lease and resumes as the single active worker (warm failover, no cold restart), while the router queue group still delivers each command exactly once — the single-active fold and one-delivery invariants hold under the added replicas

### Requirement: The per-fact fan-out concurrency is configurable

`fanoutConcurrency` MUST be surfaced on `Config` with a sane default rather than a hardcoded
constant, so the per-fact recipient fan-out bound can be tuned without a code change (no new env var
required — it is a `Config` field with a default).

#### Scenario: fanoutConcurrency is a Config field with a default
- **id:** `projector.config.fanout-concurrency`
- **GIVEN** the projector `Config`
- **WHEN** the worker bounds a per-fact fan-out
- **THEN** the bound is read from `Config` (defaulting when unset), not a hardcoded constant
