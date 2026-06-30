# ADR-0007: The projector fences its data path within the lease TTL budget

- Status: **Accepted** (openspec change `rp-realtime-hardening`, RH-02 — applied)
- Date: 2026-06-29
- Scope: the RelayPoint Participation/Fan-out service's lease-renew + data-path fencing. Constrains
  the leased single-active worker model of **ADR-0003** (Decision 4). Relates to the un-reviewed perf
  commit `467c1c8` (lease-renew tolerance), which this ADR corrects.

## Context

ADR-0003 makes the projector a single-active worker: standby replicas contend for a NATS KV leader
lease (TTL ~5s), and only the holder Delivers/fans-out/snapshots. Correctness depends on the
worker's **fenced** (lease-held) window strictly containing its data-path activity — if a former
holder keeps touching the data path after a standby could have taken the expired lease, two holders
can both `kv.Put("latest", …)` (last-writer-wins, no CAS) and corrupt the participation snapshot.

The deployed pin broke that fence. The lease-renew tolerance perf commit added a 3-attempt retry to
avoid a crash on a transient `nats: timeout`, but: the KV-lease `Renew(_ context.Context)` discards
`ctx` and rides the NATS default ~5s request timeout, and `renewWithRetry` (3 × ~5s + backoff ≈
15.6s) dwarfs the 5s TTL. So the run loop is only told the lease is in doubt ~13s after a standby
could have re-`Create`d it — a wide double-ownership window the existing safety nets (dedup +
`MaxAckPending=1`) bound for double-DELIVERY but NOT for double-SNAPSHOT. The commit chased ~18s of
availability at the cost of the fence.

## Decision

The renew path keeps the fence:

1. **Renew honours `ctx`.** The KV-lease adapter uses the passed context, not the NATS default
   timeout.
2. **Budget invariant.** The caller bounds each attempt with a per-attempt `ctx` timeout such that
   `attempts × (per_attempt_timeout + backoff) < (TTL − renewInterval)`; the attempts/timeouts are
   DERIVED from the configured TTL, so a TTL change cannot silently re-open the window. (With
   `TTL=5s`, `renewInterval=2s` → 3s slack; a ~700ms per-attempt cap → 2.7s.)
3. **Stop-the-world on overdue renew.** The instant a renew is overdue (it did not conclusively
   succeed within the budget) the worker pauses `process`/`Deliver` IMMEDIATELY — before the moment a
   standby could re-acquire the lease — not after a fixed number of failed attempts. The worker stops
   touching the data path the instant it can no longer prove it holds the lease.

A briefly slow-but-still-held lease costs at most a paused beat; correctness beats the availability
the perf commit chased. The concurrent fan-out (the other folded perf commit, `1f4309b`) is
orthogonal and preserved (ack-after-all, serial `MaxAckPending=1`).

## Consequences

- The projector readiness probe (RH-06) reflects this: a paused/lost-lease worker fails readiness and
  is taken out of rotation.
- HA replicas (RH-10) can run a warm standby safely because the fence guarantees the former holder
  has stopped before the standby goes live.
- Spec delta ids: `RDL-03` (renew budget + overdue pause), with `RDL-01`/`RDL-02` anchoring the
  concurrent fan-out the same review folds in.
