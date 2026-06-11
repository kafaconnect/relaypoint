---
id: V2-02
slice: V2
title: Participation/Fan-out projector service (lease/snapshot/feed subjects)
status: todo
specs:
  - signaling.feed.fanout-to-participants
  - signaling.feed.fanout-dedup
  - signaling.feed.core-port-isolated
  - signaling.feed.exactly-once-crash
  - signaling.feed.shard-ownership
  - signaling.feed.serial-fold
  - signaling.feed.poison-dlq
  - signaling.feed.live-only-no-history
  - signaling.feed.cursor-resume
  - signaling.feed.revoke-future-facts
  - signaling.feed.transfer-no-gap
  - signaling.feed.ephemeral-bridge
  - signaling.feed.revoke-tombstone
  - signaling.feed.write-server-only
  - signaling.feed.unified-medium
---

DEFERRED (not built in V1). The leased single-active worker that tails `tenant.*.interaction.*.log`
(`MaxAckPending=1` serial fold), projects facts into `tenant.<tid>.agent.<aid>.feed.<iid>` for
currently-participating agents (interval-guarded, deterministic `Nats-Msg-Id` dedup, ack-after-publish),
hydrates from an acked-prefix KV snapshot on lease takeover (acquire → WAIT for in-flight settle →
read ack_floor + hydrate → go live), DLQs poison facts, and writes the `feed.revoked` feed-control
tombstone on revocation. REUSES the V1 `ParticipationView`/`FoldParticipation` for the read plane so
read+write planes share one fold. Core behind owned ports (`ParticipationView`, `FeedSink`, `Cursor`).

## Log
- 2026-06-11 todo: deferred from the V1 slice; depends on the fan-out service + ephemeral feed stream + NATS KV lease.
