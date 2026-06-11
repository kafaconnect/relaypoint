---
id: V0-02
slice: V0
title: Spec delta — feed read surface, fan-out projection, backfill, revocation (stable ids)
status: done
specs:
  - signaling.feed.inbox-reads-own-feed-only
  - signaling.feed.cross-agent-denied
  - signaling.feed.unified-medium
  - signaling.feed.write-server-only
  - signaling.feed.cmd-wildcard-no-reconnect
  - signaling.feed.cmd-nonparticipant-denied
  - signaling.feed.cmd-identity-pinned
  - signaling.feed.privileged-assign-to-fact
  - signaling.feed.privileged-actor-guarded
  - signaling.feed.fanout-to-participants
  - signaling.feed.participation-from-facts
  - signaling.feed.fanout-dedup
  - signaling.feed.core-port-isolated
  - signaling.feed.exactly-once-crash
  - signaling.feed.shard-ownership
  - signaling.feed.poison-dlq
  - signaling.feed.inbox-prefix-isolated
  - signaling.feed.backfill-on-assignment
  - signaling.feed.history-participation-checked
  - signaling.feed.cursor-resume
  - signaling.feed.revoke-future-facts
  - signaling.feed.revoke-cancels-backfill
  - signaling.feed.transfer-no-gap
  - signaling.feed.ephemeral-bridge
  - signaling.feed.revoke-tombstone
---

Author `specs/signaling-core/spec.md` ADDED requirements with `#### Scenario:` blocks carrying
stable `id:`s: per-agent feed as the inbox read surface (no direct `.log`, own-feed only,
unified medium, server-only write); fan-out projection by server-checked participation
(participation-from-facts, dedup, loose-coupled core); history backfill (browser stays off
`.log`); revocation (future facts stop, transfer no-gap, retained-feed policy). Reuse the
signaling-core envelope verbatim (feed copies the source fact). `openspec validate
agent-feed-fanout --strict` must pass.

## Log
- 2026-06-11 done: 4 ADDED requirements, 13 scenarios with stable ids; validate --strict green.
- 2026-06-11 remediation: reworked to a pinned auth boundary (cross-review BLOCKED). Now 10 ADDED
  requirements / 24 scenarios: wildcard `.cmd` + server-side participant authz (no write
  reconnect), privileged participation-command→fact contract (source A), exactly-once/sharded/HA
  fan-out (crash, DLQ, shard rebalance), per-connection `_INBOX_<conn>` isolation, bounded
  history-read backfill, revocation-epoch intervals, ephemeral feed + tombstone. validate
  --strict green.
- 2026-06-11 architect remediation (Fable 5, owner-approved): (1) write identity = ACL-pinned
  `.cmd.<self>` subject suffix (replaces unimplementable subject-mapping; mirrors `.signal.<self>`)
  — router takes identity from last subject token, `actor_mismatch` on payload disagreement; added
  scenario `signaling.feed.cmd-identity-pinned`. (2) projector = leased single-active worker + KV
  snapshot hydration (effectively-once, not exactly-once); sharding demoted to a scale-out
  appendix; rewrote `exactly-once-crash` + `shard-ownership` scenarios. validate --strict green.
