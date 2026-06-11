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
  - signaling.feed.fanout-to-participants
  - signaling.feed.participation-from-facts
  - signaling.feed.fanout-dedup
  - signaling.feed.core-port-isolated
  - signaling.feed.backfill-on-assignment
  - signaling.feed.cursor-resume
  - signaling.feed.revoke-future-facts
  - signaling.feed.transfer-no-gap
  - signaling.feed.revoke-retention-policy
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
