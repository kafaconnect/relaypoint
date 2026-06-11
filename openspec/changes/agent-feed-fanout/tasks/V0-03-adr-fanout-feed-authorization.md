---
id: V0-03
slice: V0
title: ADR — per-agent fan-out feed as the inbox authorization model
status: done
specs: []
---

Author `docs/architecture/decisions/0003-agent-fanout-feed.md` recording the decision to make
the agent inbox read through a per-agent fan-out feed (server-checked participation) instead of
direct per-interaction `.log` subscribe or a tenant-wide read grant. It changes the
authorization architecture signaling-core's auth-callout section established (per-interaction
grant → per-agent feed grant), so an ADR is required (DoD). Capture: context (multi-thread inbox
vs per-interaction grant), the research baseline, the decision, the participation-source open
question, and consequences (new fan-out service, desk rework). Reference signaling-core and the
spec delta ids.

## Log
- 2026-06-11 done: authored `docs/architecture/decisions/0003-agent-fanout-feed.md` (Accepted) as
  part of the cross-review remediation — pins all 8 decisions (feed grant, participation source A
  via privileged command→fact, wildcard `.cmd` + server-side authz, sharded exactly-once fan-out,
  `_INBOX_<conn>` isolation, bounded history-read, revocation-epoch intervals, ephemeral feed);
  references signaling-core + the 24 spec delta ids + the desk rework.
