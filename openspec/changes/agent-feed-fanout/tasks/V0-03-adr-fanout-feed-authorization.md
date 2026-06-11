---
id: V0-03
slice: V0
title: ADR — per-agent fan-out feed as the inbox authorization model
status: todo
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

NOTE: this task is part of the design package; it is `todo` here because the ADR file is written
when the change is accepted (apply phase), not at proposal time. Listed so the DoD's "ADR added
if architecture changed" is tracked.

## Log
