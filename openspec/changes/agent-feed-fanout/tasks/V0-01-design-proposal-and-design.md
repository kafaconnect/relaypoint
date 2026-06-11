---
id: V0-01
slice: V0
title: Author proposal + design (research-backed fan-out feed; participation-source crux flagged)
status: done
specs: []
---

Write `proposal.md` (why per-interaction grant doesn't scale to the multi-thread inbox; the
decided per-agent feed model; impact; open questions) and `design.md` (research baseline —
Matrix `/sync`, Slack Socket Mode, Stream Chat, Twilio `UserConversation`; the 6 decisions:
feed subject + auth grant, participation/fan-out service with the participation-source crux
flagged, projection copy-vs-pointer + ordering/dedup, history backfill, revocation, tenant
isolation). Align to signaling-core (do not supersede). Surface every owed decision in an Open
Questions section. DESIGN ONLY — no application code.

## Log
- 2026-06-11 done: proposal.md + design.md authored; participation-source flagged as the crux
  open question (recommend `.log`-only with Desk-as-commander); desk `rp1-web-consumer-auth`
  rework noted as a dependent follow-up.
