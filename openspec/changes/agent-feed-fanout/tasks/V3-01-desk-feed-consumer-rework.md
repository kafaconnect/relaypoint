---
id: V3-01
slice: V3
title: Desk-side feed consumer rework (rp1-web-consumer-auth → per-agent feed)
status: todo
specs: []
---

DEFERRED (not built here, and NOT in this repo). The dependent desk follow-up: rework
`rp1-web-consumer-auth` to consume `tenant.<tid>.agent.<aid>.feed.>` (drop tenant-wide read + direct
`.log`), get conversation history from desk REST against Postgres (RelayPoint serves none), issue
assignment as the privileged participation command (`…cmd.<desk-svc-identity>`), publish
`…interaction.*.cmd.<self>` with the minted `_INBOX_<conn>` reply scope, and render from the SAME
projected `Event` envelope. Tracked on the DESK repo — this change does not edit desk.

## Log
- 2026-06-11 todo: dependent follow-up on the desk repo; recorded for traceability, not implemented here.
