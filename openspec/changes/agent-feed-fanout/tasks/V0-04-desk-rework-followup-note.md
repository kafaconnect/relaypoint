---
id: V0-04
slice: V0
title: Note the dependent desk rework (rp1-web-consumer-auth → per-agent feed)
status: done
specs: []
---

Record — in proposal Impact + design "Desk impact" — that the in-flight DESK change
`rp1-web-consumer-auth` (which assumed direct per-interaction `.log` subscribe + a desk-minted
tenant-wide read grant for the inbox) MUST be REWORKED to consume the per-agent feed
(`tenant.<tid>.agent.<aid>.feed.>`): drop the tenant-wide read grant, subscribe the one feed,
get conversation history from desk's own REST against Postgres (RelayPoint serves NO history),
render from the SAME projected `Event` envelope. This is a tracked dependent follow-up ON THE DESK
REPO — do NOT edit the desk repo from this change.

## Log
- 2026-06-11 done: desk rework captured in proposal Impact and design "Desk impact"; flagged as
  a dependent follow-up, desk repo not edited.
- 2026-06-11 final design round: history plane DROPPED from RelayPoint (owner decision) — desk
  rework note now reads "conversation history = desk REST against Postgres, RP serves none" (was
  "feed backfill / participation-checked history read"). Desk repo still not edited.
