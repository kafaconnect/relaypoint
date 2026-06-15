---
id: V0-03
slice: V0
title: independent cross-review of the carve-out ADR (codex + agy) — the GATE
status: done
issue:
specs: []
---
Independent RP-repo cross-review of ADR-0004 + the carve-out. No desk infra flip until clean. Focus:
no-lockout census completeness, no anon/bypass identity, visitor TTL + HA, rollback reverts both
service env AND NATS config.

## Log
- 2026-06-15 done: agy gate review (BLOCKED) folded — added projector to the reference
  auth_users + clarified reference-vs-shared-infra; made S3+S4 one atomic window in the ADR; formally
  retired the obsoleted client-log-write-denied scenario. Record: reviews/cross-review-20260615.md.
  (codex flaked; single-reviewer coverage noted. The live flip remains the desk story's gated step.)
