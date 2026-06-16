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
- 2026-06-15 done: agy gate review (BLOCKED) folded — made S3+S4 one atomic window in the ADR.
  Record: reviews/cross-review-20260615.md. (codex flaked; single-reviewer coverage noted. The live
  flip remains the desk story's gated step.)
- 2026-06-16 correction: the review's BLOCKER (add projector to the reference auth_users) and MEDIUM
  (retire client-log-write-denied) resolutions conflated the dev reference config with the production
  topology and broke the integration suite — reverted. `nats-server.conf` is back at origin/main
  ([router, authsvc, client]); the production 6-identity carve-out (no client) is scoped to desk's
  Helm values in ADR-0004 §3. See the "Correction" section in reviews/cross-review-20260615.md.
