# Cross-review — shared-nats-authcallout-carveout (2026-06-15) — the F1 GATE

Builder: claude. Independent reviewer: **agy** (read-only; codex flaked in this env — reduced
coverage noted). This is the gating RP-repo review of ADR-0004; no desk infra flip until clean.
Initial verdict: **BLOCKED** → all findings resolved.

| Sev | Finding | Resolution |
|-----|---------|------------|
| BLOCKER | reference `auth_users` listed only [router, authsvc] vs ADR's 6 identities | Added `projector` (its own identity, ADR §3) to the reference config + auth_users; clarified the reference is RP-STANDALONE and the SHARED-INFRA auth_users is the union assembled at the desk Helm merge (desk-rp/desk-api/connector-zalo are desk's, added at V2-01). |
| HIGH | migration applied accounts (S3) before callout (S4) — would drop anonymous browsers in the interim | ADR Migration now marks **S3+S4 as ONE atomic maintenance window** with an explicit "why" (browsers have no static-user interim; service identities validate under S3, callout lands same window). |
| MEDIUM | dropping `client` left `deploy.security.client-log-write-denied` orphaned | Added a `## REMOVED Requirements` spec delta formally retiring the scenario (obsolete — the user no longer exists). |

R1/R5 confirmed OK (rollback reverts service env AND NATS config; least-privilege carve-out; no anon/
bypass; visitor TTL cap + HA). Re-review recommended on the updated diff before the flip.

## Correction (2026-06-16) — BLOCKER & MEDIUM resolutions reverted

The initial resolutions to the BLOCKER and MEDIUM findings conflated the RP dev reference config with
the production shared-infra topology, and broke the SDK integration suite (`nats-server` aborted on an
unset `$PROJECTOR_PASSWORD`; the suite connects as `client`, which had been stripped):

- **BLOCKER (revised):** the reference `auth_users` does NOT need to mirror the 6-identity production
  topology. `deploy/nats/nats-server.conf` is the LOCAL dev/integration NATS — [router, authsvc,
  client] is correct for it; the production 6-identity carve-out (no `client`) lives in desk's Helm
  values (`shared-infra-authcallout-values.yaml`). ADR-0004 §3 now states this scope explicitly.
  `projector` was REMOVED from the reference config (it was never a dev-suite identity).
- **MEDIUM (withdrawn):** `client` is NOT dropped from the reference config, so
  `deploy.security.client-log-write-denied` is NOT orphaned. The `## REMOVED Requirements` delta was
  deleted; the `@spec:` annotation stays live on the retained `client` deny rule.

The HIGH (S3+S4 atomic window) resolution stands unchanged. Net effect: `nats-server.conf` is back at
`origin/main`; the carve-out is now doc-only (ADR + proposal + design + tasks). Verdict after
correction: gate intact, production design unchanged.
