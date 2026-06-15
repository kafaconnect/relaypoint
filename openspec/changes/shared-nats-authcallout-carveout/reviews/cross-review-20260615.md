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
