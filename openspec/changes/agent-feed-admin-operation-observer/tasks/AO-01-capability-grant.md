---
id: AO-01
slice: AO
title: Add the signed admin-operation observer grant
status: in_progress
specs: [admin-operation.profile-verifies, admin-operation.grant-isolated, admin-operation.nats-enforced]
---

Implement the exact scalar profile as an additive agent grant, retaining it only from verified Desk
EdDSA tokens. Keep normal agents, visitors, trusted backends, and dev HMAC grammar unchanged.

Acceptance:
- Same-tenant operation-hint subscribe succeeds only for the exact combined profile.
- Existing agent grants remain intact.
- Non-admin, unknown-profile, cross-tenant, and browser publish fail closed.
- Exact subject and opaque account-id-only Desk AsyncAPI coordination are pinned in the spec.
- `GOWORK=off` build, vet, format, unit, integration, and strict OpenSpec validation pass.

## Log

- 2026-07-30 in_progress: failing verifier/grant/embedded-NATS tests added before implementation.

