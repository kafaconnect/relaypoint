---
id: SBI-RP-03
slice: SBI-RP
title: Authorize opaque subscribers by capability only
status: in_progress
specs: [substrate.gate-authenticated-subscriber, substrate.token-capability-not-role, substrate.no-domain-branch-static-review]
---

## Goal

Remove `RoleAgent` as a RelayPoint authorization input while preserving visitor and trusted-backend
protocols. Grant only exact known subscriber capabilities and keep the current `.agent.` subject as
a legacy wire namespace.

## Success criteria

- `// @spec:substrate.token-capability-not-role` proves a role-only or ambiguous token fails closed.
- `// @spec:substrate.gate-authenticated-subscriber` proves a capability-only opaque subscriber gets
  its tenant-pinned grant and participation-gated command path.
- `// @spec:substrate.no-domain-branch-static-review` proves subscriber authorization contains no
  Desk role or routing-policy branch.
- HMAC development tokens use the same capability predicate.
- `GOWORK=off go build ./...`, vet, race, integration, OpenSpec strict, and cross-review pass.

## Log

- 2026-07-31: started after the recorded cross-repo design discussion.
