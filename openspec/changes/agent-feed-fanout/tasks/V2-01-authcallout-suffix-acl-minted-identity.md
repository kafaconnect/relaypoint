---
id: V2-01
slice: V2
title: Auth-callout — suffix-ACL grants + per-connection minted identity (production precondition)
status: todo
specs:
  - signaling.feed.inbox-reads-own-feed-only
  - signaling.feed.cross-agent-denied
  - signaling.feed.cmd-identity-pinned
  - signaling.feed.inbox-prefix-isolated
---

DEFERRED (not built in V1). The hard production precondition that makes the V1 suffix advisory →
enforced: the auth-callout mints a per-connection AUTHENTICATED identity and pins the ACLs to it —
`publish tenant.<tid>.interaction.*.cmd.<self>` (FIXED `<self>`; `*.cmd.<other>` denied),
`subscribe tenant.<tid>.agent.<self>.feed.>`, a per-connection minted `_INBOX_<conn>.>` (deny broad
`_INBOX.>`), replacing the shared `client` dev user. Until then the dev `client` ACL allows
`…cmd.*` (any suffix) — NOT production authorship security (signaling-core Phase-1 posture).

## Log
- 2026-06-11 todo: deferred from the V1 slice; requires NATS auth-callout config (no Go-only unit test).
