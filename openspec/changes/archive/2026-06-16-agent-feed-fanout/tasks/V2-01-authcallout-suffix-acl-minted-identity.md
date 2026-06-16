---
id: V2-01
slice: V2
title: Auth-callout — suffix-ACL grants + per-connection minted identity (production precondition)
status: done
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
- 2026-06-11 done: built `internal/authcallout/` (pure `GrantsFor` policy + `Verifier` port/`HMACVerifier`
  dev impl + NATS `Responder` adapter) and `cmd/authcallout`; minted per-connection identity pins
  `…cmd.<self>` / `agent.<self>.feed.>` / `_INBOX_<conn>.>`, denies `cmd.<other>` / `.log` / feed-publish /
  broad `_INBOX.>`; trusted-backend gets privileged-cmd + service grants. `deploy/nats/nats-server.conf`
  enables `authorization{auth_callout{issuer,account:RP,auth_users:[router,authsvc,client]}}`, `system_account: SYS`
  kept; compose + Dockerfile + .env wired. Verified on EPHEMERAL Docker NATS: agent ACLs + desk ACLs +
  bad-token-denied enforced by real NATS; existing signaling integration suite still green against the
  auth_callout conf (client exempt via auth_users). Shared `rp-nats` (infra/nats, no auth today) NOT
  reloaded — cross-repo cutover, see report.
