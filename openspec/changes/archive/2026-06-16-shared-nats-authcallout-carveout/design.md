# Design — shared-nats-authcallout-carveout

See **ADR-0004** for the full decision. Key points:
- RP sole responder; `RP`+`SYS` accounts; callout scoped to `RP`.
- `auth_users` = least-privilege static identities (router, projector, authsvc, desk-rp, desk-api,
  connector-zalo). NO anonymous, NO shared `client` (a callout bypass — removed). This is the
  PRODUCTION shared `nats` Helm release (desk's `shared-infra-authcallout-values.yaml`); the RP repo's
  `deploy/nats/nats-server.conf` is the local dev/integration reference (SDK suite connects as
  `client`) and RETAINS `client` — it is never the shared prod bus.
- Visitor credential TTL capped at min(vis_.exp, RP ceiling); responder HA via `authsvc` queue
  (both shipped in `m1_5-f1-rp-visitor-ttl-cap`).
- No-lockout census (`/connz?auth=1&subs=1`) is the gate: auth_users ⊇ {live conns} ∪ {manifest
  clients}; provision creds before accounts; accounts+callout = one window for the browser bus.
- Rollback reverts service Deployments AND the NATS config together.
