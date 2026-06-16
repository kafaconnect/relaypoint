---
id: V0-02
slice: V0
title: production carve-out omits the shared `client` user (dev reference config retains it)
status: done
issue:
specs: []
---
## Log
- 2026-06-15 done: recorded the production `auth_users` topology in ADR-0004 §3 — service identities
  only (router/projector/authsvc/desk-rp/desk-api/connector-zalo), NO shared `client` bypass. The
  production values live in the desk repo (`deploy/nats/shared-infra-authcallout-values.yaml`, the
  canonical `m1_5-f1-shared-nats-authcallout` change), which omits `client`.
- 2026-06-16 correction: the RP repo's `deploy/nats/nats-server.conf` is the LOCAL dev/integration
  reference (the SDK integration suite boots it and connects as `client`), NOT the shared prod bus, so
  it RETAINS the dev `client` user + its `.log`-write deny (@spec:deploy.security.client-log-write-denied).
  The callout-bypass removal is a production-bus boundary only — reverted an earlier edit that had
  stripped `client` from the dev reference and broke the integration suite.
</content>
</invoke>
