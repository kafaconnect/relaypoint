---
id: V0-02
slice: V0
title: drop the dev shared `client` user from the reference NATS config (callout bypass)
status: done
issue:
specs: []
---
## Log
- 2026-06-15 done: removed the `client` auth_users entry + its account user from deploy/nats/nats-server.conf
  (a static callout-bypass identity — T1 finding). Browsers/visitors mint per-connection via the responder.
