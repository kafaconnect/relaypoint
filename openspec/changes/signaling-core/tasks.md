# Tasks: signaling-core

> Verifiable via config, code, tests, or CI. Issue numbers added once synced to the board.

## NATS server (single node)
- [ ] `nats-server.conf`: JetStream; `websocket{}`; `mqtt{}` (3.1.1); system account exporting `$SYS`
- [ ] Auth (Phase 1): user/pass or token; tenant-scoped subject ACLs
- [ ] `deploy/docker-compose.yml`: nats + coturn (+ surveyor/exporter)
- [ ] Web client connects via `nats.ws` — `// @spec:signaling.nats-ws-connect`
- [ ] MQTT listener accepts connections (mobile-ready, unused) — `// @spec:signaling.mqtt-listener-ready`

## Unified interaction + offer/routing
- [ ] Event envelope `{ schema, event_type, event_id, sequence, occurred_at, medium, data }`
- [ ] Chat + call share `interaction.<id>.log`/`.signal`; medium in payload — `// @spec:signaling.unified-interaction`
- [ ] Offer on `routing.offer.user.<userId>` (request/reply, timeout) — `// @spec:signaling.offer-accept`
- [ ] RONA on timeout/reject → requeue — `// @spec:signaling.offer-rona`

## Interaction QoS split
- [ ] `interaction.<id>.log` on JetStream, ordered, durable/replayable — `// @spec:signaling.log-durable`
- [ ] `interaction.<id>.signal` on core NATS, never JetStream (ICE/typing) — `// @spec:signaling.signal-ephemeral`
- [ ] Media stays WebRTC P2P; only SDP/ICE on NATS — `// @spec:signaling.media-bypass-broker`

## Presence + notify
- [ ] Presence service `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` → `presence.<userId>` — `// @spec:signaling.presence-from-sys`
- [ ] Durable `notify.<userId>` survives reconnect — `// @spec:signaling.notify-durable`

## Multi-tenant isolation
- [ ] Tenant-prefixed subjects + per-tenant ACLs; cross-tenant denied — `// @spec:signaling.tenant-isolation`
- [ ] `interaction.<id>.*` ACL granted only on offer-accept — `// @spec:signaling.acl-after-accept`

## JetStream streams
- [ ] `INTERACTION_LOGS` (`tenant.*.interaction.*.log`), `NOTIFICATIONS` (`tenant.*.notify.*`); none for `.signal`/presence/offer

## NAT traversal
- [ ] coturn (STUN/TURN) deployed; client uses it as ICE server

## Docs (HTML) + verification
- [ ] `docs/architecture/` C4 + subject-model docs (HTML, via docs-writer)
- [ ] `openspec validate signaling-core --strict`
- [ ] Tests for every scenario id; lint/typecheck/test green
- [ ] Independent cross-review recorded

## Deferred (own changes/ADRs)
- [ ] Mobile via MQTT bridge · NKEY/JWT auth · 3-node JetStream RAFT HA cluster · SFU/media-server
