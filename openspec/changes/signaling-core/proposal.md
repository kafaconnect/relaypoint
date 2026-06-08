# Change: signaling-core

## From

An empty repository.

## To

A Phase-1 single-node NATS signaling backbone built on a **unified Interaction** model
(WebexCC-style) with a **router-authoritative** command plane. A stateful
**router/interaction service** is the single authoritative writer of every
`interaction.<id>.log` fact and owns the offer/call/interaction state machines, `sequence`
assignment, RONA/timers, the NATS-KV offer store, the orphaned reaper, and auth-callout
grant/revoke. Clients are READ-only on `.log` and publish intents as COMMANDS on
`interaction.<id>.cmd`; high-rate transient signals (ICE/typing) ride an ephemeral,
rate-limited `interaction.<id>.signal` (core NATS, never JetStream). The design is
**lifecycle-complete**: the full offer lifecycle (cancel/withdraw/accepted_elsewhere/
fast-RONA/expiry, reconnect-during-ring), the 1:1 WebRTC call lifecycle (glare/perfect-
negotiation, ICE-buffered-until-SDP, renegotiation/ICE-restart, hold/resume, cold/warm
transfer, setup-cancel, media-failure), the interaction state machine (abandon/withdraw,
orphaned reaper, offline-vs-left), delivery/ordering/idempotency (router `sequence`,
`Nats-Msg-Id` dedup, `ref_id`, gap-replay), and failure modes (token-expiry/max connection
lifetime, presence debounce, RONA penalty-box, router-crash recovery via KV + sweeper).
Presence (`$SYS`-derived, published only by the presence service) and durable notifications
are separate trees. Media never touches NATS; coturn handles NAT traversal.

## Reason

The prior version was a subject sketch that mapped the unified-interaction + QoS-split model
but missed the full lifecycle: cancel/withdraw, the offer/call/interaction races and state
machines, and router-authoritative security. A 3-way deep lifecycle audit converged on a
**router-authoritative** correction — clients must not forge `.log` facts or assign
`sequence`, and per-interaction races (double-accept, glare, gap/replay, crash recovery) must
be owned by a single stateful writer. This change makes the Phase-1 contract lifecycle-complete
and safe to build against, while still mapping an interaction 1:1 to a Desk
Session/CustomerTimeline and keeping high-rate ICE off JetStream.

## Impact

- New container: a **router/interaction service** (the only writer of `.log`; state-machine
  owner; offer/timer engine; KV offer store; orphaned reaper; auth-callout authorizer).
- New/changed subjects: `interaction.<id>.cmd` (client commands), router-only
  `interaction.<id>.log`, `routing.offer.user.<userId>.control` (terminal push),
  `routing.audit.>` (privileged-control audit); `.signal` rate-limited.
- NATS config: JetStream, `websocket{}`, `mqtt{}`, `$SYS`, auth callout, tenant-scoped ACLs
  (clients read-only on `.log`, presence subscribe-only), KV bucket for offer state.
- Establishes the subject contract, the router-authoritative security boundary, and the
  "media never touches NATS" rule.
- Deferred to later changes: multi-party/conference, mobile (MQTT bridge), NKEY/JWT auth,
  3-node JetStream RAFT HA cluster + router HA, SFU/media-server.

## Non-goals

- **1:1 calls only** — multi-party / conference is explicitly deferred.
- No mobile client yet (the MQTT listener is configured but unused).
- No HA: single-node NATS and a single router instance; 3-node RAFT cluster + router HA deferred.
- No SFU/media-server; no NKEY/JWT auth in Phase 1.
