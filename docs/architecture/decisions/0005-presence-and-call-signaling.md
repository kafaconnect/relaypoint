# ADR-0005: RelayPoint presence service + WebRTC call signaling

- Status: Proposed
- Date: 2026-06-17
- Scope: RelayPoint's presence service and the WebRTC call-signaling protocol (M2 R1, the design that
  unblocks desk M2 R2 web-call). Relates to: ADR-0003 (per-agent feed), ADR-0004 (sole auth-callout
  responder), desk ADR-0010 (the transitional agent-console bare-NATS presence grant this retires) and
  desk ADR-0012 §7 (widget transport). RelayPoint is BOTH desk's realtime layer AND a standalone
  library — this design stays **Desk-agnostic** (opaque context, no customer/routing assumptions).
  Synthesised from two independent design passes (codex + agy).

## Context

R2 (web-call) needs presence ("which agent is online to ring?") and a low-latency WebRTC signaling
path (offer/answer/ICE). Two constraints shape the design: (1) the durable interaction `.log`
(JetStream) must NOT carry high-volume ephemeral ICE candidates, which are useless to replay and would
bloat the log; (2) the current desk agent-console publishes presence directly to bare NATS under a
**transitional** ADR-0010 grant (`tenant.<tid>.presence.<self>.>`) — a client-publish carve-out that
violates router authority and must retire when this ships.

## Decision

### 1. Presence — service-published, router/connection-authoritative
A presence service derives liveness from the NATS connection lifecycle (the auth-callout-minted
identity per ADR-0004) plus a lightweight client heartbeat; clients SUBSCRIBE only, never publish
presence. Authority: the RP router owns room membership (from `.log` participation facts); the
presence service owns liveness.

- Subjects (service-published, client subscribe-only):
  - `tenant.<tid>.presence.user.<uid>.state`
  - `tenant.<tid>.presence.room.<room_id>.state`  (`room_id` is opaque — Desk maps it to a conversation)
- Heartbeat/TTL: client heartbeat every ~15s; a user is `online` while it has ≥1 live connection;
  marked stale after ~45s without heartbeat; a ~5s disconnect debounce absorbs reconnects. Multi-device
  = liveness is the OR over a user's connections (`device_count`).
- Payload: a new `proto/relaypoint/presence/v1/presence.proto` (`PresenceState{tenant_id,user_id,
  status,device_count,observed_at,expires_at}`, `RoomPresenceState{tenant_id,room_id,online_user_ids,
  observed_at,expires_at}`).
- **Grant migration:** remove the agent presence-publish grant `tenant.<tid>.presence.<self>.>` from
  the auth-callout grants (`internal/authcallout/grants.go`). This revokes the transitional desk
  ADR-0010 carve-out; the desk console switches to heartbeat + subscribe.

### 2. Call signaling — durable lifecycle facts, ephemeral media negotiation
Split the two concerns (resolving the codex/agy difference):

- **Durable call LIFECYCLE facts on the existing `.log`** (`tenant.<tid>.interaction.<iid>.cmd.<self>`
  → `…log`): `call.invite`, `call.ringing`, `call.answered`, `call.declined`, `call.cancelled`,
  `call.timed_out`, `call.connected`, `call.ended`, `call.failed`. These ARE the `call`-interaction
  timeline entry + state, recoverable across reconnect — they go through the router's existing
  command→fact path (gates, OCC, idempotency) unchanged.
- **Ephemeral WebRTC media negotiation on a new `.signal` subject** (NOT durable, NO JetStream):
  `tenant.<tid>.interaction.<iid>.signal.<uid>` carrying `webrtc.offer`, `webrtc.answer`,
  `webrtc.renegotiation.*`, and `webrtc.ice`. ICE/SDP are high-volume and replay-useless; a reconnect
  renegotiates. `.signal` grants are **call-scoped**: publish only `.signal.<self>`, subscribe only the
  interaction's `.signal.*`.
- **Ring** (route an inbound call to a callee before an interaction is joined):
  `tenant.<tid>.routing.offer.user.<callee_uid>` (+ `.control` for terminal/cancel).

State machine: `idle → ringing → (answered | declined | cancelled | timed_out)`; then
`answered → connecting → active → (ended | failed)`. Aligns with the existing state-machine doc.

### 3. One active call per party
The router enforces it via a `CallOccupancy` port backed by **NATS KV** (CAS reserve of caller+callee
keys `tenant.<tid>.user.<uid>` on `call.invite`; promote on connect; release on any terminal). NATS KV
(not in-memory) so it is correct across router replicas. An invite targeting an occupied party gets a
`busy` terminal.

### 4. Isolation
The auth-callout-minted identity's tenant MUST equal the subject tenant; a command/signal suffix MUST
equal the actor user id (forged-author rejected). Tenant isolation is the same boundary ADR-0004
established; call adds no cross-tenant path.

## Consequences

- New: `cmd/presence`, `internal/presence`, `proto/relaypoint/presence/v1`, the call payload types in
  `proto/relaypoint/interaction/v1`, a `.signal` subject space + call-scoped grants, the `CallOccupancy`
  KV port, and the grant migration. Desk-specific concepts stay OUT (opaque `room_id`/context).
- The `.log` stays clean of ICE/SDP (durable = lifecycle facts only).
- **Compat (pre-1.0):** proto/subject additions are additive, but revoking client-published presence is
  a BREAKING change for the transitional desk console → ship as a minor pre-1.0 (`v0.5.0`) with a
  migration note; post-1.0 it would be a major.
- **Desk migration path:** deploy the presence service + call grants → desk console stops publishing
  old presence (heartbeat + subscribe) → remove the ADR-0010 grant → desk widget lazy-loads the WebRTC
  client only when a call starts (protects the ADR-0012 §7 widget IIFE size).

## Alternatives (rejected)

- **Client-published presence** (status quo bare-NATS grant): violates router authority + complicates
  multi-device merge; rejected.
- **All-ephemeral call protocol** (agy's first pass): no durable record → no `call` timeline entry and
  no recovery of call state; rejected in favour of the durable-lifecycle / ephemeral-media split.
- **SDP on the durable `.log`** (codex's first pass): SDP is large and per-negotiation; only the
  lifecycle facts belong on the timeline, so SDP rides ephemeral `.signal` with ICE.
- **In-memory occupancy**: wrong under router replicas; use NATS KV CAS.
- **Durable presence**: presence is liveness, not history; durable storage adds no value.
