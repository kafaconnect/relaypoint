# Delta for Signaling Core

All subjects are prefixed `tenant.<tenantId>.` (omitted below for brevity). Subjects are
dot-separated, lowercase; ids are ULID/UUID (no dots/slashes). Event envelope:
`{ schema, event_type, event_id, sequence, occurred_at, medium, data }`.

## ADDED Requirements

### Requirement: Single-node NATS backbone (JetStream + WebSocket + MQTT + $SYS)
The system MUST run one NATS server with JetStream enabled, the WebSocket and MQTT (3.1.1)
listeners active, and a system account exporting `$SYS` connection events.

#### Scenario: Web client connects over WebSocket
- **id:** `signaling.nats-ws-connect`
- **GIVEN** a NATS server with a `websocket{}` listener
- **WHEN** a `nats.ws` client connects from the browser
- **THEN** the connection succeeds and the client can publish and subscribe

#### Scenario: MQTT listener configured (mobile-ready, unused now)
- **id:** `signaling.mqtt-listener-ready`
- **WHEN** the server starts with `mqtt{}` (3.1.1) enabled
- **THEN** the MQTT listener accepts connections (no mobile client uses it yet)

### Requirement: Unified Interaction — medium is a payload property
The system MUST model every chat / voice / video / email / social exchange as one
**Interaction**. The **medium is a field in the event payload**, NEVER part of the subject.
There MUST be no medium-specific subjects (no `call.invite`, no `chat.offer`).

#### Scenario: A chat and a call share the same interaction subjects
- **id:** `signaling.unified-interaction`
- **GIVEN** an interaction that starts as chat and escalates to a voice call
- **WHEN** chat messages and call events are published
- **THEN** they use the same `interaction.<id>.log` / `.signal` subjects, differing only by `event_type`/`medium` in the payload

### Requirement: Offer/routing separated from interaction updates
The system MUST deliver an interaction **offer** to a target user (who is NOT yet in the
interaction) on a separate routing tree `routing.offer.user.<userId>` (also team/queue
variants) via **NATS request/reply** with an accept timeout. On timeout or reject the
router MUST apply **RONA** (redirect-on-no-answer) and requeue. Offer delivery MUST NOT
share a subject or stream with interaction updates.

#### Scenario: Offer accepted within timeout
- **id:** `signaling.offer-accept`
- **GIVEN** the router publishes a request to `routing.offer.user.<userId>` with `timeout_ms`
- **WHEN** the client replies `accept` before the timeout
- **THEN** the router grants the user ACL on `interaction.<id>.*` and returns the `interaction_id`

#### Scenario: No answer triggers RONA
- **id:** `signaling.offer-rona`
- **GIVEN** an offer with a timeout
- **WHEN** no valid `accept` arrives before the timeout (or the client replies `reject`)
- **THEN** the router requeues the interaction (RONA) and the user is not joined

### Requirement: Interaction events split by QoS (durable log vs ephemeral signal)
Per interaction the system MUST expose exactly two subjects: a **durable, ordered**
`interaction.<id>.log` (JetStream) for facts, and an **ephemeral** `interaction.<id>.signal`
(core NATS, NEVER JetStream) for high-rate transient signals. The business `event_type`
lives in the payload within each.

#### Scenario: Log events are durable and replayable
- **id:** `signaling.log-durable`
- **GIVEN** a `message.created` (or `call.accepted`, `webrtc.offer`, `interaction.ended`) on `interaction.<id>.log` while a participant is offline
- **WHEN** the participant reconnects and consumes the stream
- **THEN** the event is delivered in order (durable, not lost)

#### Scenario: Signal events never persist
- **id:** `signaling.signal-ephemeral`
- **WHEN** `webrtc.ice` / `typing` events are published on `interaction.<id>.signal`
- **THEN** they are delivered at-most-once on core NATS and are NOT stored by JetStream

### Requirement: Media stays off NATS
The system MUST carry only **SDP/ICE** over NATS (SDP offer/answer as durable state on
`.log`; ICE candidates ephemerally on `.signal`); audio/video MUST flow over WebRTC
peer-to-peer (relayed by coturn only when NAT requires it).

#### Scenario: Call media bypasses the broker
- **id:** `signaling.media-bypass-broker`
- **GIVEN** two browser tabs in an established call
- **WHEN** audio/video flows between them
- **THEN** no media packet passes through NATS (only SDP/ICE did, during setup)

### Requirement: Presence derived from `$SYS`
The presence service MUST derive presence from `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` (not
MQTT LWT) and publish per-user state on `presence.<userId>`.

#### Scenario: Presence updates on connect/disconnect
- **id:** `signaling.presence-from-sys`
- **WHEN** a user's connection opens or closes
- **THEN** the presence service publishes the new state on `presence.<userId>`

### Requirement: Durable notifications on JetStream
The system MUST deliver notifications durably via JetStream on `notify.<userId>`, so a
disconnected user receives them on reconnect.

#### Scenario: Notification survives a reconnect
- **id:** `signaling.notify-durable`
- **GIVEN** a notification published while the user is offline
- **WHEN** the user reconnects and consumes the stream
- **THEN** the notification is delivered (durable)

### Requirement: Multi-tenant subject isolation
Every subject MUST be tenant-prefixed and protected by per-tenant ACLs. A user MUST NOT
read or write another tenant's subjects, and MUST gain `interaction.<id>.*` access only
after accepting the offer for that interaction.

#### Scenario: Cross-tenant subscription denied
- **id:** `signaling.tenant-isolation`
- **WHEN** a user authenticated for tenant A subscribes to `tenant.<B>.interaction.*.>`
- **THEN** NATS denies the subscription

#### Scenario: No interaction access before assignment
- **id:** `signaling.acl-after-accept`
- **GIVEN** a user who has not accepted an interaction's offer
- **WHEN** they subscribe to that `interaction.<id>.log`
- **THEN** access is denied until the router grants ACL on accept
