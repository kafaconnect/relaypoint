# Change: signaling-core

## From

An empty repository.

## To

A Phase-1 single-node NATS signaling backbone built on a **unified Interaction** model
(WebexCC-style): the medium (chat/voice/video/…) is a payload property, never a subject.
The design separates the **offer/routing** phase (target-addressed, request/reply, timeout
→ RONA) from **in-interaction updates**, and splits interaction updates by **QoS** into a
durable `interaction.<id>.log` (JetStream, ordered) and an ephemeral
`interaction.<id>.signal` (core NATS — ICE/typing). Presence (`$SYS`-derived) and durable
notifications are separate trees. Media never touches NATS; coturn handles NAT traversal.

## Reason

RelayPoint is the signaling service for KafaConnect Desk and a reusable web signaling
backbone. The unified-interaction + QoS-split model (validated 3 ways and against WebexCC)
maps an interaction 1:1 to a Desk Session/CustomerTimeline, isolates the critical
offer/ring signal from the high-volume update firehose, and keeps high-rate ICE off
JetStream — while letting `event_type` evolve in the payload behind stable subjects.

## Impact

- New service: NATS config, the offer/routing tree, the interaction log/signal subjects,
  presence service, durable notifications, coturn, multi-tenant subject ACLs.
- Establishes the subject contract + the "media never touches NATS" boundary.
- Mobile (MQTT bridge), NKEY/JWT auth, the 3-node JetStream RAFT HA cluster, and a
  server-side SFU/media-server are out of scope (later phases).

## Non-goals

- No mobile client yet (the MQTT listener is configured but unused).
- No HA cluster, no SFU/media-server, no NKEY/JWT auth in Phase 1.
