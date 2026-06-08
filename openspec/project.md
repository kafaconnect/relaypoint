# RelayPoint — NATS Web Signaling Backbone

## Purpose

RelayPoint is a real-time **signaling backbone** for web apps. One NATS cluster carries
**four channels — chat, presence, voice/video signaling, notification** — and doubles
as the microservices fabric. **Web-first**; mobile (MQTT) and the HA cluster are
deliberately deferred to later phases.

It is the **signaling service for [KafaConnect Desk](https://github.com/kafaconnect/desk)**
(consumed there as a submodule), but is built as a standalone, reusable backbone — not
Desk-specific. Hard rule: dependencies are **100% open source**; the project itself is
**source-available** — free for internal use, with a commercial license required to ship it
inside a product sold to third parties (see `LICENSE`).

## Scope (this service owns / does NOT own)

- **Owns:** the signaling/control plane — chat transport, presence, WebRTC **SDP/ICE
  exchange**, and notifications, all over NATS subjects; the presence service; auth at
  the NATS edge.
- **Does NOT own:** the **media plane** — audio/video never touches NATS; WebRTC media
  flows **peer-to-peer** (relayed by coturn only when NAT requires it). A server-side
  SFU/media-server is a later concern.

## Tech Stack

- **Messaging core:** NATS server (single node, JetStream enabled).
- **Client transport:** WebSocket (NATS native `websocket{}`).
- **Web client:** `nats.ws` (NATS' official browser WebSocket client).
- **MQTT bridge:** NATS `mqtt{}` (MQTT 3.1.1) — configured now, unused until the mobile phase.
- **Media:** WebRTC peer-to-peer; **never** through NATS.
- **NAT traversal:** coturn (STUN/TURN).
- **Presence service:** a backend (Go or TS) subscribing to `$SYS` connection events.
- **Auth (Phase 1):** user/pass or token; NKEY/JWT later.
- **Observability:** nats-surveyor / prometheus-nats-exporter → Prometheus → Grafana.

## Architecture Decisions (locked)

- NATS is the **single backbone** for both client signaling and microservices (NATS `micro`).
- **Router-authoritative.** A stateful **router/interaction service** is the trusted-server
  writer of every `interaction.<id>.log` fact and owns the offer/call/interaction state machines.
  Clients are READ-only on `.log` and publish intents as COMMANDS on `interaction.<id>.cmd`;
  high-rate ICE/typing ride an ephemeral per-publisher `interaction.<id>.signal.<userId>`.
- **Unified Interaction.** One interaction per conversation across all media; the **medium is a
  payload field**, never a subject (no `call.invite` / `chat.offer`).
- **Media never touches NATS** — only the (opaque) media-negotiation descriptor + ICE transit it;
  audio/video go WebRTC-direct (coturn on NAT). The descriptor is an opaque blob tagged by
  `media_profile` (Phase-1: `webrtc-p2p`); the WebRTC SDP/ICE/glare choreography is that profile,
  owned by the client **SDK** (`MediaAdapter` port) — a vendor/SFU adapter is a future profile.
- **Subject ↔ topic auto-map** (`a.b.c` ↔ `a/b/c`) keeps the future MQTT-mobile bridge rework-free.
- **Presence is derived** from `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` — not MQTT LWT.
- **Single node now;** a 3-node JetStream RAFT cluster is the HA path, deferred.

## Conventions

- **Subjects:** dot-separated, lowercase, prefixed `tenant.<tenantId>.`; ids are ULID/UUID.
- **Ephemeral** (ICE, typing, presence) on **core NATS** (at-most-once); **durable** facts
  (`.log`) and notifications on **JetStream**.
- **Subject layout** (unified-interaction, router-authoritative):
  - interaction: `interaction.<id>.log` (JetStream, router-written), `interaction.<id>.cmd`
    (client intents, write-only), `interaction.<id>.signal.<userId>` (core NATS, ephemeral, ICE/typing)
  - routing: `routing.offer.user.<userId>` (request/reply + `_INBOX`), `routing.offer.user.<userId>.control`
    (terminal push), `routing.audit.>` (JetStream)
  - presence: `presence.<userId>` (presence service is the sole publisher)
  - notification: `notify.<userId>` (JetStream)

## Phases

- **Phase 1 (now):** single-node NATS (JetStream + WebSocket + MQTT listeners), `nats.ws` web
  client, presence service via `$SYS`, coturn, auth user/pass or token, the 4 subject groups.
- **Later:** mobile via the MQTT bridge; NKEY/JWT auth; 3-node JetStream RAFT HA cluster; SFU/media-server.

## Current OpenSpec state

- No shipped capabilities yet. Active change: **`signaling-core`** — the Phase-1 backbone.
- Planned change: **`client-sdk`** — `@relaypoint/client` (TS browser) + `relaypoint-go` (Go
  server) SDKs over the `signaling-core` contract; `MediaAdapter` + `MediaCredentialProvider`
  ports; design-first (implementation follows a buildable server).
- See `docs/architecture/` once authored; ADRs in `docs/architecture/decisions/`.
