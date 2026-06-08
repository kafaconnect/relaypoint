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
- **Media never touches NATS** — only SDP/ICE transit it; audio/video go WebRTC-direct (SFU later).
- **Subject ↔ topic auto-map** (`a.b.c` ↔ `a/b/c`) keeps the future MQTT-mobile bridge rework-free.
- **Presence is derived** from `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` — not MQTT LWT.
- **Single node now;** a 3-node JetStream RAFT cluster is the HA path, deferred.

## Conventions

- **Subjects:** dot-separated, lowercase.
- **Ephemeral** (ICE, typing, presence ping) on **core NATS** (at-most-once).
- **Durable** (notification) on **JetStream**.
- **Subject layout:**
  - chat: `chat.dm.<userA>.<userB>`, `chat.room.<roomId>`, typing `chat.room.<roomId>.typing`
  - presence: `presence.<userId>`
  - call: invite `call.invite.<calleeId>` (request/reply), ICE `call.<callId>.ice.<fromUserId>`, control `call.<callId>.control`
  - notification: `notify.<userId>` (JetStream)

## Phases

- **Phase 1 (now):** single-node NATS (JetStream + WebSocket + MQTT listeners), `nats.ws` web
  client, presence service via `$SYS`, coturn, auth user/pass or token, the 4 subject groups.
- **Later:** mobile via the MQTT bridge; NKEY/JWT auth; 3-node JetStream RAFT HA cluster; SFU/media-server.

## Current OpenSpec state

- No shipped capabilities yet. Active change: **`signaling-core`** — the Phase-1 backbone.
- See `docs/architecture/` once authored; ADRs in `docs/architecture/decisions/`.
