# RelayPoint

**RelayPoint** — a NATS **web signaling backbone**. One NATS cluster carries chat,
presence, voice/video signaling, and notifications on a **unified Interaction** model, and
doubles as the microservices fabric. Web-first; mobile (MQTT) and HA are deferred.

It is the signaling service for [KafaConnect Desk](https://github.com/kafaconnect/desk)
(consumed there as a submodule), built standalone + reusable.

## Model (see `docs/architecture/`)

- **Unified Interaction** — the medium (chat/voice/video) is a payload field, never a subject.
- **Offer ≠ updates** — offers ring a target user on `routing.offer.user.<id>` (request/reply, timeout → RONA), separate from interaction updates.
- **QoS split** — per interaction: `interaction.<id>.log` (JetStream, durable, ordered) vs `interaction.<id>.signal` (core NATS, ephemeral: ICE/typing).
- **Media off NATS** — only SDP/ICE transit it; A/V are WebRTC peer-to-peer (coturn for NAT).
- All subjects tenant-prefixed (`tenant.<id>.…`).

## How we work

Spec-driven via [OpenSpec](https://github.com/Fission-AI/OpenSpec) (`openspec/`); active
change `signaling-core`. Architecture docs in `docs/architecture/` (HTML). Same workflow +
skills as KafaConnect Desk.

## Run (local, once implemented)

```bash
docker compose -f deploy/docker-compose.yml up -d   # nats (jetstream+ws+mqtt) + coturn
```

## License

**Source-available** — © 2026 luongdev. Free for internal use; shipping it inside a
commercial product sold to others requires a license (luongld514@gmail.com). See
[`LICENSE`](./LICENSE). Third-party dependencies are 100% open source.
