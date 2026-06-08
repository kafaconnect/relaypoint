# Design: signaling-core

> The full architecture narrative + diagrams live in `docs/architecture/` (HTML). This
> file records the decisions; it does not duplicate the diagrams.

## Key decisions (locked, 3-way consensus + WebexCC-grounded)

- **Unified Interaction.** One interaction per conversation across all media; the medium is
  a payload field, never a subject. No `call.invite` / `chat.offer`.
- **Offer ≠ updates.** The offer/ring is a separate, target-addressed routing concern with
  timeout/RONA — never on the interaction update subjects.
- **Split by QoS, not by domain.** Per interaction: `…​.log` (JetStream, durable, ordered)
  vs `…​.signal` (core NATS, ephemeral). `event_type` lives in the payload within each.
- **Media off NATS.** SDP/ICE only over NATS (SDP→`.log`, ICE→`.signal`); A/V are WebRTC P2P.

## Subjects (prefix `tenant.<tenantId>.`)

| Purpose | Subject | Transport |
|---|---|---|
| Offer / routing | `routing.offer.user.<userId>` (+ team/queue) | core NATS, request/reply (timeout → RONA) |
| Interaction log (durable facts) | `interaction.<id>.log` | JetStream |
| Interaction signal (ephemeral) | `interaction.<id>.signal` | core NATS |
| Presence | `presence.<userId>` | core NATS / KV (from `$SYS`) |
| Notification | `notify.<userId>` | JetStream |

- `.log` event_types: `interaction.started/ended`, `message.created/updated/deleted`,
  `participant.joined/left`, `interaction.accepted/assigned/transferred/held/resumed`,
  `webrtc.offer/answer`, `call.accepted/hold/ended`, `recording.started/stopped`.
- `.signal` event_types: `webrtc.ice`, `typing.*`, `presence.cursor`, `media.level`.

## JetStream streams (by leaf class, not per-interaction)

- `INTERACTION_LOGS` ← `tenant.*.interaction.*.log` (file storage; per-interaction ordering)
- `NOTIFICATIONS` ← `tenant.*.notify.*`
- (optional) `ROUTING_AUDIT` ← `tenant.*.routing.audit.>` — audit only, not live offer delivery
- NO stream for `.signal`, `presence.*`, or live offer subjects.

## Transports, presence, auth

- Client: `nats.ws` over the `websocket{}` listener. `mqtt{}` (3.1.1) configured, unused
  until the mobile phase; `a.b.c ↔ a/b/c` auto-map keeps that bridge rework-free.
- Presence service subscribes `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` → `presence.<userId>`.
  It connects under the system (`SYS`) account to read `$SYS`, and publishes
  `presence.<userId>` on the `APP` account over a **separate APP connection** (two
  connections: one SYS for reads, one APP for writes).
- Auth (Phase 1): user/pass or token at the edge + subject ACLs (tenant scope; interaction
  ACL granted on offer-accept). NKEY/JWT deferred. See **Authorization** below.

### Authorization

Static permissions cannot safely express per-interaction grants: Phase-1 auth is just
user/pass or token, clients connect directly over `nats.ws`, and the set of interactions a
user may touch is decided at runtime by offer-accept. Granting tenant-wide
`interaction.*` would defeat the `acl-after-accept` requirement.

We therefore run a **NATS authorization service via the server's auth callout**
(`authorization { auth_callout { issuer/account/auth_users... } }`). The callout service
authenticates each connection's Phase-1 token and **mints that connection's allowed
subjects** — initially only the user's own `routing.offer.user.<self>`, `notify.<self>`,
`presence.<self>` (plus tenant-prefixed publish to offer replies). It holds NO
`interaction.*` permission.

On **offer-accept**, the router records the accept and the accepting user's authorization
is **updated/re-issued through the callout** to add ONLY
`tenant.<tid>.interaction.<id>.>` for that one interaction. A user authorized for
interaction X thus cannot subscribe to interaction Y. This keeps grants dynamic and
least-privilege under the Phase-1 token model; NKEY/JWT (and per-user signing keys) remain
deferred.

Because the auth callout authorizes at **CONNECT time**, an already-connected `nats.ws`
client cannot have its existing connection's permissions widened in place. So on accept the
router issues the client a short-lived **interaction-scoped token** and the client
**reconnects** (transparent over `nats.ws`); the callout re-authenticates that token and
mints the new connection's subjects, now including `tenant.<tid>.interaction.<id>.>`. The
interaction subtree is therefore authorized for the reconnected connection, not retrofitted
onto the old one.

## Client subscription discipline

Clients subscribe to specific subjects (`interaction.<id>.log`, `interaction.<id>.signal`,
`routing.offer.user.<userId>`, `notify.<userId>`, `presence.<userId>`) and filter by
`event_type` locally — NOT the `interaction.<id>.>` wildcard (avoids ICE flood). The WebRTC
subsystem subscribes to ICE only when the user joins the call.

## Desk mapping

`interaction` = Desk **Session**; `.log` events = **CustomerTimeline** facts; `.signal`
does not enter the timeline unless it causes a durable state change (re-emitted on `.log`).

## Out of scope (later phases / own changes)

Mobile via MQTT bridge; NKEY/JWT auth; 3-node JetStream RAFT HA cluster; SFU/media-server.
