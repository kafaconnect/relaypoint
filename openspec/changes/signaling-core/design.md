# Design: signaling-core

> The full architecture narrative + diagrams live in `docs/architecture/` (HTML). This
> file records the decisions; it does not duplicate the diagrams.

## Phase-1 scope (locked)

Web-first; calls are **1:1 only** (conference/multi-party deferred); **single-node** NATS
(HA/RAFT deferred). Hard invariant: **media NEVER touches NATS** — only SDP/ICE transit it.

## Key decisions (locked, 3-way consensus from a deep lifecycle audit)

- **Router-authoritative.** A stateful **router/interaction service** is the SINGLE
  authoritative writer of every `interaction.<id>.log` fact and the owner of the offer,
  call, and interaction state machines, the `sequence` counter, RONA/timers, the KV offer
  store, the orphaned reaper, and auth-callout grant/revoke. It is the ONLY component with
  write access to `.log`.
- **Command plane vs `.log`.** Clients are READ-only on `.log` and publish intents as
  COMMANDS on `interaction.<id>.cmd`. The router validates (tenant, actor role,
  state-machine legality, `author == connection identity`, payload `tenant_id` == subject
  tenant), assigns `sequence`, and appends the authoritative fact. Forged authorship and
  illegal transitions are rejected.
- **Unified Interaction.** One interaction per conversation across all media; the medium is
  a payload field, never a subject. No `call.invite` / `chat.offer`.
- **Split by QoS, not by domain.** Per interaction: `.log` (JetStream, durable, ordered)
  for facts, `.cmd` (core NATS, write-only) for client intents, per-publisher
  `.signal.<userId>` (core NATS, ephemeral, rate-limited; author is in the subject, sub `*`)
  for ICE/typing. Never `.signal` on JetStream.
- **Media off NATS.** SDP→`.log`, ICE→`.signal`; A/V are WebRTC P2P (coturn only on NAT).

## Subjects (prefix `tenant.<tenantId>.`)

| Purpose | Subject | Transport | Who writes |
|---|---|---|---|
| Interaction commands (client intents) | `interaction.<id>.cmd` | core NATS | clients (write-only) |
| Interaction log (durable facts) | `interaction.<id>.log` | JetStream | **router only** |
| Interaction signal (ephemeral, per-publisher) | `interaction.<id>.signal.<userId>` (sub `*`) | core NATS (rate-limited) | participants (own `<userId>` only; author in subject) |
| Offer ring (request/reply) | `routing.offer.user.<userId>` (+ team/queue) | core NATS req/reply, reply via `_INBOX`+nonce | router |
| Offer control (terminal push) | `routing.offer.user.<userId>.control` | core NATS | router only |
| Presence | `presence.<userId>` | core NATS | **presence service only** |
| Notification | `notify.<userId>` | JetStream | services |
| Routing audit | `routing.audit.>` | JetStream | router only |

- `.log` event_types: `interaction.started/assigned/transferred/abandoned/ended`,
  `message.created/updated/deleted` (carry `ref_id`), `participant.joined/offline/left`,
  `webrtc.offer/answer`, `webrtc.renegotiation.offer/answer`,
  `call.answered/connected/held/resumed/cancelled/ended/media_failed/setup_failed`,
  `recording.started/stopped`.
- `.signal.<userId>` event_types: `webrtc.ice`, `typing.*`, `presence.cursor`, `media.level`.

## JetStream streams (by leaf class, not per-interaction)

- `INTERACTION_LOGS` ← `tenant.*.interaction.*.log` (file storage; per-interaction ordering;
  `Nats-Msg-Id = event_id` dedup)
- `NOTIFICATIONS` ← `tenant.*.notify.*`
- `ROUTING_AUDIT` ← `tenant.*.routing.audit.>` (privileged-control audit; not live offer delivery)
- NO stream for `.cmd`, `.signal`, `presence.*`, or live offer/control subjects.

## State machines (compact)

- **Offer:** `offered → ringing → { accepted | rejected | cancelled | withdrawn |
  accepted_elsewhere | timed_out_rona | expired | no_responder_fast_rona }`. NATS
  request/reply on an `_INBOX`: the SINGLE reply is the terminal `accept`/`reject`. `ringing`
  is implicit once the request is published with no `503 No Responders` — there is NO separate
  ringing reply (it would consume the inbox). Non-reply terminals
  (cancel/withdraw/accepted_elsewhere) are pushed on `...control`, NOT via the reply.
  Double-accept = CAS on `offer_id`/`route_attempt_id` (first wins, losers
  `accepted_elsewhere`). NATS `503 No Responders` → `no_responder_fast_rona` immediately.
- **Call (1:1):** `idle → setup_offered → answered → ice_connecting → connected →
  { renegotiating | held | transferring | reconnecting } → connected`; terminal
  `{ cancelled | ended | media_failed | setup_failed }`. Glare = perfect-negotiation
  (caller=impolite, else lexical userId tie-break): the **polite** peer rolls back its own
  offer and accepts the incoming one; the **impolite** peer ignores the colliding incoming
  offer and keeps its own. ICE buffered until SDP applied.
  Renegotiation carries `negotiation_id`/generation; stale (lower-gen) discarded.
- **Interaction:** explicit machine; invalid transitions rejected (no resume after `ended`).
  `interaction.abandoned` → router withdraws ringing offers. Reaper: all participants
  offline > N min → `interaction.ended[orphaned]`. `participant.offline` (transient) vs
  `participant.left` (permanent).

## Offer state in KV

The router persists active offers in NATS KV `offer.active.<userId>` with TTLs, one entry
**per fanned-out user**: a team/queue offer writes `offer.active.<userId>` for EACH user it
fans the offer out to (there is no team-level KV the client reads). A reconnecting client
reconstructs only its own pending offers from `offer.active.<self>`; a client-local ring
timeout is the backstop. On accept/withdraw/terminal the router clears those per-user KV
entries. A TTL sweeper + idempotent terminal transitions survive router crash (no
stuck-ringing / no phantom assignment).

## Transports, presence, auth

- Client: `nats.ws` over the `websocket{}` listener. `mqtt{}` (3.1.1) configured, unused
  until the mobile phase; `a.b.c ↔ a/b/c` auto-map keeps that bridge rework-free.
- Presence service subscribes `$SYS.ACCOUNT.*.{CONNECT,DISCONNECT}` under the **SYS**
  account (read) and publishes `presence.<userId>` on the **APP** account over a separate
  connection (two connections: SYS read, APP write). It debounces ~5s before broadcasting
  disconnect and tracks session/device counts to avoid false RONA on flap. Clients are
  subscribe-only on presence.

### Authorization (auth callout)

Static permissions cannot express per-interaction grants, so Phase-1 runs a **NATS
authorization service via the server's auth callout** (`authorization { auth_callout {...} }`).
The callout authenticates each connection's short-lived, nonce/audience-bound,
actor+interaction-scoped token and mints that connection's subjects — initially only the
user's own `routing.offer.user.<self>(.control)`, `notify.<self>`, `presence.<self>` (read),
and `interaction.<accepted>.cmd` + `.signal.<self>` write, `.signal.*` + `.log` read for
interactions already accepted. It holds NO blanket `interaction.*` and NO `.log` write.

Because the callout authorizes at **CONNECT time**, an existing connection cannot be widened
in place. On **offer-accept** the router issues the client a short-lived interaction-scoped
token and the client **reconnects** (transparent over `nats.ws`); the callout mints the new
connection's subjects, now including ONLY `tenant.<tid>.interaction.<id>.>` for that one
interaction. A user authorized for interaction X cannot touch interaction Y.

### Token expiry / connection lifetime

The callout runs only at CONNECT, so token expiry mid-session is bounded by an enforced
**maximum NATS connection lifetime** (or a ledger that actively kills connections whose token
expired). The client refreshes its scoped token and reconnects. Privileged controls
(cancel/withdraw/transfer/ACL grant-revoke) are audited (actor + reason) on `routing.audit.>`.

## Client subscription discipline

Clients subscribe to specific subjects (`interaction.<id>.log`, `interaction.<id>.signal.*`,
`routing.offer.user.<userId>`, `routing.offer.user.<userId>.control`, `notify.<userId>`,
`presence.<userId>`) and filter by `event_type` locally — NOT the `interaction.<id>.>`
wildcard (avoids ICE flood). The WebRTC subsystem subscribes to ICE only when the user joins
the call, and buffers ICE until the matching SDP is applied.

## Desk mapping

`interaction` = Desk **Session**; `.log` events = **CustomerTimeline** facts; `.signal`
does not enter the timeline unless it causes a durable state change (re-emitted on `.log`).

## Out of scope (later phases / own changes)

Multi-party / **conference calls**; mobile via MQTT bridge; NKEY/JWT auth; **3-node
JetStream RAFT HA cluster** + router HA; SFU/media-server.
