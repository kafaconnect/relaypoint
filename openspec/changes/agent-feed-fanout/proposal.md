# Change: agent-feed-fanout

## Why

signaling-core authorizes an agent's browser by **direct per-interaction grant**: on
offer-accept the auth-callout mints a connection that may `subscribe`
`tenant.<tid>.interaction.<id>.>` (the canonical `.log` + `.signal`). That works for ONE
interaction at a time, but the agent **inbox** is inherently multi-interaction: an agent
watches many open chat threads (and a live call) at once. Re-deriving that surface as "N direct
`.log` subscriptions" forces one of two bad shapes:

- a **per-interaction reconnect storm** (every newly-assigned thread = a token refresh +
  reconnect to widen the callout grant), or
- a **tenant-wide read grant** (`subscribe tenant.<tid>.interaction.*.log`) so the browser can
  watch everything without reconnecting — which hands every agent's browser read access to
  EVERY interaction in the tenant, breaking the per-interaction isolation signaling-core's
  security requirement promises.

The in-flight DESK change `rp1-web-consumer-auth` hit exactly this and (provisionally) chose
the tenant-wide read grant, flagging it as an owner decision. A cross-agent research room
converged on a different, well-trodden model instead: the browser subscribes to **one personal
fan-out feed**, never to raw conversation subjects. This is the shape Matrix `/sync`, Slack
Socket Mode, Stream Chat, and Twilio `UserConversation` all use — the server fans the facts a
participant is entitled to into a per-user channel; the client never authorizes per-room.

## What this change does

Adds a **per-agent fan-out feed** as the agent inbox's read surface, generalizing
signaling-core's offer-accept/auth-callout model from "one interaction grant" to "one personal
feed grant":

- **Feed subject + grant.** Each agent has ONE personal feed
  `tenant.<tid>.agent.<aid>.feed.>` (a sub-subject per interaction:
  `tenant.<tid>.agent.<aid>.feed.<interaction_id>`). The auth-callout grants an inbox
  connection ONLY: `subscribe tenant.<tid>.agent.<aid>.feed.>` + the EXISTING `.cmd` publish
  plane (per accepted interaction) + the agent's own `routing.offer.user.<aid>(.control)`,
  `notify.<aid>`, `presence.<aid>` (read). It grants **NO** direct
  `tenant.<tid>.interaction.*.log` subscribe. `<aid>` MUST be the connection's authenticated
  user — an agent can read only its own feed.

- **Participation/Fan-out service.** A new server-side RelayPoint service (a trusted-server
  consumer, NOT a client) tails the canonical `tenant.*.interaction.*.log`, maintains
  `(tenant, interaction, agent)` participation from the **participation facts the router already
  writes** (`participant.joined` / `participant.left`, plus the assignment/transfer facts), and
  **projects each fact into the feed of every currently-participating agent**:
  `tenant.<tid>.agent.<aid>.feed.<interaction_id>`. The canonical `.log` is **unchanged** and
  remains the sole source of truth; the feed is a derived projection.

- **Unified chat + voice.** The feed carries facts for ANY medium (chat, voice, video) — medium
  stays a payload field, never a subject (signaling-core invariant). There is **no per-medium
  auth fork**: inbox read scope is the same feed for chat and voice. A voice **media** leg may
  still reconnect for the narrow `interaction.<id>.signal.<self>` + `.cmd` media scope
  (signaling-core's accept-reconnect), but NEVER to widen inbox READ scope.

- **History / backfill.** On assignment/open the Fan-out service **backfills** the feed from the
  canonical `.log` (replay from `sequence 0` up to live) so a freshly-participating agent sees
  the thread's history without a direct `.log` subscribe; the browser tracks a feed cursor and
  the service is the only authority that reads `.log` on its behalf.

- **Revocation.** On `participant.left` / un-assign / transfer-away the service STOPS projecting
  future facts into that agent's feed sub-subject; policy for already-delivered feed retention
  (audit/history) is defined.

## Impact

- New container/service: **RelayPoint Participation/Fan-out service** — a trusted-server
  JetStream consumer of `tenant.*.interaction.*.log`; the only NEW publisher of
  `tenant.<tid>.agent.<aid>.feed.>`. Loose-coupling rule: it depends on owned ports
  (a `ParticipationView` + a `FeedSink`), not on `nats.JetStreamContext` in its core.
- New subjects: `tenant.<tid>.agent.<aid>.feed.<interaction_id>` (server-write, agent-read-own).
  `.log` / `.cmd` / `.signal` / offer subjects are UNCHANGED.
- Auth-callout: a NEW grant shape (feed-subscribe + `.cmd`-publish; NO `.log` subscribe) for the
  inbox connection — generalizes signaling-core's per-interaction grant.
- ADR: a new ADR records the fan-out-feed decision (it changes the authorization architecture
  signaling-core's auth-callout section established).
- **Dependent DESK follow-up (not in this repo):** the desk change `rp1-web-consumer-auth` —
  which assumed a direct per-interaction subscribe + a desk-minted tenant-wide read grant — MUST
  be REWORKED to consume the per-agent feed instead (`agent.<aid>.feed.>`, no tenant-wide read,
  no direct `.log`). Tracked as a follow-up on the desk repo; this change does not edit it.

## Open questions (decisions owed — see design "Open questions")

1. **Participation source — the crux.** Whose facts establish `(tenant, interaction, agent)`?
   RelayPoint's router writes `participant.joined/left` and the assignment/transfer facts, so RP
   *can* be self-sufficient. BUT: does RP actually OWN assignment, or does Desk decide who is
   assigned and merely have RP record it? If assignment authority lives in Desk, how does Desk
   feed participation in — a Desk-issued command the router turns into a `participant.joined`
   fact (preferred: keeps the feed driven purely by `.log`), or an out-of-band participation API?
   This determines whether the Fan-out service is `.log`-only or needs a second input.
2. Fact projection: **copy the full event** into the feed vs a **pointer** (feed carries
   `{interaction_id, sequence}` and the client fetches)? Copy is simpler and matches the
   research baselines; pointer keeps one stored copy but reintroduces a per-interaction read.
3. Feed **ordering/dedup**: per-`<interaction_id>` ordering is the canonical `sequence`; is a
   feed-global order across interactions needed, or is per-interaction order sufficient (inbox
   merges by interaction)?
4. Retained-feed policy on revocation: purge the agent's feed sub-subject, or retain for
   audit/history with a tombstone? (Interacts with whether the feed is JetStream-durable.)
5. Tenant isolation baseline: hard `tenant.<tid>` subject prefix on the feed (assumed) — confirm
   no account-level shortcut.

## Non-goals

- No change to the canonical `.log` / `.cmd` / `.signal` / offer contract or the protobuf wire.
- No change to the router's authority over `.log` facts (the feed is read-only projection).
- Not editing the desk repo (the desk rework is a tracked dependent follow-up).
- No multi-party/conference, no mobile, no HA fan-out clustering (single fan-out instance;
  HA/sharding deferred like the router's).
