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
feed grant". This proposal PINS every decision (no OR-branches): the result is a deterministic,
implementable authorization boundary.

- **Feed subject + grant.** Each agent has ONE personal feed
  `tenant.<tid>.agent.<aid>.feed.>` (a sub-subject per interaction:
  `tenant.<tid>.agent.<aid>.feed.<interaction_id>`). The `.cmd` subject GAINS an **identity
  suffix**: `tenant.<tid>.interaction.<iid>.cmd.<identity>`, mirroring the repo's EXISTING
  `.signal.<userId>` authorship precedent — the author is in the subject and the NATS publish-ACL
  (not a payload field) binds the command to its publisher. The auth-callout grants an inbox
  connection ONLY: `subscribe tenant.<tid>.agent.<aid>.feed.>` + a command-publish grant
  `publish tenant.<tid>.interaction.*.cmd.<self>` (wildcard interaction, **FIXED `<self>`
  suffix** — a client can only publish as itself; `*.cmd.<other>` is denied) + a
  **per-connection minted reply-inbox** `subscribe _INBOX_<conn>.>` + the agent's own
  `routing.offer.user.<aid>(.control)`, `notify.<aid>`, `presence.<aid>` (read). It grants
  **NO** direct `tenant.<tid>.interaction.*.log` subscribe and **NO** broad `_INBOX.>`. `<aid>`
  MUST be the connection's authenticated user — an agent reads only its own feed. Because the
  interaction token is a wildcard, a newly-assigned agent can issue commands with **no
  reconnect**; the **router** subscribes `tenant.*.interaction.*.cmd.*`, takes the publisher
  identity from the LAST subject token (NEVER the payload), and enforces participation
  server-side (an agent may only act on interactions it participates in, checked against the
  `.log`-derived membership).

- **Participation = `.log` facts (PINNED: source A).** Participation `(tenant, interaction,
  agent)` is derived **SOLELY** from `.log` facts (`participant.joined` / `participant.left` /
  `interaction.assigned`). There is no second control plane. Desk, a trusted backend, does NOT
  call a participation API: it issues an **authorized assignment/participation command** as
  `…cmd.<desk-svc-identity>` (privileged `participant.assign` / `unassign` / `transfer`) that
  the router **validates (actor role from the authenticated suffix identity + authz) and writes
  as the `participant.joined` / `interaction.assigned` / `participant.left` fact** on the
  canonical `.log`, with audit fields (commanding actor, reason, request id). The role gate
  exempts trusted-backend identities from the participant check. Single source of truth, single
  ordering authority.

- **Participation/Fan-out service (leased single-active worker, effectively-once).** A new
  server-side RelayPoint service (a trusted-server JetStream consumer, NOT a client) tails the
  canonical `tenant.*.interaction.*.log` on ONE durable consumer, maintains participation from
  those facts, and **projects each fact into the feed of every currently-participating agent**.
  It runs as a **single ACTIVE worker** with standby replicas behind a **NATS KV leader lease**
  (TTL ~5s) — NO partition subject-mapping, NO per-shard durables, NO rebalance protocol; it is
  not engineered to higher availability than the single-node NATS + single router it derives
  from (per signaling-core Phase-1). **Hydration** is one linear catch-up: the participation
  view is snapshotted to KV every N facts/seconds keyed by stream sequence; on start/failover the
  worker loads the snapshot, does a read-only fold of the tail up to the ack floor, then goes
  live. A source `.log` message is **acked ONLY after all intended per-agent feed publishes
  succeed**; feed publishes carry a deterministic `Nats-Msg-Id` for idempotent replay, so
  delivery is **effectively-once (at-least-once delivery + idempotent feed publish)** — a
  lease-failover double-ownership window is safe under the same dedup. Cursor storage,
  retry/backoff with redelivery backstop, and poison/DLQ are specified. A `{{partition(N,…)}}`
  sharded scale-out path is documented as an additive future option (subjects/semantics
  unchanged). The canonical `.log` is **unchanged** and remains the sole source of truth.

- **Unified chat + voice.** The feed carries facts for ANY medium (chat, voice, video) — medium
  stays a payload field, never a subject (signaling-core invariant). There is **no per-medium
  auth fork**. A voice **media** leg may still reconnect for the narrow
  `interaction.<id>.signal.<self>` media scope (signaling-core's accept-reconnect), but NEVER to
  widen inbox READ scope and NEVER for `.cmd` (the wildcard command grant already covers it).

- **Backfill = bounded history-read COMMAND (PINNED).** The feed carries LIVE facts from
  assignment forward only — it is NEVER replayed from `sequence 0`. On assignment the agent gets
  prior history via a **participation-checked, bounded, paginated history-read request**
  (`tenant.<tid>.agent.<self>.feed.history`) that the service answers AFTER checking
  membership. Range/pagination/ordering/failure semantics and a max-auto-backfill threshold are
  specified. The browser never reads `.log` directly.

- **Revocation epoch (PINNED).** Membership is an **interval `[join_seq, left_seq)`**. Every
  projection AND every queued/in-flight backfill is epoch/interval-guarded, so NO
  post-revocation feed write occurs. A `participant.left` racing a `participant.joined` backfill
  cancels the backfill. Transfer keeps **new-leg-before-old-revoked**.

- **Feed durability (PINNED: ephemeral low-retention).** The feed is an **EPHEMERAL,
  short-max-age JetStream stream** sized only to bridge a live disconnect gap. The canonical
  `.log` plus the history-read command are the long-term / audit source. Purge and
  `feed.revoked` tombstone behavior are specified.

## Impact

- New container/service: **RelayPoint Participation/Fan-out service** — a leased single-active,
  trusted-server JetStream consumer of `tenant.*.interaction.*.log`; the only NEW publisher of
  `tenant.<tid>.agent.<aid>.feed.>` and the responder to `…feed.history`. Loose-coupling rule:
  its core depends on owned ports (a `ParticipationView`, a `FeedSink`, a `Cursor`, a
  `HistoryReader`), not on `nats.JetStreamContext`.
- New subjects: `tenant.<tid>.agent.<aid>.feed.<interaction_id>` (server-write, agent-read-own)
  and `tenant.<tid>.agent.<aid>.feed.history` (request/reply, participation-checked).
  `.log` / `.cmd` / `.signal` / offer subjects are UNCHANGED.
- Auth-callout: a NEW grant shape for the inbox connection (feed-subscribe + ACL-pinned
  `publish tenant.<tid>.interaction.*.cmd.<self>` (fixed `<self>` suffix) + per-connection
  `_INBOX_<conn>.>` reply scope; NO `.log` subscribe, NO broad `_INBOX.>`) — generalizes
  signaling-core's per-interaction grant and its `.signal.<self>` authorship precedent.
- Subject change: the command subject GAINS an identity suffix
  `tenant.<tid>.interaction.<iid>.cmd.<identity>` (publisher in subject, ACL-enforced); the
  router subscribes `tenant.*.interaction.*.cmd.*`.
- Router: a NEW server-side participation authorization check on every
  `tenant.<tid>.interaction.*.cmd.*` (already the `.cmd` writer), taking the publisher identity
  from the last subject token (never the payload) and requiring `actor_id` to match it; and a NEW
  privileged assignment/participation command path that Desk uses (as `…cmd.<desk-svc-identity>`)
  to land `participant.*` / `interaction.assigned` facts with audit fields.
- ADR: a new ADR records the fan-out-feed authorization architecture change.
- **Dependent DESK follow-up (not in this repo):** the desk change `rp1-web-consumer-auth` —
  which assumed a direct per-interaction subscribe + a desk-minted tenant-wide read grant — MUST
  be REWORKED to consume the per-agent feed instead (`agent.<aid>.feed.>`, no tenant-wide read,
  no direct `.log`; history via the `feed.history` command; assignment via the privileged
  participation command). Tracked as a follow-up on the desk repo; this change does not edit it.

## Non-goals

- No change to the canonical `.log` / `.cmd` / `.signal` / offer contract or the protobuf wire
  (the privileged participation command reuses the existing `.cmd` plane + `Event` envelope).
- No change to the router's authority over `.log` facts (the feed is read-only projection).
- Not editing the desk repo (the desk rework is a tracked dependent follow-up).
- No multi-party/conference, no mobile.
