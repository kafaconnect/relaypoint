# Design — agent-feed-fanout

## Context

signaling-core established: the router is the sole `.log` writer; clients are read-only on
`.log` and write `.cmd`; per-interaction read is granted by the **auth-callout** on offer-accept
(the connection reconnects with a token that adds `tenant.<tid>.interaction.<id>.>`). On accept
the router already records `interaction.assigned` / `participant.joined` on
`interaction.<id>.log` (core-flows). That authorizes ONE interaction. The agent inbox needs MANY
at once. This change adds a per-agent **fan-out feed** as the inbox read surface and keeps the
rest of signaling-core intact.

This design **aligns with signaling-core** (it does not supersede it): `.log`/`.cmd`/`.signal`,
offer-accept, the auth-callout, the "medium is a payload field" invariant, the `_INBOX`+nonce
reply pattern, and router authority are all unchanged. It adds a derived read projection and a
server-side fan-out authority — which the auth-callout section already anticipates as
"additional trusted server-side authorities".

**This is a pinned, implementable auth boundary.** Every decision below is a single choice; there
are no OR-branches or "decision owed" markers. The owner has CONFIRMED participation source = A.

## Research baseline (why a per-user feed, not per-room subscribe)

The decided model matches established systems — none expose raw per-room subjects to browsers:

- **Matrix `/sync`** — the client polls ONE personal sync endpoint; the server returns the rooms
  the user is joined to. The client never subscribes per-room.
- **Slack Socket Mode** — ONE WebSocket per app/user; the server pushes the events that
  connection is entitled to.
- **Stream Chat / Twilio `UserConversation`** — a per-USER view fans in the conversations the
  user participates in; membership is server-checked, not client-asserted.

Common shape: **server-side participation check → fan facts into a per-user channel → client
reads only its own channel.** That is precisely `tenant.<tid>.agent.<aid>.feed.>`.

## Decision 1 — Feed subject layout + auth-callout grant (PINNED)

Subject: `tenant.<tid>.agent.<aid>.feed.<interaction_id>` (lowercase, dot-separated, ids carry
no dots — same rules as `.log`). `<aid>` = the agent's authenticated userId. One sub-subject per
interaction lets the client (and revocation) operate per-interaction while the client subscribes
the single wildcard `tenant.<tid>.agent.<aid>.feed.>`.

Auth-callout grant for an **inbox** connection (`<self>` = authenticated user, `<conn>` = a
per-connection id minted by the auth-callout):

| Capability | Subject | Note |
|---|---|---|
| Subscribe (read) | `tenant.<tid>.agent.<self>.feed.>` | the ONLY inbox read grant; `<self>`-bound |
| Publish (write) | `tenant.<tid>.interaction.*.cmd.<self>` | wildcard INTERACTION, **FIXED `<self>` suffix** — identity is in the subject (ACL-pinned authorship); `*.cmd.<other>` is denied; router authorizes participation server-side (Decision 2b) |
| Publish (request) | `tenant.<tid>.agent.<self>.feed.history` | bounded history-read request (Decision 5) |
| Subscribe (reply) | `_INBOX_<conn>.>` | **per-connection minted** reply-inbox; the request/reply replies land here (Decision 4) |
| Subscribe own offer/notify/presence | `routing.offer.user.<self>(.control)`, `notify.<self>`, `presence.<self>` | unchanged from signaling-core |

The inbox connection holds **NO** `tenant.<tid>.interaction.*.log` subscribe, **NO** `.log`
write, **NO** feed publish, and **NO** broad `_INBOX.>` (Decision 4). A voice **media** leg still
uses signaling-core's accept-reconnect to gain the narrow `interaction.<id>.signal.<self>` media
scope — a SEPARATE, narrow grant that NEVER widens inbox read scope and is NOT needed for `.cmd`
(the wildcard-interaction command grant already covers media commands).

**Write identity = ACL-pinned subject suffix (not subject-mapping).** The `.cmd` subject GAINS an
identity suffix: `tenant.<tid>.interaction.<iid>.cmd.<identity>`. This mirrors the repo's EXISTING
`.signal.<userId>` precedent (subject-model.html): the publisher's id is in the subject, so the
NATS **publish-ACL** — not a payload `actor_id` — binds each command to its author. The earlier
"subject-mapping" mechanism is **REJECTED as unimplementable**: NATS subject mappings are
subject-token transforms, NEVER the connection's authenticated identity, so they cannot pin who
published a command. The ACL-pinned suffix provably exists and closes the spoofing /
privilege-escalation hole.

**Why the wildcard-INTERACTION publish grant (kills the WRITE reconnect storm).** If `.cmd`
publish were granted per-accepted-interaction, every newly-assigned thread would force a token
refresh + reconnect to widen the write ACL — the exact storm on the WRITE plane that the feed
removes on the READ plane. Instead the inbox connection holds ONE `publish
tenant.<tid>.interaction.*.cmd.<self>` grant for its lifetime (wildcard interaction, FIXED
`<self>` suffix), and per-interaction authorization moves SERVER-SIDE into the router (Decision
2b). A newly-assigned agent can reply immediately, no reconnect; it still can only publish *as
itself* because the suffix is ACL-fixed.

## Decision 2 — Participation = `.log` facts (source A, CONFIRMED)

### 2a — Privileged command → fact contract

Participation `(tenant, interaction, agent)` is derived **SOLELY** from `.log` facts:
`participant.joined`, `participant.left`, `interaction.assigned`. There is exactly ONE writer of
those facts (the router) and ONE source of truth (`.log`). The B alternative (a separate
`routing.participation.*` control plane the service also consumes) is **removed** — it would add
a source of truth that can diverge from `.log`.

Whoever decides assignment expresses it as a fact, never as a side input:

- **Router-internal** offer-accept already lands `interaction.assigned` / `participant.joined`
  (core-flows) — unchanged.
- **Desk** (a trusted backend) decides assignment for the agent inbox. It does NOT call a
  participation API. It issues a **privileged assignment/participation command** on the existing
  `.cmd` plane as `tenant.<tid>.interaction.<id>.cmd.<desk-svc-identity>` (`command_type` ∈
  {`participant.assign`, `participant.unassign`, `participant.transfer`}). The router:
  1. **validates the actor** — the actor role comes from the identity the auth-callout
     authenticated (the `<desk-svc-identity>` subject suffix), NEVER from the payload; it must be
     a trusted-backend (Desk) identity, not an agent role; an agent connection issuing a
     participation command is rejected;
  2. **validates authz** — the target tenant/interaction is in the actor's scope;
  3. **writes the resulting fact** (`participant.joined` / `interaction.assigned` /
     `participant.left`) onto `interaction.<id>.log` with **audit fields**: `actor` (commanding
     identity, taken from the suffix), `reason`, `request_id`, `occurred_at`. The fact is the
     assignment; the command is just how it is requested.

So in the steady state the feed is driven **purely from `.log`** — one ordering authority, one
input to the Fan-out service, no client-asserted membership.

### 2b — Router enforces participation on every `.cmd` (server-side authz)

Because the inbox connection holds `publish tenant.<tid>.interaction.*.cmd.<self>` (wildcard
interaction), the NATS publish-ACL pins **authorship** (the `<self>` suffix) but no longer scopes
WHICH interaction an agent may command. The **router** (already the sole `.cmd` consumer + `.log`
writer) subscribes `tenant.*.interaction.*.cmd.*` and therefore enforces, on EVERY `.cmd`:

- the **publisher identity is the LAST subject token**, NEVER the payload. The payload `actor_id`
  MUST equal that suffix identity, else the command is REJECTED with `reason: actor_mismatch`.
- the **role** (agent vs trusted-backend) comes from the identity the auth-callout authenticated
  for that suffix, NEVER from the payload.
- for an **agent**-role command, the publishing identity must be a CURRENT participant of the
  target interaction, checked against the `.log`-derived membership interval `[join_seq, left_seq)`
  (Decision 6) for that `(interaction, agent)` via the SAME `ParticipationView` fold the projector
  uses (one shared `.log`-derived source for read AND write authz);
- a non-participant agent command is REJECTED with
  `CommandResult{REJECTED, reason: not_a_participant}` on the requester's `_INBOX_<conn>` reply —
  it never produces a `.log` fact.

This makes the wildcard-interaction grant safe: the membership ledger that drives the feed READ
plane is the SAME ledger that authorizes the agent `.cmd` WRITE plane. Privileged participation
commands (2a) — published as `…cmd.<desk-svc-identity>` — are EXEMPT from the participant check;
the role gate exempts trusted-backend identities and audits `actor` + `reason` + `request_id`
instead.

### 2c — Loose coupling

The service core depends on owned ports — a `ParticipationView` (folds `.log` facts into
membership intervals), a `FeedSink` (publishes a projected fact to an agent feed), a `Cursor`
(durable consumer read position), and a `HistoryReader` (bounded `.log` range read for
backfill) — NOT on `nats.JetStreamContext`. It MUST be unit-testable with in-memory fakes
(replay a fact sequence, assert which agent feeds receive which facts), per the repo's HARD RULE.
The router's participant-authz check (2b) reuses the SAME `ParticipationView` port so the read
and write planes cannot disagree.

## Decision 3 — Fact projection: copy, ordering, dedup (PINNED)

**Copy the full event.** The feed message IS the projected `.log` `Event` (same protobuf
envelope, same `sequence`, same `event_id`). The pointer alternative is rejected (it reintroduces
a per-interaction `.log` read on the client). The inbox renders with no extra fetch.

- **Ordering.** Per-`<interaction_id>` order is the canonical router-assigned `sequence` —
  authoritative and preserved (the feed copies it). Across interactions there is no global order
  and none is needed: the inbox groups by interaction and orders each by `sequence`.
- **Dedup / idempotent replay.** Each fan-out publish sets a deterministic
  `Nats-Msg-Id = <tid>.<aid>.<iid>.<sequence>`. The ephemeral feed stream is configured with
  dedup (`Duplicates` window ≥ the redelivery/restart horizon), so a consumer redelivery or a
  worker restart that re-projects a fact stores it **at-most-once** per `(agent, interaction,
  sequence)`.
- **Gap-replay.** The client tracks a per-interaction last-applied `sequence` from the feed; on a
  gap it asks the history-read command (Decision 5) for the missing range — it never falls back
  to a direct `.log` subscribe.

**SDK consumption.** A single `client.agentFeed()` (or `inbox()`) yields `{interaction_id,
event}` for all participating interactions over the one feed subscription; the per-interaction
`events()` stays valid for the narrow media leg. (SDK shape is a desk/SDK follow-up; this change
specifies the wire surface.)

## Decision 4 — `_INBOX` isolation: per-connection minted reply prefix (PINNED)

The history-read (Decision 5) and `.cmd` CommandResult are request/reply; their replies land on a
reply-inbox. A broad `_INBOX.>` subscribe is a **command-result snooping hole**: a feed-only read
grant does NOT close it, because request/reply replies travel on `_INBOX`, not on the feed.

The auth-callout therefore **mints a per-connection reply prefix** and grants ONLY it:

- grant `subscribe _INBOX_<conn>.>` and `publish _INBOX_<conn>.>` where `<conn>` is a
  high-entropy per-connection token bound to this connection by the auth-callout;
- **DENY** `subscribe _INBOX.>` and any other connection's `_INBOX_<other>.>`;
- the SDK is configured to use `_INBOX_<conn>` as its `InboxPrefix`, so every request it makes
  replies into its own scoped prefix.

A second client on the same tenant cannot subscribe the first client's `_INBOX_<conn>.>`, so it
cannot snoop the first client's CommandResults or history-read replies. This generalizes
signaling-core's `_INBOX`+nonce offer-reply: the nonce protected one offer; the minted prefix
protects the whole connection's reply plane.

## Decision 5 — Backfill = bounded history-read COMMAND (PINNED, not replay-from-0)

The feed carries **LIVE facts from assignment forward only** — it is NEVER seeded by replaying
`.log` from `sequence 0` into the feed (that would re-inject the entire thread history into the
ephemeral live stream, racing live projection and inflating retention). Prior history is served
by a participation-checked read command:

- **Subject:** `tenant.<tid>.agent.<self>.feed.history` (request/reply; reply on
  `_INBOX_<conn>`). Granted to the inbox connection (Decision 1).
- **Request:** `{interaction_id, from_sequence, to_sequence?, limit, direction}`.
- **Server (Fan-out service) handling:**
  1. **participation check** — the requesting `<self>` must have an OPEN or past membership
     interval covering the requested range for `interaction_id` (Decision 6); otherwise REJECT
     (`not_a_participant`), no data leaks;
  2. **bounded read** — read `interaction.<id>.log` over `[from_sequence, to_sequence]` capped at
     `limit` (server max page size, e.g. 200); the SERVICE is the only authority that reads
     `.log` on the agent's behalf;
  3. **ordering** — ascending `sequence`; a `next_cursor` is returned when the range is truncated
     (pagination); the client pages until caught up to its live feed cursor.
- **Range / ordering / failure semantics:** out-of-range → empty page; over-limit → truncated +
  `next_cursor`; `.log` read error → `CommandResult{REJECTED, reason: history_unavailable}` (the
  client retries with backoff); a request for a range BELOW the interaction's retained `.log`
  floor → REJECT (`out_of_retention`) — history beyond audit retention is not reconstructable.
- **Max-auto-backfill threshold.** On a fresh assignment the client auto-backfills at most
  `MAX_AUTO_BACKFILL` facts (e.g. last 200) to render the thread; older history is fetched
  on-demand (scroll-up) via the same command. Prevents an assignment to a 100k-fact interaction
  from flooding the inbox.
- **Cursor.** The client persists the highest `sequence` it applied per interaction; on reconnect
  it resumes the live feed and history-reads ONLY the gap below its cursor.

## Decision 6 — Revocation epoch: membership as `[join_seq, left_seq)` (PINNED)

Membership is modeled as a **half-open interval** keyed by `(tenant, interaction, agent)`:

- `participant.joined` / `interaction.assigned` at `.log` `sequence` J opens an interval
  `[J, ∞)`;
- `participant.left` / un-assign / transfer-away at `sequence` L closes it to `[J, L)`.

Every write into an agent feed is **interval-guarded** by the source fact's `sequence`:

- **Live projection.** A fact at `sequence` S is projected to agent A's feed ONLY if S falls in
  an OPEN interval of A for that interaction. The instant `participant.left` at L is folded, the
  interval closes, so facts at `sequence ≥ L` are NOT projected to A — guaranteeing no
  post-revocation feed write. (The `participant.left` fact itself, at L, is projected so the
  client can drop the interaction.)
- **Backfill guard.** A queued/in-flight history-read or fresh-assignment backfill carries the
  membership interval it was authorized under; if a `participant.left` closes the interval before
  the backfill drains, the remaining backfill is **cancelled** — a `participant.left` racing a
  `participant.joined` backfill never delivers post-revocation facts. The participation check in
  Decision 5 re-validates the interval at serve time.
- **Transfer (new-leg-before-old-revoked).** A cold transfer lands the NEW agent's
  `participant.joined` (opening its interval and triggering its backfill) BEFORE the OLD agent's
  `participant.left` is folded, so the interaction is never absent from both inboxes at once —
  mirroring signaling-core's call-leg handover.

The router's `.cmd` participant-authz (Decision 2b) uses the SAME intervals: a command at the
current `.log` head is authorized only if the requester has an OPEN interval, so a revoked agent's
late `.cmd` is rejected for the same reason its feed stops.

## Decision 7 — Participation/Fan-out service: leased single-active worker, effectively-once (PINNED)

The service is a **single ACTIVE worker** (replicas on standby behind a leader lease), NOT a
sharded fleet. It must not be engineered to higher availability than the **single-node NATS +
single router** it derives from (signaling-core Phase-1); sharding is demoted to a documented
scale-out path (appendix below).

- **One durable consumer.** ONE durable JetStream consumer on `INTERACTION_LOGS`
  (`tenant.*.interaction.*.log`). Per-interaction ordering is trivial — one consumer, stream
  order. NO partition subject-mapping, NO per-shard durables, NO rebalance protocol.
- **Leader lease.** Replicas contend for a **NATS KV leader lease** (TTL ~5s, renewed by
  heartbeat); the holder is the single active worker. On holder death the lease expires (~TTL) and
  a standby claims it and resumes from the durable cursor. A brief double-ownership window across
  failover is safe under the same idempotent `Nats-Msg-Id` dedup (Decision 3).
- **Hydration = one linear catch-up.** The participation view is snapshotted to KV every N
  facts/seconds, keyed by **stream sequence**. On start/failover the worker: (1) loads the latest
  snapshot, (2) does a **read-only fold of the tail** from the snapshot's sequence up to the
  consumer ack floor, (3) goes live. No replay-from-zero of the whole log.
- **Effectively-once fan-out (ack-after-publish).** "Effectively-once" = **at-least-once delivery
  + idempotent feed publish**; this design does NOT claim exactly-once. For each source `.log`
  fact the worker: (1) folds participation, (2) publishes the projection to EVERY
  currently-participating agent's feed with the deterministic `Nats-Msg-Id`, (3) **acks the source
  message ONLY after all intended feed publishes are acknowledged by JetStream.** If the worker
  crashes between (2) and (3), the source fact is **redelivered** and re-projected; dedup makes the
  re-projection a no-op — no drop, at-most-once into each feed. This is the
  partial-publish-then-crash guarantee.
- **Cursor storage.** The durable consumer's ack floor IS the cursor (JetStream stores it); the
  worker holds no authoritative state beyond it + the KV snapshot, so a restarted/failed-over
  worker resumes from the last acked fact.
- **Retry / backoff.** A failed feed publish is retried with exponential backoff in-worker (the
  source fact stays un-acked, so redelivery is the backstop). Transient NATS errors do not advance
  the cursor.
- **Poison / DLQ.** A fact that fails projection past `max_deliver` (e.g. malformed envelope, a
  feed subject that cannot be resolved) is routed to a **dead-letter subject**
  (`tenant.<tid>.agent.dlq.feed`) with the failure reason and the source `event_id`/`sequence`,
  and the source is acked so the consumer is not wedged. DLQ is operator-drained; it never silently
  drops.

### Scale-out path (appendix — additive, NOT in this change)

If a measured single-consumer **lag** (the trigger) exceeds budget, the service can later shard
WITHOUT changing any subject or semantic: introduce a NATS 2.10 `{{partition(N, tenant,
interaction)}}` subject-mapped split with one durable per shard, so all facts of an interaction
still land on one shard (per-interaction `sequence` order preserved), each shard leased to one
worker. The feed subjects, the deterministic `Nats-Msg-Id`, the ack-after-publish guarantee, and
the participation ports are unchanged; only the consumer fan-in widens. This is an additive
future option, not part of this change.

## Decision 8 — Feed durability / retention: ephemeral low-retention (PINNED)

The feed is an **EPHEMERAL, short-`max_age` JetStream stream** (e.g. `max_age` minutes-to-hours,
`max_msgs_per_subject` small), sized ONLY to bridge a live disconnect gap so a briefly-offline
inbox catches up without a history-read. It is NOT the long-term/audit store.

- **Long-term / audit source:** the canonical `.log` (durable, retained per audit policy) plus the
  `feed.history` read command (Decision 5). The feed never needs to retain history because history
  is reconstructable from `.log`.
- **Purge.** Feed messages age out by `max_age`; on `participant.left` the service may purge the
  agent's `…feed.<iid>` subject after writing the tombstone (below), since post-revocation feed
  content has no live purpose and `.log` retains the audit copy.
- **Tombstone.** On revocation the service writes a terminal `feed.revoked{interaction_id,
  at_sequence}` marker into `…feed.<iid>` so a reconnecting client deterministically drops the
  interaction from its inbox even if it missed the `participant.left`. The tombstone is the only
  thing that must outlive immediate purge (covered by `max_age`).
- **Why not durable-per-agent:** a durable per-agent feed would duplicate `.log` content N times
  (once per participant) under audit retention — strictly worse than one canonical `.log` +
  on-demand history-read, with no offline-inbox benefit the ephemeral bridge + history-read don't
  already give.

## Decision 9 — Tenant isolation baseline (PINNED)

Hard `tenant.<tid>` subject prefix on the feed and history subjects, enforced by the auth-callout
exactly like every other subject in signaling-core — NOT an account-level shortcut. A connection
authenticated for tenant A can subscribe only `tenant.<A>.agent.<self>.feed.>`; the `<self>`
binding prevents reading another agent's feed within the same tenant. (Phase-1 caveat: like
signaling-core, full enforcement requires the auth-callout minting per-connection scoped ACLs +
the `_INBOX_<conn>` prefix of Decision 4; the shared-`client` dev posture does not enforce it.)

## Desk impact (dependent follow-up — NOT edited here)

The desk change `rp1-web-consumer-auth` currently subscribes the browser per-interaction via
`client.interaction(sessionId).events()` and, for the multi-thread inbox, recommends a **desk
auth-callout grant widened to tenant-wide read** (`subscribe tenant.<tid>.interaction.*.log`).
This model **forbids that**. The desk change MUST be REWORKED to:

- subscribe the inbox to `tenant.<tid>.agent.<aid>.feed.>` (one feed), not per-interaction `.log`;
- drop the tenant-wide read grant; the desk-minted inbox grant becomes feed-subscribe +
  `publish tenant.<tid>.interaction.*.cmd.<self>` (wildcard interaction, ACL-pinned `<self>`
  suffix) + the minted `_INBOX_<conn>.>` reply scope (Decisions 1, 4);
- get history via the `feed.history` participation-checked command (Decision 5), not a `.log`
  replay; render the thread from `MAX_AUTO_BACKFILL` then scroll-up on demand;
- issue assignment as the **privileged participation command** as `…cmd.<desk-svc-identity>`
  (Decision 2a) so it lands as a `.log` fact, rather than calling any participation API;
- render `message.created` from the SAME projected `Event` envelope (the feed copies the fact, so
  decoding is unchanged).

This is a tracked dependent follow-up on the DESK repo. This change does not edit desk.

## Alternatives rejected

- **Participation source B (a separate `routing.participation.*` control plane).** A second source
  of truth that can diverge from `.log`; rejected — assignment is expressible as a `.log` fact via
  the privileged command (Decision 2a).
- **Per-accepted-interaction `.cmd` publish grant.** A WRITE-plane reconnect storm mirroring the
  READ-plane one this change removes; rejected for the wildcard-interaction grant + ACL-pinned
  `<self>` suffix + server-side participant authz (Decisions 1, 2b).
- **Subject-mapping to assert the publisher identity.** UNIMPLEMENTABLE: NATS subject mappings are
  subject-token transforms, never the connection's authenticated identity; rejected for the
  ACL-pinned `.cmd.<self>` suffix (which provably exists, mirroring `.signal.<self>`).
- **Broad `_INBOX.>` reply grant.** A command-result snooping hole a feed-only read grant does NOT
  close; rejected for the per-connection minted `_INBOX_<conn>.>` (Decision 4).
- **Backfill by replaying `.log` from `sequence 0` into the feed.** Re-injects whole-thread
  history into the ephemeral live stream, races live projection, inflates retention; rejected for
  the bounded history-read command (Decision 5).
- **Sharded fan-out fleet as the Phase-1 shape.** Over-engineers the projector beyond the
  single-node NATS + single router it derives from; rejected for a leased single-active worker +
  KV snapshot hydration + ack-after-publish (Decision 7), with sharding demoted to a lag-triggered
  scale-out appendix.
- **Durable per-agent feed as the audit store.** Duplicates `.log` N times under retention;
  rejected for ephemeral feed + canonical `.log`/history-read (Decision 8).
- **Tenant-wide `.log` read grant** (desk's provisional choice) — every agent's browser reads
  every interaction in the tenant; breaks signaling-core's per-interaction isolation. Rejected.
- **Per-interaction subscribe with reconnect-per-thread** — a read reconnect storm for a
  many-thread inbox; the exact problem this change exists to remove. Rejected for the inbox
  (still valid for the single accepted-interaction media leg).
- **Client-asserted membership** — the client is untrusted; participation MUST be server-checked.
  Rejected.
