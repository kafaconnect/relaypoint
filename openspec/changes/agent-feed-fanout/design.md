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
| Subscribe (reply) | `_INBOX_<conn>.>` | **per-connection minted** reply-inbox; the `.cmd` CommandResult replies land here (Decision 4) |
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

The `.cmd` **semantics are unchanged** (same command plane, same `Event` envelope, same router
authority); only the subject **shape** changes — it gains the trailing `<identity>` token. This is
a breaking subject-shape migration: the **router** must move from subscribing
`tenant.*.interaction.*.cmd` to `tenant.*.interaction.*.cmd.*` and read the publisher from the last
token, and the **SDK** must publish to `…cmd.<self>` instead of `…cmd`. Both migrations are
required for this change to land; there is no compatibility shim (the bare `.cmd` subject is
retired).

**Why the wildcard-INTERACTION publish grant (kills the WRITE reconnect storm).** If `.cmd`
publish were granted per-accepted-interaction, every newly-assigned thread would force a token
refresh + reconnect to widen the write ACL — the exact storm on the WRITE plane that the feed
removes on the READ plane. Instead the inbox connection holds ONE `publish
tenant.<tid>.interaction.*.cmd.<self>` grant for its lifetime (wildcard interaction, FIXED
`<self>` suffix), and per-interaction authorization moves SERVER-SIDE into the router (Decision
2b). A newly-assigned agent can reply immediately, no reconnect; it still can only publish *as
itself* because the suffix is ACL-fixed.

**Write-identity precondition (Phase-1 → production).** The `.cmd.<self>` suffix-ACL is only
**airtight** once the **auth-callout mints a per-connection authenticated identity** that the ACL
pins `<self>` to — replacing the dev **shared `client` user**. Under the shared-`client` dev
posture any connection could publish any `…cmd.<x>` suffix, so the suffix is advisory, not
enforced. Pinning the suffix to a minted per-connection identity is therefore a **hard production
precondition of this design, tied to the auth-callout work** (the same precondition that makes the
feed-read and `_INBOX_<conn>` ACLs of Decisions 1, 4, 9 enforceable).

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
membership intervals), a `FeedSink` (publishes a projected fact to an agent feed), and a `Cursor`
(durable consumer read position) — NOT on `nats.JetStreamContext`. It MUST be unit-testable with
in-memory fakes
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
  gap it heals via a **desk REST refetch** (Decision 5) for the missing range — it never falls back
  to a direct `.log` subscribe and RelayPoint never replays history to it.

**SDK consumption.** A single `client.agentFeed()` (or `inbox()`) yields `{interaction_id,
event}` for all participating interactions over the one feed subscription; the per-interaction
`events()` stays valid for the narrow media leg. (SDK shape is a desk/SDK follow-up; this change
specifies the wire surface.)

## Decision 4 — `_INBOX` isolation: per-connection minted reply prefix (PINNED)

The `.cmd` CommandResult is request/reply; its reply lands on a reply-inbox. A broad `_INBOX.>`
subscribe is a **command-result snooping hole**: a feed-only read grant does NOT close it, because
request/reply replies travel on `_INBOX`, not on the feed.

The auth-callout therefore **mints a per-connection reply prefix** and grants ONLY it:

- grant `subscribe _INBOX_<conn>.>` and `publish _INBOX_<conn>.>` where `<conn>` is a
  high-entropy per-connection token bound to this connection by the auth-callout;
- **DENY** `subscribe _INBOX.>` and any other connection's `_INBOX_<other>.>`;
- the SDK is configured to use `_INBOX_<conn>` as its `InboxPrefix`, so every request it makes
  replies into its own scoped prefix.

A second client on the same tenant cannot subscribe the first client's `_INBOX_<conn>.>`, so it
cannot snoop the first client's CommandResults. This generalizes
signaling-core's `_INBOX`+nonce offer-reply: the nonce protected one offer; the minted prefix
protects the whole connection's reply plane.

## Decision 5 — History is DESK's data, served by desk REST (out of RP scope, PINNED — owner decision)

**RelayPoint serves NO conversation history.** The feed carries **LIVE facts from the agent's join
point FORWARD ONLY** — it is NEVER seeded by replaying `.log` from `sequence 0`, and RelayPoint
exposes **no** `feed.history` request/reply, **no** history auth grant, **no** `MAX_AUTO_BACKFILL`,
and **no** backfill-on-assignment behavior. The earlier bounded history-read command (and its
pre-join visibility gate) is **REMOVED**.

Ownership is orthogonal: **DESK owns conversation DATA** (Postgres, the source of truth, served over
desk's REST API), **RelayPoint owns live DELIVERY** of facts from join forward. Prior messages are
desk's, not RelayPoint's, so reconstructing them is a desk REST read — not an RP subject.

- **On open / assignment** the browser loads prior messages from **desk REST** (Postgres), then
  attaches to the live feed for everything from its join point forward.
- **On reconnect / detected gap** the browser **heals via a desk REST refetch** (the existing rp1
  pattern), then resumes the live feed; it never opens a direct `interaction.<id>.log` subscribe and
  RelayPoint never replays history to it.

This keeps the live-feed authz clean: it is simply the membership interval `[join_seq, left_seq)`
(Decision 6) — there is **no** pre-join `[0, join_seq)` visibility rule, because the feed never
carries pre-join facts at all. History visibility is desk's REST authorization concern, not RP's.

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
- **Transfer (new-leg-before-old-revoked).** A cold transfer lands the NEW agent's
  `participant.joined` (opening its interval, so live facts begin projecting to it) BEFORE the OLD
  agent's `participant.left` is folded, so the interaction is never absent from both inboxes at
  once — mirroring signaling-core's call-leg handover. (The new agent loads prior messages from
  desk REST, Decision 5; RelayPoint projects only its forward-from-join live facts.)

The router's `.cmd` participant-authz (Decision 2b) uses the SAME intervals: a command at the
current `.log` head is authorized only if the requester has an OPEN interval, so a revoked agent's
late `.cmd` is rejected for the same reason its feed stops.

## Decision 7 — Participation/Fan-out service: leased single-active worker, effectively-once (PINNED)

The service is a **single ACTIVE worker** (replicas on standby behind a leader lease), NOT a
sharded fleet. It must not be engineered to higher availability than the **single-node NATS +
single router** it derives from (signaling-core Phase-1); sharding is demoted to a documented
scale-out path (appendix below).

- **One durable consumer, serially processed (`MaxAckPending=1`).** ONE durable JetStream consumer
  on `INTERACTION_LOGS` (`tenant.*.interaction.*.log`), configured `MaxAckPending=1` — NO prefetch,
  NO concurrent projection: **exactly ONE in-flight `.log` fact at a time.** Per-interaction (and
  whole-stream) ordering is therefore strict — the participation fold is a serial state machine that
  applies fact N fully (fold + fan-out + ack) before fact N+1 is delivered. This is mandatory: the
  fold is **stateful** (it mutates membership intervals), so two facts processed concurrently could
  corrupt it. NO partition subject-mapping, NO per-shard durables, NO rebalance protocol.
- **Leader lease (takeover never overlaps in-flight processing).** Replicas contend for a **NATS KV
  leader lease** (TTL ~5s, renewed by heartbeat); the holder is the single active worker. On holder
  death the lease expires (~TTL) and a standby claims it and resumes from the durable cursor.
  Because `MaxAckPending=1`, JetStream redelivers the single un-acked fact to the new holder only
  after the prior delivery's ack/redelivery timer resolves — the standby waits for ack/redelivery
  before proceeding, so a lease takeover can NEVER fold two facts (or the same fact twice)
  concurrently. Any brief double-ownership window is still made safe by the idempotent `Nats-Msg-Id`
  dedup (Decision 3); serial-by-`MaxAckPending=1` additionally guarantees no out-of-order or
  concurrent fold.
- **Hydration from an ACKED-prefix snapshot.** The participation view is snapshotted to KV every N
  facts/seconds, keyed by **stream sequence** — and the snapshot represents an **ACKED prefix
  only**: the worker advances the snapshot ONLY after the corresponding source ack / ack-floor
  advance, so **no snapshot ever represents state ahead of the durable cursor**. On start/failover
  the worker: (1) loads the **latest snapshot whose `seq ≤ durable_ack_floor`**, (2) does a
  **read-only fold of the tail** over `(snapshot_seq, ack_floor]` to go live, (3) resumes live
  projection. Because the snapshot is always at/below the ack floor, hydration never double-applies
  an already-projected fact nor skips an un-projected one. No replay-from-zero of the whole log.
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
inbox catches up without a desk REST refetch. It is NOT the long-term/audit store.

- **Long-term / audit source:** the canonical `.log` (durable, retained per audit policy) within
  RelayPoint. Conversation history for the browser is **desk REST** (Decision 5), out of RP scope —
  the feed never needs to retain history.
- **Purge.** Feed messages age out by `max_age`; on `participant.left` the service may purge the
  agent's `…feed.<iid>` subject after writing the tombstone (below), since post-revocation feed
  content has no live purpose and `.log` retains the audit copy.
- **Tombstone.** On revocation the service writes a terminal `feed.revoked{interaction_id,
  at_sequence}` marker into `…feed.<iid>` so a reconnecting client deterministically drops the
  interaction from its inbox even if it missed the `participant.left`. The tombstone is the only
  thing that must outlive immediate purge (covered by `max_age`).
- **Why not durable-per-agent:** a durable per-agent feed would duplicate `.log` content N times
  (once per participant) under audit retention — strictly worse than one canonical `.log` + desk's
  REST history, with no offline-inbox benefit the ephemeral bridge doesn't already give.

## Decision 9 — Tenant isolation baseline (PINNED)

Hard `tenant.<tid>` subject prefix on the feed subject, enforced by the auth-callout exactly like
every other subject in signaling-core — NOT an account-level shortcut. A connection
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
- get conversation history from **desk's own REST API against Postgres** (Decision 5) — desk owns
  that data; RelayPoint serves NO history, so there is no `feed.history` to call. On open/assignment
  the browser loads prior messages from desk REST, then attaches the live feed; on reconnect/gap it
  heals by a desk REST refetch (the existing rp1 pattern). It never `.log`-replays;
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
- **RelayPoint serving conversation history at all** (replay-from-0 into the feed, OR a bounded
  `feed.history` request/reply). History is desk's data (Postgres); having RP serve it duplicates
  desk's source of truth, complicates the live-feed authz with a pre-join visibility gate, and
  inflates the ephemeral stream. Rejected (owner decision): history = desk REST, the feed is
  live-only from join forward (Decision 5).
- **Sharded fan-out fleet as the Phase-1 shape.** Over-engineers the projector beyond the
  single-node NATS + single router it derives from; rejected for a leased single-active worker +
  KV snapshot hydration + ack-after-publish (Decision 7), with sharding demoted to a lag-triggered
  scale-out appendix.
- **Durable per-agent feed as the audit store.** Duplicates `.log` N times under retention;
  rejected for ephemeral feed + canonical `.log` (RP) and desk REST history (Decision 8).
- **Tenant-wide `.log` read grant** (desk's provisional choice) — every agent's browser reads
  every interaction in the tenant; breaks signaling-core's per-interaction isolation. Rejected.
- **Per-interaction subscribe with reconnect-per-thread** — a read reconnect storm for a
  many-thread inbox; the exact problem this change exists to remove. Rejected for the inbox
  (still valid for the single accepted-interaction media leg).
- **Client-asserted membership** — the client is untrusted; participation MUST be server-checked.
  Rejected.
