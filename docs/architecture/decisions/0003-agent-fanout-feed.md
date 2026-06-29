# ADR-0003: Per-agent fan-out feed is the agent-inbox authorization model

- Status: Accepted
- Date: 2026-06-11
- Scope: the RelayPoint signaling authorization boundary for the **agent inbox** read+command
  surface. Relates to: signaling-core (auth-callout, `.log`/`.cmd`/`.signal`, offer-accept),
  ADR-0002 (protobuf wire — the feed copies the same `Event`). Drives a dependent desk rework
  (`rp1-web-consumer-auth`).

## Context

signaling-core authorizes an agent's browser by a **direct per-interaction grant**: on
offer-accept the auth-callout mints a connection that may `subscribe
tenant.<tid>.interaction.<id>.>`. That authorizes ONE interaction. The agent **inbox** is
inherently multi-interaction (many open chat threads + a live call at once). Re-deriving the inbox
as N direct `.log` subscriptions forces one of two bad shapes:

- a **per-interaction reconnect storm** (every newly-assigned thread = a token refresh + reconnect
  to widen the callout grant), or
- a **tenant-wide read grant** (`subscribe tenant.<tid>.interaction.*.log`), which hands every
  agent's browser read access to EVERY interaction in the tenant — breaking the per-interaction
  isolation signaling-core promises.

The in-flight desk change `rp1-web-consumer-auth` provisionally chose the tenant-wide read grant
and flagged it as an owner decision. Established systems (Matrix `/sync`, Slack Socket Mode, Stream
Chat, Twilio `UserConversation`) all use a different shape: a per-user channel the server fans
entitled facts into; the client never authorizes per-room.

## Decision

The agent inbox reads through a **per-agent fan-out feed** with a server-checked participation
boundary. This is a PINNED, deterministic authorization model — no per-room client grant, no
tenant-wide read.

1. **Feed subject + read grant.** One personal feed `tenant.<tid>.agent.<aid>.feed.>`
   (sub-subject per interaction). The auth-callout grants the inbox connection ONLY: feed
   subscribe (`<self>`-bound), a wildcard-interaction command publish with an ACL-pinned `<self>`
   identity suffix (`publish tenant.<tid>.interaction.*.cmd.<self>`), a per-connection minted reply
   prefix, and the agent's own offer/notify/presence. NO direct `.log` subscribe, NO tenant-wide
   read.

2. **Participation = `.log` facts (source A, confirmed).** Participation `(tenant, interaction,
   agent)` is derived SOLELY from `.log` facts (`participant.joined` / `participant.left` /
   `interaction.assigned`). There is no second control plane. Desk (a trusted backend) establishes
   participation by issuing a **privileged participation command** as
   `tenant.<tid>.interaction.<id>.cmd.<desk-svc-identity>`, which the router validates (actor role
   taken from the authenticated suffix identity + authz) and writes as the fact with audit fields
   (`actor`, `reason`, `request_id`). The fact — not the command — is the single source of truth.

3. **ACL-pinned command identity suffix + server-side authz (no write reconnect).** The `.cmd`
   subject GAINS an identity suffix `tenant.<tid>.interaction.<iid>.cmd.<identity>`, mirroring the
   existing `.signal.<userId>` precedent: the publisher's id is in the subject and the NATS
   publish-ACL — not a payload field — binds the command to its author. (NATS **subject-mapping**
   was considered to assert identity and REJECTED as unimplementable: mappings are subject-token
   transforms, never the connection's authenticated identity.) The inbox connection holds ONE
   `publish tenant.<tid>.interaction.*.cmd.<self>` grant for its lifetime (wildcard interaction,
   fixed `<self>` suffix; `*.cmd.<other>` denied). The **router** subscribes
   `tenant.*.interaction.*.cmd.*`, takes the publisher identity from the LAST subject token (never
   the payload; payload `actor_id` must match it, else `actor_mismatch`), derives the role from the
   authenticated identity, and enforces participation on every agent `.cmd` against the SAME
   `.log`-derived membership (the same `ParticipationView`) that drives the feed. A newly-assigned
   agent commands with no reconnect; a non-participant agent command is rejected and writes no fact;
   trusted-backend identities are exempt from the participant check. The `.cmd` SEMANTICS are
   unchanged; only the subject SHAPE gains the `<identity>` suffix — a breaking migration the router
   (subscribe `*.cmd.*`) and the SDK (publish `…cmd.<self>`) both adopt. **Write-identity
   precondition:** the suffix-ACL is airtight ONLY once the auth-callout mints a per-connection
   authenticated identity (replacing the dev shared `client` user); pinning `<self>` to that minted
   identity is a Phase-1 → production precondition tied to the auth-callout work (under shared
   `client` the suffix is advisory, not enforced).

4. **Fan-out service: leased single-active worker, serial fold, effectively-once.** The
   Participation/Fan-out service is a SINGLE active worker (ONE durable consumer on
   `tenant.*.interaction.*.log`; standby replicas behind a NATS KV leader lease, TTL ~5s) — NOT a
   sharded fleet, not engineered to higher availability than the single-node NATS + single router it
   derives from; NO partition subject-mapping, NO per-shard durables, NO rebalance protocol. The
   durable consumer is `MaxAckPending=1` (no prefetch, no concurrent projection): EXACTLY ONE `.log`
   fact is in flight at a time, so two fetchers can never fold facts N and N+1 concurrently or out of
   order and corrupt the stateful participation fold. A lease takeover follows a FIXED ordering:
   acquire the lease → WAIT for the prior holder's in-flight delivery's ack/redelivery to settle →
   ONLY THEN read `durable_ack_floor` and hydrate → go live (reading the floor before the prior
   delivery settles is forbidden — it could advance past an un-folded fact; takeover never overlaps
   in-flight processing). It hydrates from a KV
   participation snapshot that represents an **ACKED PREFIX ONLY** (the snapshot advances only after
   the source ack, so it is never ahead of the cursor): it loads the latest
   snapshot whose `seq <= durable_ack_floor`, read-only-folds `(snapshot_seq, ack_floor]` to go live,
   then resumes. A source `.log` message is acked ONLY after all intended per-agent feed publishes
   succeed;
   publishes carry a deterministic `Nats-Msg-Id = <tid>.<aid>.<iid>.<sequence>` for idempotent
   replay, so delivery is **effectively-once (at-least-once delivery + idempotent feed publish)** —
   NOT exactly-once. Partial-publish-then-crash and a lease-failover double-ownership window yield no
   drop and at-most-once per feed. Failures retry with backoff; poison facts go to a DLQ
   (`tenant.<tid>.agent.dlq.feed`), never silently dropped. A `{{partition(N,…)}}` sharded scale-out
   (subjects/semantics unchanged) is a documented future option triggered by measured lag.

5. **`_INBOX` isolation.** The reply-inbox is a per-connection minted prefix `_INBOX_<conn>.>`;
   broad `_INBOX.>` is denied. A feed-only read grant does not close command-result snooping
   (replies travel on `_INBOX`); the minted prefix does.

6. **History is desk REST, not RelayPoint (owner decision).** The feed carries LIVE facts from the
   agent's join point FORWARD ONLY. RelayPoint serves NO conversation history — no `feed.history`
   request/reply, no history grant, no `MAX_AUTO_BACKFILL`, no backfill-on-assignment, and no
   pre-join visibility gate. History is DESK's data (Postgres, source of truth, served by desk's REST
   API); ownership is orthogonal — desk owns the DATA, RelayPoint owns live DELIVERY. On
   open/assignment the browser loads prior messages from desk REST; on reconnect/gap it heals via a
   desk REST refetch (the existing rp1 pattern); it never reads `.log` directly. The live-feed authz
   is simply the membership interval `[join_seq, left_seq)` — no pre-join `[0, join_seq)` rule.

7. **Revocation epoch.** Membership is an interval `[join_seq, left_seq)`. Every feed projection is
   interval-guarded, so no post-revocation feed write occurs. Cold transfer keeps
   new-leg-before-old-revoked.

8. **Feed durability: ephemeral low-retention.** The feed is an ephemeral, short-`max_age`
   JetStream stream sized only to bridge a live disconnect gap; the canonical `.log` is RP's
   long-term/audit source and conversation history for the browser is desk REST (out of RP scope).
   Revocation writes a `feed.revoked` tombstone — the ONE feed message that is NOT a copied `.log`
   `Event`, but a small feed-control message type carrying only `{interaction_id, at_sequence}`; a
   consumer distinguishes feed-control from a projected `Event` by a type marker. Content may then be
   purged. The feed is never the audit record.

## Consequences

- **New container/service:** the RelayPoint Participation/Fan-out service — a leased single-active
  trusted-server JetStream consumer of `tenant.*.interaction.*.log` (durable, `MaxAckPending=1`),
  the only new publisher of `tenant.<tid>.agent.<aid>.feed.>`. Its core depends on owned ports
  (`ParticipationView`, `FeedSink`, `Cursor`), not on `nats.JetStreamContext` (loose-coupling HARD
  RULE).
- **Router gains** a server-side participant-authz check on every agent `.cmd.*` (taking the
  publisher identity from the last subject token, reusing the `ParticipationView` port) and a
  privileged participation-command path (`…cmd.<desk-svc-identity>`) that lands `participant.*` /
  `interaction.assigned` facts with audit fields.
- **Subject change:** the command subject's SEMANTICS are unchanged but its SHAPE gains an identity
  suffix `tenant.<tid>.interaction.<iid>.cmd.<identity>` (publisher in subject, ACL-enforced —
  mirrors `.signal.<self>`); the router migrates to subscribe `tenant.*.interaction.*.cmd.*` and the
  SDK migrates to publish `…cmd.<self>` (no compatibility shim; bare `.cmd` retired).
- **Auth-callout gains** a new inbox grant shape (feed-subscribe + `publish
  tenant.<tid>.interaction.*.cmd.<self>` (ACL-pinned suffix) + minted `_INBOX_<conn>.>`; no `.log`
  subscribe, no broad `_INBOX.>`).
- **New subjects:** `tenant.<tid>.agent.<aid>.feed.<interaction_id>` (server-write, agent-read-own),
  `tenant.<tid>.agent.dlq.feed` (operator-drained). RelayPoint exposes NO history subject —
  conversation history is desk REST, out of RP scope. `.log`/`.signal`/offer are unchanged; `.cmd`
  gains the `<identity>` suffix (semantics unchanged); the protobuf wire (ADR-0002) is reused
  verbatim (the feed copies the `Event`).
- **Dependent desk rework:** `rp1-web-consumer-auth` MUST consume the per-agent feed (drop
  tenant-wide read + direct `.log`; conversation history stays desk REST against Postgres —
  RelayPoint serves none; assignment via the privileged participation command as
  `…cmd.<desk-svc-identity>`; `publish …interaction.*.cmd.<self>` + minted `_INBOX_<conn>`). Tracked
  on the desk repo; not edited here.

Spec delta ids: `signaling.feed.inbox-reads-own-feed-only`, `signaling.feed.cross-agent-denied`,
`signaling.feed.unified-medium`, `signaling.feed.write-server-only`,
`signaling.feed.cmd-wildcard-no-reconnect`, `signaling.feed.cmd-nonparticipant-denied`,
`signaling.feed.cmd-identity-pinned`, `signaling.feed.privileged-assign-to-fact`,
`signaling.feed.privileged-actor-guarded`, `signaling.feed.privileged-transfer-ordering`,
`signaling.feed.fanout-to-participants`, `signaling.feed.participation-from-facts`,
`signaling.feed.fanout-dedup`, `signaling.feed.core-port-isolated`,
`signaling.feed.exactly-once-crash`, `signaling.feed.shard-ownership`,
`signaling.feed.serial-fold`, `signaling.feed.poison-dlq`,
`signaling.feed.inbox-prefix-isolated`, `signaling.feed.live-only-no-history`,
`signaling.feed.cursor-resume`, `signaling.feed.revoke-future-facts`,
`signaling.feed.transfer-no-gap`, `signaling.feed.ephemeral-bridge`,
`signaling.feed.revoke-tombstone`.

## Alternatives considered

- **Participation source B** (a separate `routing.participation.*` control plane) — a second source
  of truth that can diverge from `.log`; rejected (assignment is expressible as a `.log` fact).
- **Per-accepted-interaction `.cmd` publish grant** — a write-plane reconnect storm; rejected for
  the wildcard-interaction grant + ACL-pinned `<self>` suffix + server-side participant authz.
- **NATS subject-mapping to assert the publisher identity** — unimplementable: mappings are
  subject-token transforms, never the connection's authenticated identity; rejected for the
  ACL-pinned `.cmd.<self>` suffix (mirrors `.signal.<self>`).
- **Broad `_INBOX.>` reply grant** — a command-result snooping hole a feed-only read does not
  close; rejected for the minted `_INBOX_<conn>.>`.
- **RelayPoint serving conversation history at all** (replay-from-0 into the feed OR a bounded
  `feed.history` request/reply) — duplicates desk's source of truth, complicates live-feed authz
  with a pre-join visibility gate, and inflates the ephemeral stream; rejected (owner decision):
  history is desk REST, the feed is live-only from join forward.
- **Sharded fan-out fleet as the Phase-1 shape** — over-engineers the projector beyond the
  single-node NATS + single router it derives from; rejected for a leased single-active worker + KV
  snapshot hydration + ack-after-publish (effectively-once), with sharding demoted to a
  lag-triggered scale-out path.
- **Durable per-agent feed as the audit store** — duplicates `.log` N times under retention;
  rejected for ephemeral feed + canonical `.log` (RP audit) and desk REST (browser history).
- **Tenant-wide `.log` read grant** (desk's provisional choice) — breaks per-interaction isolation.
  Rejected.

## Amendment 2026-06-26 — concurrent per-fact fan-out

The single-active leased worker still processes one `.log` fact at a time (`MaxAckPending=1`, the
serial fold invariant is unchanged), but it now publishes that fact to its N recipient feeds
**concurrently** (bounded errgroup, `fanoutConcurrency=32`) instead of one synchronous `PublishMsg`
per agent in a loop. Motivation: a fact bound for N agents previously cost N sequential publish
RTTs inside the serial consumer, multiplying tail latency under load; the concurrent fan-out
collapses that to ~one RTT. Semantics are preserved exactly — ack only after **all** intended
publishes succeed (ack-after-publish), per-`(agent, interaction, sequence)` dedup id, and any
publish failure leaves the source un-acked (`Nak` → redelivery; already-published feeds dedup to a
no-op). This does **not** change the still-deferred sharding decision above: cross-interaction
concurrency (a partitioned consumer fleet) remains the lag-triggered scale-out path, gated on
measured projector backlog under concurrent interactions. `@spec: RDL-01`.
