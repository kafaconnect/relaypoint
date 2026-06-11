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
   subscribe (`<self>`-bound), a WILDCARD command publish, a per-connection minted reply prefix,
   and the agent's own offer/notify/presence. NO direct `.log` subscribe, NO tenant-wide read.

2. **Participation = `.log` facts (source A, confirmed).** Participation `(tenant, interaction,
   agent)` is derived SOLELY from `.log` facts (`participant.joined` / `participant.left` /
   `interaction.assigned`). There is no second control plane. Desk (a trusted backend) establishes
   participation by issuing a **privileged participation command** on the existing `.cmd` plane,
   which the router validates (actor role + authz) and writes as the fact with audit fields
   (`actor`, `reason`, `request_id`). The fact — not the command — is the single source of truth.

3. **Wildcard command publish + server-side authz (no write reconnect).** The inbox connection
   holds ONE wildcard `publish tenant.<tid>.interaction.*.cmd` for its lifetime; the **router**
   enforces participation on every agent `.cmd` against the SAME `.log`-derived membership that
   drives the feed. A newly-assigned agent commands with no reconnect; a non-participant command is
   rejected and writes no fact.

4. **Fan-out service: sharded, exactly-once, HA.** The Participation/Fan-out service is SHARDABLE
   stateless workers (partition by `(tenant, interaction)` hash; durable consumer groups / leases;
   no SPOF). A source `.log` message is acked ONLY after all intended per-agent feed publishes
   succeed; publishes carry a deterministic `Nats-Msg-Id = <tid>.<aid>.<iid>.<sequence>` for
   idempotent replay. Partial-publish-then-crash yields no drop, no duplicate. Failures retry with
   backoff; poison facts go to a DLQ (`tenant.<tid>.agent.dlq.feed`), never silently dropped.

5. **`_INBOX` isolation.** The reply-inbox is a per-connection minted prefix `_INBOX_<conn>.>`;
   broad `_INBOX.>` is denied. A feed-only read grant does not close command-result snooping
   (replies travel on `_INBOX`); the minted prefix does.

6. **Backfill = bounded history-read command (not replay-from-0).** The feed carries LIVE facts
   from assignment forward only. Prior history is served by a bounded, paginated,
   participation-checked `tenant.<tid>.agent.<self>.feed.history` request the service answers after
   a membership check; a `MAX_AUTO_BACKFILL` threshold caps the auto-load. The browser never reads
   `.log` directly.

7. **Revocation epoch.** Membership is an interval `[join_seq, left_seq)`. Every projection and
   every queued backfill is interval-guarded, so no post-revocation feed write occurs; a
   `participant.left` racing a `participant.joined` backfill cancels it. Cold transfer keeps
   new-leg-before-old-revoked.

8. **Feed durability: ephemeral low-retention.** The feed is an ephemeral, short-`max_age`
   JetStream stream sized only to bridge a live disconnect gap; the canonical `.log` + the
   history-read command are the long-term/audit source. Revocation writes a `feed.revoked`
   tombstone; content may then be purged. The feed is never the audit record.

## Consequences

- **New container/service:** the RelayPoint Participation/Fan-out service — a sharded
  trusted-server JetStream consumer of `tenant.*.interaction.*.log`, the only new publisher of
  `tenant.<tid>.agent.<aid>.feed.>` and responder to `…feed.history`. Its core depends on owned
  ports (`ParticipationView`, `FeedSink`, `Cursor`, `HistoryReader`), not on
  `nats.JetStreamContext` (loose-coupling HARD RULE).
- **Router gains** a server-side participant-authz check on every agent `.cmd` (reusing the
  `ParticipationView` port) and a privileged participation-command path that lands `participant.*`
  / `interaction.assigned` facts with audit fields.
- **Auth-callout gains** a new inbox grant shape (feed-subscribe + wildcard `.cmd` + minted
  `_INBOX_<conn>.>`; no `.log` subscribe, no broad `_INBOX.>`).
- **New subjects:** `tenant.<tid>.agent.<aid>.feed.<interaction_id>` (server-write, agent-read-own),
  `tenant.<tid>.agent.<aid>.feed.history` (participation-checked request/reply),
  `tenant.<tid>.agent.dlq.feed` (operator-drained). `.log`/`.cmd`/`.signal`/offer are unchanged;
  the protobuf wire (ADR-0002) is reused verbatim (the feed copies the `Event`).
- **Dependent desk rework:** `rp1-web-consumer-auth` MUST consume the per-agent feed (drop
  tenant-wide read + direct `.log`; history via `feed.history`; assignment via the privileged
  participation command; wildcard `.cmd` + minted `_INBOX_<conn>`). Tracked on the desk repo; not
  edited here.

Spec delta ids: `signaling.feed.inbox-reads-own-feed-only`, `signaling.feed.cross-agent-denied`,
`signaling.feed.unified-medium`, `signaling.feed.write-server-only`,
`signaling.feed.cmd-wildcard-no-reconnect`, `signaling.feed.cmd-nonparticipant-denied`,
`signaling.feed.privileged-assign-to-fact`, `signaling.feed.privileged-actor-guarded`,
`signaling.feed.fanout-to-participants`, `signaling.feed.participation-from-facts`,
`signaling.feed.fanout-dedup`, `signaling.feed.core-port-isolated`,
`signaling.feed.exactly-once-crash`, `signaling.feed.shard-ownership`,
`signaling.feed.poison-dlq`, `signaling.feed.inbox-prefix-isolated`,
`signaling.feed.backfill-on-assignment`, `signaling.feed.history-participation-checked`,
`signaling.feed.cursor-resume`, `signaling.feed.revoke-future-facts`,
`signaling.feed.revoke-cancels-backfill`, `signaling.feed.transfer-no-gap`,
`signaling.feed.ephemeral-bridge`, `signaling.feed.revoke-tombstone`.

## Alternatives considered

- **Participation source B** (a separate `routing.participation.*` control plane) — a second source
  of truth that can diverge from `.log`; rejected (assignment is expressible as a `.log` fact).
- **Per-accepted-interaction `.cmd` publish grant** — a write-plane reconnect storm; rejected for
  the wildcard grant + server-side participant authz.
- **Broad `_INBOX.>` reply grant** — a command-result snooping hole a feed-only read does not
  close; rejected for the minted `_INBOX_<conn>.>`.
- **Backfill by replaying `.log` from sequence 0 into the feed** — re-injects whole-thread history
  into the ephemeral stream and races live projection; rejected for the bounded history-read.
- **Single fan-out instance** — a SPOF with no exactly-once guarantee; rejected for sharded workers
  + ack-after-publish.
- **Durable per-agent feed as the audit store** — duplicates `.log` N times under retention;
  rejected for ephemeral feed + canonical `.log`/history-read.
- **Tenant-wide `.log` read grant** (desk's provisional choice) — breaks per-interaction isolation.
  Rejected.
