# Delta for Signaling Core — agent fan-out feed

All subjects are prefixed `tenant.<tenantId>.` (omitted below for brevity), dot-separated,
lowercase; ids carry no dots/slashes — the same rules as `.log`. This delta ADDS a per-agent
fan-out **feed** as the agent-inbox read surface and a server-side **Participation/Fan-out
service** that projects canonical `.log` facts into it. The canonical `interaction.<id>.log` /
`.cmd` / `.signal` / offer subjects, the event envelope, and router authority are UNCHANGED; the
feed is a derived, read-only projection of the same facts. The feed message reuses the
signaling-core event envelope verbatim (it is a copy of the source `.log` `Event`, same
`sequence`/`event_id`). This is a PINNED, deterministic authorization boundary: participation is
derived SOLELY from `.log` facts (source A); the inbox `.cmd` grant is wildcard-interaction with an
ACL-pinned `<self>` identity suffix authorized server-side; the feed is ephemeral with the
canonical `.log` + a history-read command as the audit source.

## ADDED Requirements

### Requirement: Per-agent fan-out feed is the inbox read surface
The system MUST expose, per agent, ONE personal feed subject tree
`tenant.<tid>.agent.<aid>.feed.<interaction_id>` carrying the facts of every interaction that
agent currently participates in. An agent's **inbox** connection MUST read its inbox ONLY by
subscribing the single wildcard `tenant.<tid>.agent.<aid>.feed.>`; it MUST NOT subscribe any
`tenant.<tid>.interaction.*.log` directly and MUST NOT hold any tenant-wide `.log` read. `<aid>`
MUST be the connection's authenticated user — an agent reads only its own feed. The feed is
**write-only by the trusted server-side Fan-out service** and read-only for the agent; no client
may publish a feed subject. This generalizes signaling-core's per-interaction offer-accept grant
(ONE interaction) to a per-agent feed grant (MANY interactions) without re-granting or
reconnecting per interaction.

The feed MUST carry facts for ANY medium (chat / voice / video). Medium stays a payload field,
never a subject — there MUST be no per-medium feed fork and no per-medium inbox-read grant. A
voice **media** leg MAY still use signaling-core's accept-reconnect to obtain the narrow
`interaction.<id>.signal.<self>` media scope, but that grant MUST NOT widen inbox read scope and
MUST NOT be required for `.cmd` (the wildcard command grant below already covers media commands).

#### Scenario: Agent inbox reads only its own feed, never raw interaction logs
- **id:** `signaling.feed.inbox-reads-own-feed-only`
- **GIVEN** an agent `alice` authenticated for tenant `T`
- **WHEN** her inbox connection is authorized
- **THEN** it may `subscribe tenant.T.agent.alice.feed.>`
- **AND** it holds NO `subscribe tenant.T.interaction.*.log` and NO tenant-wide `.log` read

#### Scenario: One agent cannot read another agent's feed
- **id:** `signaling.feed.cross-agent-denied`
- **GIVEN** agent `alice` authenticated for tenant `T`
- **WHEN** she subscribes `tenant.T.agent.bob.feed.>`
- **THEN** NATS denies the subscription (`<aid>` is bound to the authenticated user)
- **Phase-1:** like signaling-core, full enforcement requires the auth-callout minting a per-connection, `<self>`-scoped feed ACL; the shared-`client` dev posture does not satisfy this.

#### Scenario: Chat and voice share the same feed and inbox grant
- **id:** `signaling.feed.unified-medium`
- **GIVEN** an agent participating in a chat interaction and a voice interaction
- **WHEN** facts for both are projected to the agent
- **THEN** both arrive on the SAME `tenant.<tid>.agent.<aid>.feed.<interaction_id>` tree, differing only by `event_type`/`medium` in the payload, under ONE inbox read grant (no per-medium auth fork)
- **AND** a voice media-leg reconnect grants only `interaction.<id>.signal.<self>` for that one interaction and never widens inbox read scope

#### Scenario: No client may publish a feed subject
- **id:** `signaling.feed.write-server-only`
- **GIVEN** a connected agent client
- **WHEN** it attempts to publish `tenant.<tid>.agent.<aid>.feed.<iid>`
- **THEN** NATS denies the publish (the feed is written only by the trusted Fan-out service)

### Requirement: Command publishes carry an ACL-pinned identity suffix; participation is authorized server-side (no write reconnect)
The command subject MUST carry an identity suffix `tenant.<tid>.interaction.<iid>.cmd.<identity>`,
mirroring the existing `.signal.<userId>` precedent: the publisher's id is in the subject and the
NATS publish-ACL — NOT a payload field — binds each command to its author. The inbox connection
MUST hold a SINGLE grant `publish tenant.<tid>.interaction.*.cmd.<self>` for its lifetime (wildcard
INTERACTION, FIXED `<self>` suffix; `*.cmd.<other>` MUST be denied) — NOT a per-accepted-interaction
grant — so a newly-assigned agent can issue commands with NO token refresh and NO reconnect, yet can
only publish AS ITSELF. The router MUST subscribe `tenant.*.interaction.*.cmd.*` and MUST take the
publisher identity from the LAST subject token, NEVER from the payload; a payload `actor_id` that
does not equal that suffix identity MUST be REJECTED (`reason: actor_mismatch`). The actor ROLE
(agent vs trusted-backend) MUST come from the identity the auth-callout authenticated, NEVER from
the payload. Because the wildcard interaction no longer scopes WHICH interaction an agent may
command, the **router MUST enforce participation server-side on EVERY agent-role `.cmd`**: the
publishing identity MUST be a CURRENT participant of the target interaction (an OPEN membership
interval, per the revocation-epoch requirement), checked against the same `.log`-derived
membership (the same `ParticipationView` fold) that drives the feed. A `.cmd` from a non-participant
agent MUST be REJECTED with a `CommandResult{REJECTED}` and MUST NOT produce any `.log` fact. The
router's participant check and the Fan-out service's projection check MUST use the SAME participation
view so the WRITE and READ planes cannot disagree.

#### Scenario: Newly-assigned agent commands without reconnect
- **id:** `signaling.feed.cmd-wildcard-no-reconnect`
- **GIVEN** agent `alice` whose inbox connection was authorized once with `publish tenant.T.interaction.*.cmd.alice`
- **WHEN** she is newly assigned interaction `I` (a `participant.joined{agent: alice}` fact on `interaction.I.log`) and publishes a command to `tenant.T.interaction.I.cmd.alice`
- **THEN** she does NOT refresh her token or reconnect to widen the publish grant
- **AND** the router takes her identity from the `alice` subject suffix and accepts the command because she is now a current participant of `I`

#### Scenario: Non-participant command is rejected server-side
- **id:** `signaling.feed.cmd-nonparticipant-denied`
- **GIVEN** agent `carol` who is NOT a participant of interaction `I` but holds `publish tenant.T.interaction.*.cmd.carol`
- **WHEN** she publishes a command to `tenant.T.interaction.I.cmd.carol`
- **THEN** the router replies `CommandResult{REJECTED, reason: not_a_participant}` on her reply-inbox
- **AND** no `.log` fact is written for `I`

#### Scenario: A client cannot publish a command as another identity
- **id:** `signaling.feed.cmd-identity-pinned`
- **GIVEN** agent `carol` granted only `publish tenant.T.interaction.*.cmd.carol`
- **WHEN** she attempts to publish to `tenant.T.interaction.I.cmd.alice` (another agent's suffix), or publishes to her own `…cmd.carol` with a payload `actor_id: alice`
- **THEN** the NATS publish-ACL denies the cross-identity subject, and the router REJECTS the payload-mismatch case with `reason: actor_mismatch` — neither produces a `.log` fact

### Requirement: Participation is established by a privileged command the router writes as a fact
Participation `(tenant, interaction, agent)` MUST be derived SOLELY from `.log` facts
(`participant.joined`, `participant.left`, `interaction.assigned`); the system MUST NOT consume any
second participation control plane. A trusted backend (Desk) MUST establish participation NOT by
calling a participation API but by issuing a **privileged assignment/participation command** on the
existing `.cmd` plane as `tenant.<tid>.interaction.<id>.cmd.<desk-svc-identity>` (`command_type` ∈
{`participant.assign`, `participant.unassign`, `participant.transfer`}). The router MUST: (1)
validate the ACTOR — the role MUST be derived from the authenticated suffix identity (the
`<desk-svc-identity>` last subject token), NEVER from the payload, and MUST carry the
trusted-backend role; an agent-role connection issuing a participation command MUST be rejected; (2)
validate AUTHZ (the target tenant/interaction is in the actor's scope); and (3) write the resulting
`participant.joined` / `interaction.assigned` / `participant.left` fact onto `interaction.<id>.log`
with AUDIT fields (commanding `actor` taken from the suffix, `reason`, `request_id`, `occurred_at`).
Trusted-backend identities MUST be EXEMPT from the agent participant check. The fact — not the
command — is the single source of truth for participation.

#### Scenario: Trusted backend assignment lands as an audited log fact
- **id:** `signaling.feed.privileged-assign-to-fact`
- **GIVEN** Desk (trusted-backend identity `desk`) authorized for tenant `T`
- **WHEN** it issues `participant.assign{interaction: I, agent: bob, reason: r, request_id: q}` on `tenant.T.interaction.I.cmd.desk`
- **THEN** the router derives the trusted-backend role from the `desk` suffix, validates authz, and writes `participant.joined{agent: bob}` on `interaction.I.log` carrying audit fields `actor=desk`, `reason=r`, `request_id=q`
- **AND** participation for `(T, I, bob)` is derived from that fact, not from any side channel

#### Scenario: Agent-role connection cannot issue a participation command
- **id:** `signaling.feed.privileged-actor-guarded`
- **GIVEN** an ordinary agent connection (agent role `alice`, not trusted-backend)
- **WHEN** it publishes `participant.assign{interaction: I, agent: self}` on `tenant.T.interaction.I.cmd.alice`
- **THEN** the router derives the agent role from the `alice` suffix, REJECTS it (actor validation fails), and writes no participation fact

### Requirement: Fan-out service projects log facts by server-checked participation
A trusted server-side **Participation/Fan-out service** MUST consume the canonical
`tenant.*.interaction.*.log`, maintain `(tenant, interaction, agent)` participation, and project
each fact into the feed of EVERY currently-participating agent
(`tenant.<tid>.agent.<aid>.feed.<interaction_id>`). It MUST be a trusted-server consumer, NEVER a
client, and MUST NOT modify the canonical `.log` (the `.log` remains the sole source of truth; the
feed is a derived projection). Participation MUST be **server-derived from facts**, never asserted
by the client: the service MUST add an agent on `participant.joined` / `interaction.assigned` and
remove the agent on `participant.left`. The service core MUST depend on owned ports (a
participation view over facts, a feed sink, a durable cursor, a bounded history reader), not on a
concrete NATS client, and MUST be unit-testable with an in-memory fake (replay a fact sequence,
assert the resulting per-agent feed projections).

The projected feed message MUST reuse the signaling-core event envelope unchanged (a copy of the
source `.log` `Event`, carrying the same router-assigned `sequence` and `event_id`). Per
interaction the feed MUST preserve the canonical `sequence` ordering; the fan-out publish MUST be
at-most-once per `(agent, interaction, sequence)` via a deterministic dedup id
(`Nats-Msg-Id = <tid>.<aid>.<iid>.<sequence>`), so a consumer redelivery or a fan-out restart does
not project a fact twice into an agent's feed. Cross-interaction global ordering is NOT required
(the inbox groups by interaction and orders each by `sequence`).

#### Scenario: A new message is fanned only to participating agents
- **id:** `signaling.feed.fanout-to-participants`
- **GIVEN** interaction `I` whose participant set (from `.log` facts) is agents `alice` and `bob`, and agent `carol` is not a participant
- **WHEN** the router appends a `message.created` fact on `interaction.I.log` with `sequence` N
- **THEN** the Fan-out service projects that fact (same envelope, same `sequence` N) into `tenant.<tid>.agent.alice.feed.I` and `tenant.<tid>.agent.bob.feed.I`
- **AND** it projects NOTHING into `tenant.<tid>.agent.carol.feed.I` (carol is not a participant)
- **AND** it does not modify `interaction.I.log`

#### Scenario: Participation is derived from facts, not client-asserted
- **id:** `signaling.feed.participation-from-facts`
- **GIVEN** the Fan-out service folding `interaction.I.log`
- **WHEN** it observes `participant.joined{agent: alice}` then later `participant.left{agent: alice}`
- **THEN** facts between join and leave are projected to `tenant.<tid>.agent.alice.feed.I`, and facts after the leave are not
- **AND** a client asserting it participates in `I` (without a `.log` join fact) receives nothing on its feed for `I`

#### Scenario: Redelivered fact projects at most once per agent feed
- **id:** `signaling.feed.fanout-dedup`
- **GIVEN** a fact at `sequence` N already projected to `tenant.<tid>.agent.alice.feed.I`
- **WHEN** the durable consumer redelivers that fact, or the Fan-out service restarts and re-reads it
- **THEN** the deterministic per-`(agent, interaction, sequence)` dedup id makes the feed store it once (the agent does not see a duplicate)

#### Scenario: Fan-out core is unit-testable without live NATS
- **id:** `signaling.feed.core-port-isolated`
- **GIVEN** the Fan-out service core behind owned participation-view + feed-sink + cursor + history-reader ports
- **WHEN** a sequence of `.log` facts is replayed through an in-memory fake
- **THEN** the test asserts exactly which agent feeds receive which facts, with no NATS client imported into the core (loose-coupling HARD RULE)

### Requirement: Fan-out is effectively-once across crash and failover (leased single-active worker)
The Participation/Fan-out service MUST run as a SINGLE active worker — ONE durable JetStream
consumer on `tenant.*.interaction.*.log`, with standby replicas contending for a NATS KV leader
lease (TTL ~5s, heartbeat-renewed) — NOT a sharded fleet; it MUST NOT use partition subject-mapping,
per-shard durables, or a rebalance protocol (it is not engineered to higher availability than the
single-node NATS + single router it derives from). Delivery MUST be **effectively-once
(at-least-once delivery + idempotent feed publish)** — the service MUST NOT claim exactly-once. A
source `.log` message MUST be acked ONLY after ALL intended per-agent feed publishes for that fact
are acknowledged; if the worker crashes after some but not all publishes, the un-acked source fact
MUST be redelivered and re-projected, and the deterministic dedup id MUST make the redelivery a
no-op — yielding NO drop and at-most-once per `(agent, interaction, sequence)`. The service MUST
**hydrate** by loading a KV snapshot of the participation view (keyed by stream sequence, written
every N facts/seconds), doing a read-only fold of the tail up to the durable ack floor, then going
live — NOT replaying the whole log from sequence 0. The durable consumer's ack floor MUST be the
recovery cursor; a failed publish MUST be retried with backoff WITHOUT advancing the cursor; a fact
failing past `max_deliver` MUST be routed to a dead-letter subject (`tenant.<tid>.agent.dlq.feed`)
with the failure reason and source `event_id`/`sequence` (never silently dropped). A transient
double-ownership window across lease failover MUST remain safe via the same dedup. A
`{{partition(N,…)}}` sharded scale-out (one durable per shard, subjects/semantics unchanged) MAY be
added later, triggered by measured single-consumer lag, but is NOT part of this requirement.

#### Scenario: Partial publish then crash drops nothing and duplicates nothing
- **id:** `signaling.feed.exactly-once-crash`
- **GIVEN** a fact at `sequence` N to be fanned to agents `alice` and `bob`, where the active worker publishes to `alice`'s feed then crashes BEFORE publishing to `bob` and BEFORE acking the source
- **WHEN** the un-acked source fact is redelivered (to the same worker or the standby that wins the lease)
- **THEN** the redelivery re-publishes to both feeds; `alice`'s feed dedups `sequence` N to one copy and `bob`'s feed now receives it
- **AND** the source is acked only after BOTH publishes succeed (no drop, at-most-once per feed)

#### Scenario: Failover resumes from the lease, snapshot, and durable cursor with no SPOF on the data path
- **id:** `signaling.feed.shard-ownership`
- **GIVEN** the Fan-out service running as one active worker holding the KV leader lease plus standby replicas
- **WHEN** the active worker dies and a standby acquires the expired lease
- **THEN** the new active worker hydrates from the latest KV participation snapshot, read-only-folds the tail up to the durable ack floor, and resumes live projection from the durable cursor with per-interaction `sequence` order preserved and no lost or duplicated projection

#### Scenario: Poison fact is dead-lettered, not wedged
- **id:** `signaling.feed.poison-dlq`
- **GIVEN** a fact whose projection fails repeatedly (e.g. malformed envelope) past `max_deliver`
- **WHEN** the delivery limit is reached
- **THEN** the service routes it to `tenant.<tid>.agent.dlq.feed` with the failure reason and source `event_id`/`sequence`, acks the source so the consumer is not wedged, and an operator can drain the DLQ

### Requirement: Reply-inbox is a per-connection minted prefix (no command-result snooping)
The system MUST mint a per-connection reply prefix for the inbox connection and grant ONLY that
prefix. The request/reply replies for the inbox connection (the `.cmd` `CommandResult` and the
history-read response) MUST land on a per-connection reply prefix `_INBOX_<conn>.>`, where `<conn>` is
a high-entropy per-connection token bound to the connection by the auth-callout. The auth-callout
MUST grant `subscribe`/`publish` ONLY for that connection's `_INBOX_<conn>.>` and MUST DENY broad
`_INBOX.>` and any other connection's `_INBOX_<other>.>`. The SDK MUST use `_INBOX_<conn>` as its
inbox prefix so every request replies into its own scoped prefix. A feed-only read grant does NOT by
itself close command-result snooping (replies travel on `_INBOX`, not the feed); this requirement
closes it.

#### Scenario: A second client cannot snoop another connection's command results
- **id:** `signaling.feed.inbox-prefix-isolated`
- **GIVEN** two connected clients in tenant `T`, each granted only its own `_INBOX_<conn>.>`
- **WHEN** client 1 issues a `.cmd` or history-read whose reply lands on `_INBOX_<conn1>` and client 2 attempts to subscribe `_INBOX.>` or `_INBOX_<conn1>.>`
- **THEN** NATS denies client 2's subscription, so client 2 cannot observe client 1's CommandResult or history-read reply
- **Phase-1:** full enforcement requires the auth-callout minting the `<conn>`-scoped reply ACL; the shared-`client` dev posture does not satisfy this.

### Requirement: Feed history backfill is a bounded participation-checked read command (not replay-from-0)
The feed MUST carry only LIVE facts from the agent's assignment forward; the system MUST NOT seed an
agent feed by replaying `interaction.<id>.log` from `sequence 0`. Prior history MUST be served by a
bounded, paginated, participation-checked **history-read request** on
`tenant.<tid>.agent.<self>.feed.history` (reply on the connection's `_INBOX_<conn>`), which the
Fan-out service answers ONLY after confirming the requesting `<self>` has a membership interval
covering the requested range. The request carries `{interaction_id, from_sequence, to_sequence?,
limit, direction}`; the service reads `.log` over the range capped at a server `limit` and returns
ascending `sequence` with a `next_cursor` when truncated. A range below the interaction's retained
`.log` floor MUST be REJECTED (`out_of_retention`); a `.log` read error MUST yield
`CommandResult{REJECTED, reason: history_unavailable}`. A fresh assignment MUST auto-backfill at most
`MAX_AUTO_BACKFILL` facts; older history is fetched on-demand via the same command. The browser MUST
track a per-interaction feed cursor and, on reconnect or a detected gap, history-read ONLY the
missing range — never falling back to a direct `.log` subscribe.

#### Scenario: Newly-assigned agent gets bounded history via the read command, not direct log
- **id:** `signaling.feed.backfill-on-assignment`
- **GIVEN** interaction `I` with existing `.log` facts at `sequence` 1..N and agent `bob` newly assigned (a `participant.joined{agent: bob}` fact)
- **WHEN** `bob`'s inbox requests `feed.history{interaction: I, ...}` for up to `MAX_AUTO_BACKFILL` recent facts
- **THEN** the Fan-out service confirms `bob`'s membership, reads `.log` for the bounded range, and replies with ascending facts (plus `next_cursor` if truncated)
- **AND** `bob`'s browser never subscribes `interaction.I.log` directly, and the feed itself is never seeded from `sequence 0`

#### Scenario: History read denied for a non-participant
- **id:** `signaling.feed.history-participation-checked`
- **GIVEN** agent `carol` who has no membership interval for interaction `I`
- **WHEN** she sends `feed.history{interaction: I, ...}`
- **THEN** the service REJECTS it (`not_a_participant`) and returns no `.log` data

#### Scenario: Reconnect resumes from the feed cursor without a log subscribe
- **id:** `signaling.feed.cursor-resume`
- **GIVEN** an agent that applied feed facts for `I` up to `sequence` M, then disconnects
- **WHEN** it reconnects and resumes `tenant.<tid>.agent.<aid>.feed.>`
- **THEN** it history-reads only for `sequence > M` (the gap) and resumes live, never opening a direct `interaction.I.log` subscription

### Requirement: Membership is an interval and every feed write is epoch-guarded (no post-revocation write)
Membership MUST be modeled as a half-open interval `[join_seq, left_seq)` keyed by `(tenant,
interaction, agent)`: a `participant.joined` / `interaction.assigned` at `.log` `sequence` J opens
`[J, ∞)`; a `participant.left` / un-assign / transfer-away at `sequence` L closes it to `[J, L)`.
Every projection AND every queued/in-flight backfill MUST be guarded by this interval so NO
post-revocation feed write occurs: a fact at `sequence` S is projected to agent A ONLY if S falls in
an OPEN interval of A for that interaction, and a backfill carries the interval it was authorized
under so that a `participant.left` closing the interval CANCELS the remaining backfill — a
`participant.left` racing a `participant.joined` backfill MUST NOT deliver any post-revocation fact.
For a cold transfer the new-leg-before-old-revoked ordering MUST be preserved: the NEW agent's
`participant.joined` (opening its interval + triggering its backfill) MUST be applied BEFORE the OLD
agent's `participant.left` is folded, so the interaction is never absent from both inboxes at once.
The router's `.cmd` participant check MUST use the SAME intervals (an OPEN interval authorizes a
command), so a revoked agent's late `.cmd` is rejected for the same reason its feed stops.

#### Scenario: Future facts stop the instant the interval closes
- **id:** `signaling.feed.revoke-future-facts`
- **GIVEN** agent `alice` with an OPEN interval `[J, ∞)` for `I`, receiving its facts on `tenant.<tid>.agent.alice.feed.I`
- **WHEN** a `participant.left{agent: alice}` fact lands at `sequence` L (closing the interval to `[J, L)`)
- **THEN** the `participant.left` fact at L is projected (so the client drops `I`), and NO fact at `sequence >= L` is ever projected to `alice` for `I`

#### Scenario: A leave racing a join-backfill cancels the backfill
- **id:** `signaling.feed.revoke-cancels-backfill`
- **GIVEN** agent `alice` newly joined `I` (backfill in flight) when a `participant.left{agent: alice}` is folded before the backfill drains
- **WHEN** the interval closes
- **THEN** the remaining queued/in-flight backfill is cancelled and no post-revocation fact is delivered to `alice` for `I`

#### Scenario: Transfer fans the new agent before revoking the old
- **id:** `signaling.feed.transfer-no-gap`
- **GIVEN** interaction `I` transferred from `alice` to `bob` (cold transfer)
- **WHEN** the Fan-out service applies the transfer facts
- **THEN** `bob`'s interval opens and its backfill is triggered BEFORE `alice`'s interval is closed, so the interaction is never absent from both inboxes at once (new-leg-before-old-revoked, mirroring the call leg handover)

### Requirement: Feed is ephemeral low-retention; canonical log + history-read is the audit source
The agent feed MUST be an EPHEMERAL, short-`max_age` JetStream stream sized ONLY to bridge a live
disconnect gap — it MUST NOT be the long-term/audit store. The canonical `interaction.<id>.log`
(retained per audit policy) plus the bounded `feed.history` read command MUST be the long-term /
audit source; the feed MUST NOT duplicate `.log` per-agent for retention. Feed messages MUST age out
by `max_age`. On revocation the service MUST write a terminal `feed.revoked{interaction_id,
at_sequence}` tombstone into the agent's `…feed.<iid>` so a reconnecting client deterministically
drops the interaction even if it missed the `participant.left`; post-revocation feed content MAY then
be purged (the `.log` retains the audit copy). The feed MUST NOT be silently used as the audit record.

#### Scenario: Feed is the disconnect-gap bridge, log is the audit record
- **id:** `signaling.feed.ephemeral-bridge`
- **GIVEN** the agent feed configured with a short `max_age` and the canonical `.log` retained per audit policy
- **WHEN** an agent is briefly offline and reconnects within `max_age`
- **THEN** it catches up from the retained feed window without a history-read; history older than `max_age` is fetched via the `feed.history` command against the canonical `.log`

#### Scenario: Revocation writes a tombstone, then content may be purged
- **id:** `signaling.feed.revoke-tombstone`
- **GIVEN** a revoked agent feed for `I`
- **WHEN** the service applies the revocation
- **THEN** it writes a terminal `feed.revoked{interaction_id: I, at_sequence: L}` so a reconnecting client drops `I`, after which the agent's `…feed.I` content MAY be purged (the canonical `.log` retains the audit copy; the feed is never the audit record)
