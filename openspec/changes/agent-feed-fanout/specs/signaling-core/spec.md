# Delta for Signaling Core — agent fan-out feed

All subjects are prefixed `tenant.<tenantId>.` (omitted below for brevity), dot-separated,
lowercase; ids carry no dots/slashes — the same rules as `.log`. This delta ADDS a per-agent
fan-out **feed** as the agent-inbox read surface and a server-side **Participation/Fan-out
service** that projects canonical `.log` facts into it. The canonical `interaction.<id>.log` /
`.cmd` / `.signal` / offer subjects, the event envelope, and router authority are UNCHANGED; the
feed is a derived, read-only projection of the same facts. The feed message reuses the
signaling-core event envelope verbatim (it is a copy of the source `.log` `Event`, same
`sequence`/`event_id`).

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
`interaction.<id>.signal.<self>` + `.cmd` media scope, but that grant MUST NOT widen inbox read
scope (it never adds feed or `.log` read beyond the one media interaction).

#### Scenario: Agent inbox reads only its own feed, never raw interaction logs
- **id:** `signaling.feed.inbox-reads-own-feed-only`
- **GIVEN** an agent `alice` authenticated for tenant `T`
- **WHEN** her inbox connection is authorized
- **THEN** it may `subscribe tenant.T.agent.alice.feed.>` and the `.cmd` plane for accepted interactions
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
- **AND** a voice media-leg reconnect grants only `interaction.<id>.signal.<self>`+`.cmd` for that one interaction and never widens inbox read scope

#### Scenario: No client may publish a feed subject
- **id:** `signaling.feed.write-server-only`
- **GIVEN** a connected agent client
- **WHEN** it attempts to publish `tenant.<tid>.agent.<aid>.feed.<iid>`
- **THEN** NATS denies the publish (the feed is written only by the trusted Fan-out service)

### Requirement: Fan-out service projects log facts by server-checked participation
A trusted server-side **Participation/Fan-out service** MUST consume the canonical
`tenant.*.interaction.*.log`, maintain `(tenant, interaction, agent)` participation, and project
each fact into the feed of EVERY currently-participating agent
(`tenant.<tid>.agent.<aid>.feed.<interaction_id>`). It MUST be a trusted-server consumer, NEVER a
client, and MUST NOT modify the canonical `.log` (the `.log` remains the sole source of truth; the
feed is a derived projection). Participation MUST be **server-derived from facts**, never asserted
by the client: the service MUST add an agent on the participation facts the router writes
(`participant.joined`, and the assignment/transfer-accept facts) and remove the agent on
`participant.left` / un-assign / transfer-away. The service core MUST depend on owned ports (a
participation view over facts and a feed sink), not on a concrete NATS client, and MUST be
unit-testable with an in-memory fake (replay a fact sequence, assert the resulting per-agent feed
projections).

The projected feed message MUST reuse the signaling-core event envelope unchanged (a copy of the
source `.log` `Event`, carrying the same router-assigned `sequence` and `event_id`). Per
interaction the feed MUST preserve the canonical `sequence` ordering; the fan-out publish MUST be
at-most-once per `(agent, interaction, sequence)` (a deterministic dedup id), so a consumer
redelivery or a fan-out restart does not project a fact twice into an agent's feed. Cross-interaction
global ordering is NOT required (the inbox groups by interaction and orders each by `sequence`).

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
- **GIVEN** the Fan-out service core behind owned participation-view + feed-sink ports
- **WHEN** a sequence of `.log` facts is replayed through an in-memory fake
- **THEN** the test asserts exactly which agent feeds receive which facts, with no NATS client imported into the core (loose-coupling HARD RULE)

### Requirement: Feed history backfill keeps the browser off the canonical log
When an agent newly participates in an interaction (assignment / open), the system MUST make that
interaction's prior history available to the agent WITHOUT a direct `.log` subscribe. On the
`participant.joined` / assignment fact for agent A on interaction I, the Fan-out service MUST
either replay `interaction.I.log` from `sequence 0` and project each prior fact into
`tenant.<tid>.agent.A.feed.I`, OR answer a server-side history-read request for I after checking
A's participation — in both cases the SERVICE is the only authority that reads `.log` on the
agent's behalf, and the browser authorizes by feed-subscribe only. The agent MUST track a
per-interaction feed cursor (highest applied `sequence`) and, on reconnect or a detected gap,
resume from the cursor and request backfill only for the missing range — never falling back to a
direct `.log` subscribe.

#### Scenario: Newly-assigned agent gets history via the feed, not direct log
- **id:** `signaling.feed.backfill-on-assignment`
- **GIVEN** interaction `I` with existing `.log` facts at `sequence` 1..N and agent `bob` newly assigned (a `participant.joined{agent: bob}` fact)
- **WHEN** the Fan-out service observes the join
- **THEN** it makes facts 1..N available to `bob` via `tenant.<tid>.agent.bob.feed.I` (replayed into the feed or served by a participation-checked history read)
- **AND** `bob`'s browser never subscribes `interaction.I.log` directly

#### Scenario: Reconnect resumes from the feed cursor without a log subscribe
- **id:** `signaling.feed.cursor-resume`
- **GIVEN** an agent that applied feed facts for `I` up to `sequence` M, then disconnects
- **WHEN** it reconnects and resumes `tenant.<tid>.agent.<aid>.feed.>`
- **THEN** it requests backfill only for `sequence > M` (the gap) and resumes live, never opening a direct `interaction.I.log` subscription

### Requirement: Feed revocation stops future facts on un-assign / transfer-away
The system MUST stop projecting future facts of interaction I into
`tenant.<tid>.agent.A.feed.I` once agent A leaves I — on `participant.left`, un-assignment, or
transfer-away — by removing A from I's participant set before the next projection. For a cold transfer the new-leg-before-old-revoked
ordering of signaling-core MUST be preserved: the NEW agent's feed MUST receive I (its backfill)
BEFORE the OLD agent's feed is revoked, so there is no inbox gap on the interaction. The policy
for ALREADY-delivered / retained feed content on revocation MUST be defined: either the feed is
non-durable so nothing is retained (the agent drops I from its inbox on the `participant.left` it
sees), or the feed is durable and the service writes a terminal `feed.revoked` marker for I with
retained content governed by an audit/retention policy (not silently purged).

#### Scenario: Future facts stop after the agent leaves
- **id:** `signaling.feed.revoke-future-facts`
- **GIVEN** agent `alice` participating in `I` and receiving its facts on `tenant.<tid>.agent.alice.feed.I`
- **WHEN** a `participant.left{agent: alice}` (or un-assign / transfer-away) fact lands on `interaction.I.log`
- **THEN** the Fan-out service projects no further facts of `I` into `tenant.<tid>.agent.alice.feed.I`

#### Scenario: Transfer fans the new agent before revoking the old
- **id:** `signaling.feed.transfer-no-gap`
- **GIVEN** interaction `I` transferred from `alice` to `bob` (cold transfer)
- **WHEN** the Fan-out service applies the transfer facts
- **THEN** `bob`'s feed receives `I` (its backfill) BEFORE `alice`'s feed for `I` is revoked, so the interaction is never absent from both inboxes at once (new-leg-before-old-revoked, mirroring the call leg handover)

#### Scenario: Retained-feed policy on revocation is explicit
- **id:** `signaling.feed.revoke-retention-policy`
- **GIVEN** a revoked agent feed for `I`
- **WHEN** the feed-durability mode is non-durable versus JetStream-durable
- **THEN** non-durable retains nothing (the agent drops `I` on the `participant.left` it observes), and durable writes a terminal `feed.revoked` marker for `I` and retains prior content only per the audit/retention policy (never silently purged)
