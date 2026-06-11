# Design — agent-feed-fanout

## Context

signaling-core established: the router is the sole `.log` writer; clients are read-only on
`.log` and write `.cmd`; per-interaction read is granted by the **auth-callout** on offer-accept
(the connection reconnects with a token that adds `tenant.<tid>.interaction.<id>.>`). That
authorizes ONE interaction. The agent inbox needs MANY at once. This change adds a per-agent
**fan-out feed** as the inbox read surface and keeps the rest of signaling-core intact.

This design **aligns with signaling-core** (it does not supersede it): `.log`/`.cmd`/`.signal`,
offer-accept, the auth-callout, the "medium is a payload field" invariant, and router authority
are all unchanged. It adds a derived read projection and a server-side fan-out authority — which
the auth-callout section already anticipates as "additional trusted server-side authorities".

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

## Decision 1 — Feed subject layout + auth-callout grant

Subject: `tenant.<tid>.agent.<aid>.feed.<interaction_id>` (lowercase, dot-separated, ids carry
no dots — same rules as `.log`). `<aid>` = the agent's authenticated userId. One sub-subject per
interaction lets the client (and revocation) operate per-interaction while the client subscribes
the single wildcard `tenant.<tid>.agent.<aid>.feed.>`.

Auth-callout grant for an **inbox** connection (generalizes the per-interaction grant):

| Capability | Subject | Note |
|---|---|---|
| Subscribe (read) | `tenant.<tid>.agent.<self>.feed.>` | the ONLY inbox read grant; `<self>` = authenticated user |
| Publish (write) | `tenant.<tid>.interaction.<accepted>.cmd` | existing command plane, per accepted interaction |
| Subscribe own offer/notify/presence | `routing.offer.user.<self>(.control)`, `notify.<self>`, `presence.<self>` | unchanged from signaling-core |

The inbox connection holds **NO** `tenant.<tid>.interaction.*.log` subscribe and **NO** `.log`
write. A voice **media** leg still uses signaling-core's accept-reconnect to gain the narrow
`interaction.<id>.signal.<self>` + `.cmd` media scope — that is a SEPARATE, narrow grant and
NEVER widens inbox read scope.

## Decision 2 — Participation/Fan-out service (the crux)

A new trusted-server service tails the canonical log and projects facts into agent feeds.

```
tenant.*.interaction.*.log  --(JetStream durable consumer)-->  Fan-out service
                                                                 |  for each fact:
                                                                 |   1. update (tenant,interaction,agent) participation
                                                                 |   2. for each PARTICIPATING agent A:
                                                                 v        publish to tenant.<tid>.agent.<A>.feed.<iid>
```

**Participation source.** The service derives `(tenant, interaction, agent)` from facts the
router ALREADY writes on `.log`:

- `participant.joined` → add agent to the interaction's participant set.
- `participant.left` → remove agent (revocation, Decision 5).
- assignment / transfer facts (`interaction.assigned` on accept, `interaction.transferred` /
  `interaction.transfer.accepted`) → the canonical "who is on this interaction now" transitions.

So in the steady state the feed is driven **purely from `.log`** — no second source, no
client-asserted membership. The service is a projection of the same facts the inbox would
otherwise have read directly; it just enforces the participation check server-side.

> **OPEN QUESTION (crux — flagged).** This holds *iff RelayPoint owns assignment* (the router
> writes `participant.joined` / `interaction.assigned`). In signaling-core the router owns
> offer-accept → join, so it does. BUT the IMPLEMENTED router today only writes
> `participant.joined/left` it is *commanded* to (it has no offer/assignment engine yet — that is
> spec'd, not built). And in the desk integration, **Desk decides who is assigned**. Two
> resolutions, decision owed:
> - **(A) `.log`-only (preferred).** Whoever decides assignment (router engine, or Desk via a
>   `.cmd`) makes it land as a `participant.joined` / `interaction.assigned` FACT on `.log`. The
>   Fan-out service stays single-input (`.log`) and the feed is a pure projection. Desk feeds
>   participation in by issuing the command that becomes the fact — it does NOT call a separate
>   participation API.
> - **(B) Second input.** A participation control plane (`routing.participation.*` or an RP API)
>   the service ALSO consumes. Adds a source of truth that can diverge from `.log`; rejected
>   unless assignment authority genuinely cannot be expressed as a `.log` fact.
> The recommendation is **(A)**: keep the feed driven by `.log` so there is one ordering
> authority and the fan-out service has one input. This decision is owed to the owner /
> cross-review and is the single biggest risk in this change.

**Loose coupling.** The service core depends on owned ports — a `ParticipationView` (folds `.log`
facts into the participant set) and a `FeedSink` (publishes a projected fact to an agent feed) —
NOT on `nats.JetStreamContext`. It MUST be unit-testable with in-memory fakes (replay a fact
sequence, assert which agent feeds receive which facts), per the repo's HARD RULE.

## Decision 3 — Fact projection: copy vs pointer, ordering, dedup

**Copy the full event** (recommendation): the feed message IS the projected `.log` `Event`
(same envelope, same `sequence`). Matches the research baselines and lets the inbox render with
no extra fetch. The alternative (a pointer `{interaction_id, sequence}` the client resolves via
a server read command) keeps one stored copy but reintroduces a per-interaction read round-trip —
deferred to the open question.

- **Ordering.** Per-`<interaction_id>` order is the canonical router-assigned `sequence` —
  authoritative and preserved (the feed copies it). Across interactions there is no global order
  and none is needed: the inbox groups by interaction and orders each by `sequence`.
- **Dedup.** The feed message carries the source fact's `sequence` (and `event_id`); the client
  dedups per interaction by `sequence` exactly as a direct `.log` consumer would. The fan-out
  publish uses a deterministic `Nats-Msg-Id = <tenant>.<aid>.<iid>.<sequence>` so a redelivered
  fact (consumer redelivery / fan-out restart) projects at-most-once into each agent feed.
- **Gap-replay.** The client tracks a per-interaction last-applied `sequence` from the feed; on a
  gap it asks the service to backfill (Decision 4) — it never falls back to a direct `.log`
  subscribe.

**SDK consumption.** Today the SDK exposes `client.interaction(id).events()` (per-interaction
`.log` subscribe). The new surface is a single `client.agentFeed()` (or `inbox()`) that yields
`{interaction_id, event}` for all participating interactions over the one feed subscription. The
per-interaction `events()` stays valid for the narrow accepted-interaction/media case; the inbox
uses the feed. (SDK shape is a desk/SDK follow-up; this change specifies the wire surface.)

## Decision 4 — History / backfill on open/assignment

When an agent newly participates (assignment/open) it has no feed history for that interaction.
The Fan-out service backfills:

- On the `participant.joined` / `interaction.assigned` fact for agent A on interaction I, the
  service **replays `.log` for I from `sequence 0`** and projects each prior fact into
  `tenant.<tid>.agent.<A>.feed.<I>` (the same `Nats-Msg-Id` dedup keeps it exactly-once even if
  live projection races the backfill).
- The browser never reads `.log` directly for history. If history is large, the alternative is a
  **server-side read command** (`feed.history` request the service answers AFTER checking
  participation) that streams the range — same authority (service checks participation), no
  client `.log` grant. Which of "backfill-into-feed" vs "history-read-command" (or both, by
  size) is an open question; both keep the browser off direct `.log`.
- Cursor: the client persists the highest `sequence` it applied per interaction; on reconnect it
  resumes the feed and requests backfill only for gaps below its cursor.

## Decision 5 — Revocation on un-assign / transfer-away

On `participant.left` / un-assign / `interaction.transferred` (away from A), the service STOPS
projecting future facts of that interaction into `tenant.<tid>.agent.<A>.feed.<I>`: A is removed
from I's participant set, so subsequent facts are not fanned to A.

- **Future facts:** guaranteed stopped (participation check precedes every projection).
- **Already-delivered / retained feed:** policy decision owed (open question). Options: (a) the
  feed is NON-durable (core NATS) so nothing is retained and the client drops the interaction
  from its inbox on the `participant.left` it sees; (b) the feed is JetStream-durable for offline
  delivery, and on revocation the service writes a terminal `feed.revoked` marker and the
  retained range is governed by an audit/retention policy (not silently purged, for audit). The
  new-leg-before-old-revoked ordering of signaling-core's transfer is preserved: the NEW agent's
  feed gets the interaction (backfill) BEFORE the OLD agent's is revoked, so there is no gap.

## Decision 6 — Tenant isolation baseline

Hard `tenant.<tid>` subject prefix on the feed (`tenant.<tid>.agent.<aid>.feed.>`), enforced by
the auth-callout exactly like every other subject in signaling-core — NOT an account-level
shortcut. A connection authenticated for tenant A can subscribe only
`tenant.<A>.agent.<self>.feed.>`; `<self>` binding prevents reading another agent's feed within
the same tenant. (Phase-1 caveat: like signaling-core, full enforcement requires the auth-callout
minting per-connection scoped ACLs; the shared-`client` dev posture does not enforce it.)

## Desk impact (dependent follow-up — NOT edited here)

The desk change `rp1-web-consumer-auth` currently: subscribes the browser per-interaction via
`client.interaction(sessionId).events()` and, for the multi-thread inbox, recommends a **desk
auth-callout grant widened to tenant-wide read** (`subscribe tenant.<tid>.interaction.*.log`).
This model **forbids that** (no tenant-wide read, no direct `.log` for the browser). The desk
change MUST be REWORKED to:

- subscribe the inbox to `tenant.<tid>.agent.<aid>.feed.>` (one feed), not per-interaction `.log`;
- drop the tenant-wide read grant entirely; the desk-minted grant becomes feed-subscribe + `.cmd`;
- get history via feed backfill / the server-side history-read command, not a `.log` replay;
- render `message.created` from the SAME projected `Event` envelope (the feed copies the fact, so
  decoding is unchanged).

This is a tracked dependent follow-up on the DESK repo. This change does not edit desk.

## Open questions (consolidated — decisions owed)

1. **Participation source (crux).** (A) `.log`-only with Desk-as-commander vs (B) a second
   participation input — see Decision 2. Owner/cross-review decision; recommend (A).
2. **Projection.** Copy full event (recommended) vs pointer — Decision 3.
3. **Ordering scope.** Per-interaction `sequence` only (recommended) vs a feed-global order.
4. **Backfill mechanism.** Replay-into-feed vs server history-read-command vs both-by-size —
   Decision 4.
5. **Retained-feed on revocation.** Non-durable drop vs durable + `feed.revoked` + audit
   retention — Decision 5; couples to whether the feed is JetStream-durable.
6. **Feed durability.** Core NATS (ephemeral, simplest, no offline replay) vs JetStream (durable,
   offline inbox, enables backfill from the feed itself) — touches 4 and 5.

## Alternatives rejected

- **Tenant-wide `.log` read grant** (desk's provisional choice) — every agent's browser reads
  every interaction in the tenant; breaks signaling-core's per-interaction isolation. Rejected.
- **Per-interaction subscribe with reconnect-per-thread** — a reconnect storm for a
  many-thread inbox; the exact problem this change exists to remove. Rejected for the inbox
  (still valid for the single accepted-interaction media leg).
- **Client-asserted membership** (client tells the server which interactions to feed) — the
  client is untrusted; participation MUST be server-checked. Rejected.
