# Delta for Projector (Participation/Fan-out service) — substrate-boundary invariant

Removes the tenant-wide **roster override** of structural fan-out so the projector's recipient set is
folded participation (`coveredBy`) ALONE. This restores the substrate boundary (desk ADR-0020,
*domain-free delivery substrates*): the delivery plane gates on membership STRUCTURE, never a desk
product rule ("every agent sees every interaction"). The projector still tails the shared
`tenant.*.interaction.*.log` (`INTERACTION_LOGS`) and writes `tenant.<tid>.agent.<aid>.feed.>`
(`AGENT_FEED`) + the DLQ subject; it holds a single-active KV leader lease. Owned ports are now
`LogSource`, `FeedSink`, `LeaseStore`, `SnapshotStore` — the `Roster` port is REMOVED.

## ADDED Requirements

### Requirement: Fan-out recipients come from folded participation alone — no roster override

The projector MUST derive a fact's recipient feeds SOLELY from folded participation
(`coveredBy(view, sequence)`): the agents whose membership interval `[JoinSeq, LeftSeq)` contains the
fact's sequence. There MUST be no tenant-wide override of that structural result — no `Roster` port,
no `DeskRoster` HTTP pull, and no static `TenantWideAgents` set — so "every agent sees every
interaction" (a desk product rule) cannot be re-imposed at the substrate. A non-participating tenant
agent MUST receive nothing; an agent participates iff a `participant.joined`/`participant.left` fold
on the interaction's own `.log` places its interval over the fact's sequence. As a consequence the
projector's per-fact critical path MUST NOT make any synchronous roster/HTTP call to desk.

#### Scenario: Recipients are exactly the folded participants, with no roster pull
- **id:** `substrate.no-roster-pull`
- **GIVEN** an interaction whose `.log` folds to participants {alice, bob} and a tenant agent `stranger` who never joined it
- **WHEN** the projector fans out a fact at a sequence covered by both alice's and bob's membership intervals
- **THEN** alice and bob each receive the fact and `stranger` receives nothing — the recipient set is `coveredBy(view, sequence)` ALONE
- **AND** there is no `Roster` port or roster/HTTP pull on the projector path (fan-out is structural-only), so no tenant-wide override can widen the set

## REMOVED Requirements

### Requirement: A roster outage retries unbounded; an empty roster soft-fails and is never cached

**Reason:** the roster override (`Config.Roster` / `DeskRoster` / `Config.TenantWideAgents`) is
deleted, so there is no roster lookup to retry, soft-fail, or cache. The scenarios
`projector.roster.unbounded-retry`, `projector.roster.empty-not-cached`, and
`projector.roster.empty-soft-fail` are retired with the behaviour. Fan-out is now structural-only
(`substrate.no-roster-pull`); an interaction with no folded participants simply has no recipients, so
the "empty roster" soft-fail case no longer exists.
