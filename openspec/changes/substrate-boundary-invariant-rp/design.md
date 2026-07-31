# Design — substrate-boundary-invariant-rp

## Context

The projector is the leased single-active fan-out worker: it tails the shared
`tenant.*.interaction.*.log`, folds participation into a `ParticipationView` per interaction, and
projects each fact into the feeds of the agents who participate. The correct, structural recipient
set is `coveredBy(view, sequence)` — the agents whose membership interval `[JoinSeq, LeftSeq)`
contains the fact's sequence (agent-feed-fanout Decision 6; `internal/signaling/participation.go`).

An override was layered on top: when `Config.Roster` (or the static `Config.TenantWideAgents`) was
set, `process` discarded the structural result and fanned every fact to a **tenant-wide** agent set
pulled from desk. That encodes desk's product rule — "every agent sees every interaction" — inside
the substrate, which ADR-0020 (*domain-free delivery substrates*) says must not happen: the substrate
gates on delivery structure (participation/membership), never a domain census.

Desk's SBI-01 slice (merged in `kafaconnect/desk`) makes desk emit participation facts as its source
of truth, so `coveredBy` now yields the intended recipient set on its own. The override is therefore
both wrong-in-principle and redundant.

## Decision

Delete the roster override and make `coveredBy` the SOLE recipient source. Keep the participation
fold (`coveredBy`, `internal/signaling`) exactly as-is.

### What is deleted

- **`internal/projector/projector.go`**
  - the recipient-override `switch` in `process` (`case p.cfg.Roster != nil` → `resolveRoster`;
    `case len(p.cfg.TenantWideAgents[...]) > 0`) — leaving
    `recipients := coveredBy(view, e.Sequence)` as the only assignment;
  - `resolveRoster()` — the unbounded in-process roster retry loop;
  - `errRosterUnavailable` — now unused;
  - `Config.{Roster, TenantWideAgents, RosterRetryWindow}` and their `withDefaults()` defaulting;
  - the now-unused `log/slog` import and the per-fact `log` handle (only the roster branch logged).
- **`internal/projector/ports.go`** — the `Roster` port interface.
- **`internal/projector/roster_http.go`** + **`roster_http_test.go`** — the whole `DeskRoster` HTTP
  adapter (its only consumer was `Config.Roster`).
- **`internal/obs/metrics.go`** + **`metrics_test.go`** — `RosterErrors`
  (`relaypoint_projector_roster_errors_total`); its only writer was `resolveRoster`. The `Naks` help
  text no longer cites a "roster blip".
- **`cmd/projector/main.go`** — the `PROJECTOR_FANOUT_MODE` switch (`tenant-roster` / `tenant-wide`
  arms), the `NewDeskRoster` wiring, `parseTenantAgents`, `rosterCacheTTL`, and the now-unused
  `net/http` + `strings` imports.

### What stays (unchanged)

- `coveredBy(view, sequence)` — the structural recipient function.
- `internal/signaling/participation.go` — the fold (`ApplyFact`, intervals, `ParticipationView`).
- The `LogSource` / `FeedSink` / `LeaseStore` / `SnapshotStore` ports; lease fencing (RDL-03);
  the Nak→DLQ delivery guard; concurrent fan-out (RDL-01/02); the `AGENT_FEED` subject family.

### Tests

- `TestFanoutFromParticipationOnlyNoRosterPull` (`@spec:substrate.no-roster-pull`) — with the roster
  removed, fan-out is correct from folded participation ALONE (participants get the fact; a
  never-joined tenant agent gets nothing), and no `Roster` port exists to pull from on the projector
  path.
- Tests that previously injected a roster/`TenantWideAgents` purely to seed recipients
  (`TestProjector_PropagatesTraceFromLogToFeed`, `TestExhaustedDeliveryToDLQ`,
  `TestConcurrentFanoutAllRecipients`, `TestFencedInFlightPublishNaksNotAcks`) now seed participation
  `participant.joined` facts instead.
- The roster-behaviour tests are removed with the behaviour: `TestTenantWideFanoutShortcut`,
  `TestTenantRosterFanout`, `TestTenantRosterErrorRecoversInProcessNoNak`,
  `TestRosterErrorHeldViaInProgressThenBoundedNakNeverDLQ`, `TestEmptyRosterSoftFailNotDropped`, and
  the `fakeRoster` / `errOnceRoster` fakes. The `roster_http_test.go` suite is deleted with its file.

The removed `@spec:` ids (`projector.roster.unbounded-retry`, `projector.roster.empty-not-cached`,
`projector.roster.empty-soft-fail`) are retired by this change's spec delta (the roster requirement
they anchored no longer exists in the substrate).

## Deferred follow-up

The **`agent` → `subscriber` subject rename** remains deferred. The feed subject family
`tenant.*.agent.*.feed.>` is
   subscribed to by live clients (desk-web). Renaming it is a WIRE-BREAKING change requiring
coordinated client rollout, so subjects are kept AS-IS.

## Design discussion (agreed 20260731T022847Z)

The capability follow-up is now accepted after an independent cross-repo discussion using `agy`
(`gemini-3.6-flash-high`, read-only, 10-minute bound). The standalone `gemini` CLI was unavailable.
The agreed design brings capability authorization into this change.

Desk-minted subscriber tokens contain verified `tid`, opaque `sub`, and one exact capability:
`agent-feed` or `agent-feed-admin-operation-observer`. They omit both `role` and `cid`. RelayPoint
rejects subscriber capability combined with either claim, rejects unknown or missing capability,
and rejects legacy `role=agent` as a grant source.

`GrantsFor` keeps the visitor and trusted-backend role protocols, then selects the base subscriber
grant only from the exact capability profile. The observer profile adds only the same-tenant admin
operation hint. The `.agent.` token in feed subjects is retained as a legacy wire namespace and is
not interpreted as a product role.

The router accepts an ordinary non-backend writer only when it has a known subscriber capability.
Every non-start write remains gated by current folded participation. Privileged participation
commands remain trusted-backend-only. Dev HMAC minting and verification use the same exact
capability predicate, so development cannot bypass the production grant model.

Rejected alternatives are a role fallback, an unknown-capability base grant, and an immediate
wire-breaking `.subscriber.` subject migration.

## Alternatives considered

- **Keep `Roster` as a fallback when participation is empty.** Rejected — a participation-empty
  interaction has no structural recipients by definition; re-introducing a tenant-wide fallback would
  re-import the exact product rule ADR-0020 removes.
- **Leave `TenantWideAgents` as a dev-only shortcut.** Rejected — it is the same override with a
  static source; keeping it leaves a substrate leak reachable by config.
