# Change: substrate-boundary-invariant-rp

RelayPoint companion to the desk change `substrate-boundary-invariant` (SBI). The desk-side slice
(SBI-01, merged in `kafaconnect/desk`) makes desk emit participation facts as its source of truth.
This slice (SBI-03) removes the corresponding leak in the substrate: the projector's tenant-wide
agent-**roster override** of structural fan-out.

## From

The projector's fan-out picks recipients from folded participation (`coveredBy(view, sequence)`), but
that structural result is then **overridden** by a tenant-wide roster whenever one is configured:

- `Config.Roster` (a `Roster` port fed by `DeskRoster`, an HTTP adapter that pulls
  `GET {desk}/tenants/<tid>/agents`) — every fact fans to EVERY agent the desk roster returns,
  regardless of whether that agent participates in the interaction.
- `Config.TenantWideAgents` — a static dev variant of the same override.

This bakes a **desk product rule** — "every agent sees every interaction" — into the signaling
substrate. RelayPoint's delivery plane is supposed to gate on delivery STRUCTURE (participation /
membership), never on a domain census of who-should-see-what. The override also puts a synchronous
HTTP dependency on desk (`DeskRoster`) directly on the projector's per-fact critical path, with its
own retry/soft-fail/cache machinery, purely to reconstruct a recipient set that folded participation
already yields once desk emits the facts.

## To

Fan-out is **structural-only**: `recipients := coveredBy(view, e.Sequence)` is the SOLE recipient
source. The roster override and its entire plumbing are deleted:

- the `case p.cfg.Roster != nil` / `case len(p.cfg.TenantWideAgents[...]) > 0` branch in `process`,
- `resolveRoster` (+ the unused `errRosterUnavailable`),
- the `Roster` port, `Config.{Roster,TenantWideAgents,RosterRetryWindow}` fields and their
  `withDefaults` logic,
- the `DeskRoster` HTTP adapter (`roster_http.go`) and its tests,
- the `PROJECTOR_FANOUT_MODE` / `DESK_ROSTER_*` / `PROJECTOR_TENANT_AGENTS` wiring in
  `cmd/projector`,
- the now-dead `relaypoint_projector_roster_errors_total` metric.

`coveredBy` and the `internal/signaling` participation fold are UNCHANGED — they were always the
correct structural source; this change just makes them the only one.

## Reason

This is the substrate-boundary fix (desk ADR-0020, *domain-free delivery substrates*): the substrate
must not carry a product's who-sees-what rule. Desk now owns participation as facts (SBI-01), so the
override is both wrong-in-principle and redundant. Removing it is **additive to the live cutover** and
low-risk: with desk emitting participation, `coveredBy` already produces the intended recipient set,
and the deleted paths were only exercised when a roster/tenant-wide override was explicitly wired.

## Deferred (NOT in this change — follow-ups)

Two adjacent substrate-boundary cleanups are intentionally OUT OF SCOPE here so this stays a
low-risk, roster-only removal (see `design.md` for detail):

1. **`RoleAgent` → capability gate / `GrantsFor` change.** Desk still authenticates connectors and
   agents with `role=agent`, so the existing `RoleAgent` grant model is left UNCHANGED — nothing
   breaks. Reworking role→capability grants is a separate authz change.
2. **`agent` → `subscriber` subject rename.** Renaming the feed subject family
   (`tenant.*.agent.*.feed.>`) is WIRE-BREAKING for connected clients (e.g. desk-web subscribes to
   that pattern) and needs coordinated client work. Subjects are kept AS-IS.

## Impact

- **Components:** `internal/projector/{projector.go,ports.go}` (delete roster branch + port + config
  fields); `internal/projector/roster_http.go` + `roster_http_test.go` (deleted); `internal/obs/
  metrics.go` + `metrics_test.go` (drop the roster_errors metric); `cmd/projector/main.go` (drop the
  fan-out-mode switch and roster wiring); `internal/projector/projector_test.go` (roster-injected
  fan-out tests re-seeded from participation; roster-behavior tests removed).
- **Streams/subjects:** none change. The projector still tails the shared
  `tenant.*.interaction.*.log` (`INTERACTION_LOGS`) and writes `tenant.<tid>.agent.<aid>.feed.>`
  (`AGENT_FEED`) + the DLQ subject. No subject rename (deferred).
- **Config/env:** REMOVED (no longer read) — `PROJECTOR_FANOUT_MODE`, `DESK_ROSTER_URL`,
  `DESK_ROSTER_TOKEN`, `DESK_ROSTER_TTL`, `PROJECTOR_TENANT_AGENTS`. No NEW env var is added.
- **Metrics:** `relaypoint_projector_roster_errors_total` removed (its only writer is gone).
- **Cross-repo:** consumes desk `substrate-boundary-invariant` (SBI-01) participation emission; aligns
  the substrate with desk ADR-0020. Desk repo is untouched by this change.
