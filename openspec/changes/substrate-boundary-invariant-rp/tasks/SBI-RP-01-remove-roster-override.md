---
id: SBI-RP-01
slice: SBI-RP
title: Delete the tenant-wide roster override so fan-out is structural-only (coveredBy)
status: done
specs: [substrate.no-roster-pull]
---

## Goal
The projector overrides structural fan-out (`coveredBy(view, sequence)`) with a tenant-wide roster
whenever `Config.Roster` or `Config.TenantWideAgents` is set, baking desk's "every agent sees every
interaction" product rule into the substrate (against ADR-0020). Desk now emits participation facts
(SBI-01), so the override is both wrong-in-principle and redundant.

## Success criteria (test-first)
- A FAILING `@spec:substrate.no-roster-pull` test first: with no roster, fan-out is correct from
  folded participation ALONE (participants get the fact; a never-joined tenant agent gets nothing),
  and no `Roster` port exists to pull from on the projector path.
- `recipients := coveredBy(view, e.Sequence)` is the SOLE recipient source in `process` — the
  override `switch` is gone.
- `resolveRoster`, `errRosterUnavailable`, the `Roster` port, and `Config.{Roster, TenantWideAgents,
  RosterRetryWindow}` are deleted; `roster_http.go` is deleted.
- The dead `relaypoint_projector_roster_errors_total` metric is removed.

## Files
- `internal/projector/projector.go` (delete override switch, `resolveRoster`, `errRosterUnavailable`,
  Config roster fields + `withDefaults` roster default, unused `log/slog`)
- `internal/projector/ports.go` (delete the `Roster` port)
- `internal/projector/roster_http.go`, `roster_http_test.go` (delete)
- `internal/obs/metrics.go`, `metrics_test.go` (drop `RosterErrors`)

## Spec
`// @spec:substrate.no-roster-pull`

## Log
- done: removed the roster/tenant-wide override branch in `process` — `coveredBy(view, e.Sequence)` is
  now the sole recipient source; deleted `resolveRoster`, `errRosterUnavailable`, the `Roster` port,
  `Config.{Roster,TenantWideAgents,RosterRetryWindow}` + their defaulting, `roster_http.go` +
  `roster_http_test.go`, and the `relaypoint_projector_roster_errors_total` metric. New test
  `TestFanoutFromParticipationOnlyNoRosterPull` (@spec:substrate.no-roster-pull). build/vet/test green;
  gofmt clean.
