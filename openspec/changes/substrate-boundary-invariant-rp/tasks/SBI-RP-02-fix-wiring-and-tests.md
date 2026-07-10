---
id: SBI-RP-02
slice: SBI-RP
title: Fix cmd/projector wiring and re-seed projector tests from participation
status: done
specs: [substrate.no-roster-pull]
---

## Goal
Removing the roster override leaves dangling references: `cmd/projector` wires a fan-out-mode switch
that constructs a `DeskRoster`, and several projector tests injected a roster/`TenantWideAgents` only
to seed a recipient set. These must resolve without re-introducing the override.

## Success criteria
- `cmd/projector/main.go` no longer reads `PROJECTOR_FANOUT_MODE` / `DESK_ROSTER_*` /
  `PROJECTOR_TENANT_AGENTS`, constructs no `DeskRoster`, and drops the now-unused `parseTenantAgents`
  / `rosterCacheTTL` helpers and `net/http` / `strings` imports. No NEW env var added.
- Tests that used a roster/tenant-wide set purely to seed recipients now seed `participant.joined`
  facts instead and pass unchanged in intent (trace continuity, exhausted-delivery DLQ, concurrent
  fan-out dedup, fenced in-flight Nak).
- Roster-behaviour tests + `fakeRoster`/`errOnceRoster` are removed with the behaviour.
- `go build ./...`, `go vet`, `go test ./internal/projector/... ./internal/signaling/...
  ./internal/obs/... ./cmd/projector/...`, and the `-tags integration` projector suite are green;
  `gofmt -l` reports nothing.

## Files
- `cmd/projector/main.go` (drop the fan-out-mode switch + roster wiring + helpers + imports)
- `internal/projector/projector_test.go` (re-seed from participation; delete roster-behaviour tests)

## Spec
`// @spec:substrate.no-roster-pull` (shared with SBI-RP-01)

## Log
- done: `cmd/projector` now builds a fixed structural-fan-out `Config` (no mode switch, no
  `DeskRoster`); removed `parseTenantAgents`/`rosterCacheTTL` and `net/http`/`strings` imports.
  Re-seeded `TestProjector_PropagatesTraceFromLogToFeed`, `TestExhaustedDeliveryToDLQ`,
  `TestConcurrentFanoutAllRecipients`, `TestFencedInFlightPublishNaksNotAcks` from
  `participant.joined` facts; deleted `TestTenantWideFanoutShortcut`, `TestTenantRosterFanout`,
  `TestTenantRosterErrorRecoversInProcessNoNak`, `TestRosterErrorHeldViaInProgressThenBoundedNakNeverDLQ`,
  `TestEmptyRosterSoftFailNotDropped`, `fakeRoster`, `errOnceRoster`. All suites green incl.
  `-tags integration`; gofmt clean.
