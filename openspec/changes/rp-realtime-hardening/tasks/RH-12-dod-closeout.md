---
id: RH-12
slice: RH
title: DoD — spec-tree sync, anchor RDL ids, ADRs, cross-reviews, CI green
status: done
specs: [RDL-01, RDL-02, RDL-03]
---

## Goal
Close the Definition of Done for the epic, and repair the process debt the two un-reviewed perf
commits left. `openspec/specs/` is empty (only `.gitkeep`); the `RDL-01`/`RDL-02` ids are tagged in
tests but anchored in no spec; the lease-fencing decision (RH-02) changes the ADR-0003 leased-worker
model and needs its own ADR.

## Success criteria
- Run the spec-sync (`openspec sync` / the repo's sync step) to MATERIALIZE the live spec tree under
  `openspec/specs/` after the per-capability deltas land; the `RDL-01`/`RDL-02`/`RDL-03` ids resolve
  in the materialized tree (no longer dangling).
- ADR-0007 (lease-fencing, this change — Proposed → Accepted on apply) is recorded; a note is added
  to ADR-0002 / the `router-occ` decision for the OCC-token fix (RH-01).
- An INDEPENDENT cross-review is recorded per testable slice (builder ≠ reviewer); all C/H findings
  fixed; risk-tiered.
- `openspec validate --strict` passes (CLI not installed in the authoring env — run in CI/apply);
  lint/typecheck/`go test ./...` + `-tags integration ./...` green on ephemeral PG/NATS.
- Board Status + Release-Train reconciled on close (independent of milestone).

## Files
- `openspec/specs/**` (materialized by the sync, not hand-edited)
- `docs/architecture/decisions/0007-projector-lease-fencing.md` (Proposed stub in this change)
- `docs/architecture/decisions/0002-wire-format-protobuf.md` + the router-occ decision (OCC-token note)
- `openspec/changes/rp-realtime-hardening/reviews/` (cross-review records)

## Spec
`// @spec:RDL-01`, `// @spec:RDL-02`, `// @spec:RDL-03` (anchored into the materialized spec tree)

## Log
- done: DoD closeout. (1) Materialized the canonical spec tree (FIRST materialization) from this
  change's deltas → `openspec/specs/{observability,projector,authcallout,deploy,signaling-core}/spec.md`.
  (2) `@spec` resolution check over the Go tree: 89 distinct ids; all 26 ids this epic introduces
  (`RDL-01/02/03` + every RH-06..11 id) resolve in `openspec/specs/`. The remaining 63 are
  PRE-EXISTING (8 already-archived changes whose deltas were never synced + a set of orphan
  `authcallout.visitor.*`/`obs.*`/`web-call.*`/`f1.*` tags) — flagged, out of this change's delta
  scope. (3) ADR-0007 flipped Proposed→Accepted; ADR-0003 gained a reciprocal amendment noting the
  fence constrains its Decision 4. (4) Fixed a regression this epic's comment-sweep (7043bf9) caused:
  it stripped the `// program-output` logging-gate marker in `cmd/signaltest-inject/main.go`,
  breaking `check-logging.sh` — restored. (5) CI green (GOWORK=off): `check-logging.sh`,
  `check-skills-mirror.sh`, `go build/vet ./...`, `gofmt -l .` clean, `go test ./...` and
  `go test -count=1 -p 1 -tags integration ./...` all pass (7 pkgs; integration against a live
  JetStream NATS). RH-10 desk-side deploy lands in the kafaconnect/desk repo.
