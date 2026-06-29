---
id: RH-12
slice: RH
title: DoD — spec-tree sync, anchor RDL ids, ADRs, cross-reviews, CI green
status: todo
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
- todo
