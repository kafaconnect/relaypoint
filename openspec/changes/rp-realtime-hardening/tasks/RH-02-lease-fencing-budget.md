---
id: RH-02
slice: RH
title: CRITICAL — fence lease renewal within the TTL budget, stop-the-world on overdue renew, and review the 2 folded perf commits
status: todo
specs: [RDL-01, RDL-02, RDL-03]
---

## Goal
The lease-renew tolerance perf commit (`467c1c8`) widened the unfenced-processing window to ~3× the
lease TTL: `Renew` ignores `ctx` (rides the NATS ~5s default) and `renewWithRetry` (~15.6s) dwarfs
`TTL=5s`, so the worker keeps Delivering/fanning-out ~13s after a standby could re-`Create` the
lease → both `kv.Put("latest", …)` (no CAS) → snapshot corruption. Also formally review + spec the
two un-reviewed perf commits folded into this epic (concurrent fan-out `1f4309b` = `RDL-01`/`RDL-02`;
lease tolerance `467c1c8` = corrected by `RDL-03`).

## Success criteria (test-first)
- Unit tests for `renewWithRetry` budget + the fencing pause FIRST (today `fakeLease.Renew` always
  returns nil → zero coverage on the riskiest path).
- `Renew(ctx)` HONOURS `ctx`; the caller bounds each attempt with a per-attempt `ctx` timeout so
  total retry time `< (TTL − renewInterval)`; attempts×(timeout+backoff) DERIVED from the TTL.
- On an OVERDUE renew the worker pauses `process`/`Deliver` IMMEDIATELY (stop-the-world), not after
  3 failed attempts; no two holders ever both fan out or both snapshot.
- `RDL-01`/`RDL-02` scenarios (concurrent fan-out, ack-after-all, Nak-on-partial + dedup) pass and
  are now spec-anchored (they were tagged in `projector_test.go` but defined in no requirement).

## Files
- `internal/projector/nats.go` (`kvLease.Renew` honours `ctx`; per-attempt timeout)
- `internal/projector/projector.go` (`renewWithRetry` budget from TTL; overdue-renew pause of `process`/`Deliver`)
- `cmd/projector/main.go` (lease TTL/renewInterval feed the derived budget)
- `internal/projector/projector_test.go` (renew-budget + fencing-pause unit tests; `fakeLease` can fail/stall)

## Spec
`// @spec:RDL-01`, `// @spec:RDL-02`, `// @spec:RDL-03`

## Log
- todo
