---
id: V1-01
slice: V1
title: Document OCC/dedup ordering as broker-dependent + pin the OCC-before-dedup path with a unit test
status: done
specs: [router.occ.dedup-ordering-agnostic]
---

Fix the misleading `LogStore.Append` contract comment in `internal/signaling/store.go`: the
dedup-vs-OCC ordering is BROKER-DEPENDENT, not a guarantee (single-server R1 runs the
expected-subject check before dedup), so callers MUST treat `ErrOCCConflict` as "rebuild + re-check
command_id dedup" — which the router already does. Add a unit test (`router_unit_test.go`) with a
fake `LogStore` whose initial fold is one sequence behind the true tail and that returns
`ErrOCCConflict` on the first `Append` for an already-committed `command_id` (R1 OCC-before-dedup):
assert the router re-folds and returns the original cached accepted `CommandResult` (status
accepted, same `caused_by`), appending NO second fact. Documentation + regression test only — no
runtime behaviour change. No NATS in the test (loose coupling); no protobuf/desk change.

## Log
- 2026-06-11 done: store comment now states broker-dependent OCC/dedup ordering; new
  `TestCore_OCCBeforeDedupReplaysAccepted` exercises the R1 OCC-before-dedup path via in-memory fake
  (asserts accepted + caused_by + exactly one Append). `gofmt`/`go vet`/`go build`/`go test ./...`
  clean; `go test -tags integration ./internal/signaling/` green.
