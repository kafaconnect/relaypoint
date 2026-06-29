---
id: RH-01
slice: RH
title: CRITICAL — OCC token is the broker-committed stream seq, not ++ on the shared stream
status: todo
specs: [router.occ.committed-stream-seq]
---

## Goal
Stop the router guessing its per-subject OCC token with `streamSeq++`. On the SHARED
`INTERACTION_LOGS` stream, `ExpectLastSequencePerSubject` compares the GLOBAL stream sequence, so an
interleaving interaction makes the guess stale → spurious `ErrOCCConflict` on ~every concurrent-load
append, which also burns the single retry budget and wrongly rejects a coincident genuine race.

## Success criteria (test-first)
- A FAILING regression integration test first: interleave TWO interactions appending alternately on
  the shared stream; assert NO spurious OCC conflict and a dense/monotonic per-interaction sequence.
  (The current fake models OCC as a per-subject count and the integration test uses one interaction
  on a reset stream — neither can exhibit the bug; the fake must track the global stream seq.)
- `LogStore.Append` returns the committed message's stream sequence; `jetstreamStore` returns
  `ack.Sequence`; the router sets `st.streamSeq = <returned>` after a clean append, never `++`.
- Dup/error paths still self-correct via `Replay`/rebuild; all `router-occ` semantics preserved.

## Files
- `internal/signaling/store.go` (`LogStore.Append` signature + `jetstreamStore` returns `ack.Sequence`)
- `internal/signaling/router.go` (set `streamSeq` from the return at :402 and :626, drop `++`)
- `internal/signaling/router_unit_test.go` (fake tracks the global stream seq, not a per-subject count)
- `internal/signaling/router_occ_integration_test.go` (new interleaved-interactions regression test)

## Spec
`// @spec:router.occ.committed-stream-seq`

## Log
- todo
