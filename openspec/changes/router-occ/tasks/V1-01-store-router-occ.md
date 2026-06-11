---
id: V1-01
slice: V1
title: Per-subject OCC on the log append — store port + router retry + live-NATS test
status: done
specs: [router.occ.expected-subject-seq]
---

`LogStore.Append` takes an expected last-subject-sequence and publishes with
`nats.ExpectLastSequencePerSubject(...)`; a wrong-last-sequence APIError (10071) maps to the
retryable sentinel `ErrOCCConflict`, every other error fails closed. `Replay` also returns the
subject's last STREAM sequence (the OCC token, distinct from the dense per-interaction `sequence`).
`HandleCommand` carries the token, and on an OCC conflict re-folds ONCE and retries; a second loss
is a retryable rejection. All prior semantics (dedup, divergent-payload conflict,
illegal-transition reject, sole-writer, dup/poison reconcile) preserved. New integration test:
two routers over one stream prove no duplicate sequence under contention and the loser
re-folds/retries; it catches the bug (duplicate seq) when OCC is removed.

## Log
- 2026-06-11 done: store/router OCC + unit-fake OCC + live-NATS concurrency test; `gofmt`/`go vet`/`go build` clean, `go test ./...` green, `go test -tags integration ./internal/signaling/` green (TestOCC_* pass; bug reproduced when OCC neutered). commit 7a3cfad
