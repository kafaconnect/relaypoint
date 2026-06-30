---
id: RH-07
slice: RH
title: MED — no-wait rebuild fetch plus bounded in-process router state
status: done
specs: [router.rebuild.no-wait-fetch, router.state.idle-evict]
---

## Goal
`Replay` pays a ~250ms pull-`Fetch(128, MaxWait 250ms)` tail that blocks to expiry on a drained/
under-full subject — on first access AND every OCC conflict AND every dup (compounds with RH-01).
Separately, in-process state is unbounded: the `inter` map (no TTL/LRU) + per-interaction `results`
(one entry per `command_id`, never pruned) are evicted only on ended/poison, so long-lived open
interactions grow router memory at ME multi-tenant scale (no router KV snapshot, unlike the projector).

## Success criteria (test-first)
- A test asserting a drained-subject rebuild returns immediately (no `MaxWait` tail) and yields
  identical state; a test asserting idle eviction + transparent rebuild-on-next-access.
- `Replay` issues a `no_wait` fetch (or `GetLastMsg`/ordered-consumer/direct-get) so a drained
  subject returns immediately.
- `inter` map + `results` gain idle-TTL/LRU eviction (state is rebuildable from the log); the memory
  model is documented.

## Files
- `internal/signaling/store.go` (`Replay` no-wait fetch / direct read)
- `internal/signaling/router.go` (idle-TTL/LRU eviction of `inter` + `results`)
- `internal/signaling/router_unit_test.go` / integration test (no-wait + eviction tests)

## Spec
`// @spec:router.rebuild.no-wait-fetch`, `// @spec:router.state.idle-evict`

## Log
- done. `store.go`: `Replay` no longer pays the ~250ms `Fetch(128, MaxWait)` tail. It now bounds the
  drain by the subject's last STREAM seq via `GetLastMsg` (real-time, no lag) and re-checks it after
  each drain pass — an empty subject returns before any consumer, a drained one stops the instant it
  reads the last fact, and the re-check catches a straggler appended mid-drain so the OCC token stays
  current under contention (every productive fetch returns immediately; `replayFetchMaxWait` is only a
  hung-server safety bound). Chosen primitive: `GetLastMsg`-targeted drain + re-check (not a literal
  pull no_wait, whose legacy resend re-blocks; not a subject-count, which lags under concurrent appends
  and caused an OCC poison-storm).
- `router.go`: `inter` gained idle-TTL + LRU eviction (`pruneLocked` on insert; `lastUsed` stamped on
  every access; defaults `maxInter=4096`, `idleTTL=30m`) and `results` a per-interaction FIFO dedup
  cache via `putResult` (default `maxResults=1024`). Both are rebuildable from the log, so eviction is
  safe (broker dedup + re-fold cover an evicted command_id). Tunables are `WithStateLimits(...)` /
  defaulted struct fields — no new env.
- Companion (enabled by the now-cheap rebuild): the OCC loser re-folds up to `maxRefold` (default 32)
  times before poisoning, restoring the liveness the old 250ms-rebuild tail provided as accidental
  backoff; without it a tailless rebuild let tight single-subject contention exhaust the 1-retry budget
  and poison-cascade. `TestOCC_ConcurrentSingleInteraction` stays 10/10.
- New tests: `// @spec:router.rebuild.no-wait-fetch` `TestReplay_DrainedSubjectNoMaxWaitTail`
  (integration: empty + populated-drained replay both <100ms, identical state, deterministic token);
  `// @spec:router.state.idle-evict` `TestCore_IdleEvictionRebuildsOnNextAccess` and
  `TestCore_LRUCapAndResultsBounded` (idle + LRU eviction, bounded results, transparent rebuild-on-
  next-access with no double-append after eviction).
- Follow-up (cross-review FIX 2): bounding `results` exposed that after BOTH the 2-min broker dedup
  window AND in-memory eviction, a retry of a still-legal committed command_id on a >`maxResults`-long
  interaction could double-append. Fixed by setting the stream's `Duplicates` to `logStreamDedupWindow`
  (1h) so the BROKER is the durable exactly-once authority within the window and the bounded cache is a
  safe fast-path (`signaling.stream.dedup-window`). NOTE: unbounded per-command dedup is a DEFERRED
  future option; the `Duplicates` window is the current guarantee boundary.
