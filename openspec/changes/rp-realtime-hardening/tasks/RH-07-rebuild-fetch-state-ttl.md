---
id: RH-07
slice: RH
title: MED — no-wait rebuild fetch plus bounded in-process router state
status: todo
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
- todo
