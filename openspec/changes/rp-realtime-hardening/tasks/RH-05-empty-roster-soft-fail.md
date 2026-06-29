---
id: RH-05
slice: RH
title: HIGH — empty roster (200, empty agents) must soft-fail and never be cached
status: done
specs: [projector.roster.empty-not-cached, projector.roster.empty-soft-fail]
---

## Goal
An empty roster (HTTP 200, empty agents) is cached 60s and acks+drops every fact (len(recipients)==0
→ nil → ack), so a tenant darks for a minute. `DeskRoster.Agents` caches whatever `fetch` returns —
including an empty success — and only HTTP errors are treated as non-cacheable.

## Success criteria (test-first)
- A FAILING empty-roster Nak test + an empty-not-cached test first.
- The projector treats an empty roster as a SOFT failure (Nak/retry), not ack+drop, unless a tenant
  legitimately has zero agents (bounded zero-agent path).
- `DeskRoster` caches ONLY non-empty results (an empty success is not stored), mirroring the
  "errors are not cached" intent, so the cache window can't dark a tenant.

## Files
- `internal/projector/projector.go` (empty-recipients-from-roster → Nak/soft-fail, not ack-drop)
- `internal/projector/roster_http.go` (`Agents` caches only non-empty `fetch` results)
- `internal/projector/roster_http_test.go` / `projector_test.go` (empty-Nak + empty-not-cached tests)

## Spec
`// @spec:projector.roster.empty-not-cached`, `// @spec:projector.roster.empty-soft-fail`

## Log
- done: projector roster arm now soft-fails an empty roster (200, no agents) → Nak/retry (never ack-drop, never DLQ, never fan to the empty set); a zero-agent tenant is indistinguishable from a mid-rebuild so we prefer retry (documented terse). `DeskRoster.Agents` caches ONLY non-empty results (empty success returned but not stored), mirroring "errors are not cached". Tests: `TestEmptyRosterSoftFailNotDropped` (@spec:projector.roster.empty-soft-fail) + `TestDeskRoster_EmptyNotCachedNonEmptyCached` (@spec:projector.roster.empty-not-cached). build/vet/race green.
- cross-review follow-up (with RH-04): the empty-roster path now also retries IN-PROCESS via `msg.InProgress()` (same bounded `Config.RosterRetryWindow`) before the `Nak` fallback, so a mid-rebuild window holds the delivery instead of burning `MaxDeliver` — still never ack-drops, never DLQs, never fans to the empty set. `DeskRoster` empty-not-cached behavior unchanged.
