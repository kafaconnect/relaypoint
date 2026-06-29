---
id: RH-05
slice: RH
title: HIGH — empty roster (200, empty agents) must soft-fail and never be cached
status: todo
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
- todo
