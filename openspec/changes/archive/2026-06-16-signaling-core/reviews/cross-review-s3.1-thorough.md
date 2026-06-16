# Thorough cross-review (T1, parallel) — signaling-core S3.1
LOOSE-COUPLING: OK (both) — core depends on LogStore port, no NATS import, fake-store unit tests.
Both BLOCKED on correctness; all fixed:
- caused_by = command_id (was event_id) — matches the contract;
- getState singleflight (fixes stale-state resurrection vs concurrent eviction);
- Replay fails closed on real errors (no partial state); dup-ack resyncs state from log;
- rejected commands memoised → command_id conflict enforced; transient rejections re-evaluate;
- ref_id required for message edit/delete; stream renamed INTERACTION_LOGS (design).
- identity empty-fallback documented NOT production-safe (auth-callout required in prod).
Added tests: caused_by, ref_id, rejected-reuse-conflict. HIGH (prod identity) is a documented auth-callout deferral.

## codex
VERDICT: BLOCKED
RUBRIC:
R1 GAP — chat interaction states implemented are `"" -> started -> ended`; terminal reject/evict exists, but admitted `message.updated/deleted` have no message/ref state validation.
R2 GAP — covered variants: duplicate, conflict, illegal before start/after end, restart replay; missing/bugged variants: divergent retry after rebuild, ambiguous publish success, rejected-command reuse.
R3 OK — same-interaction writers serialize on `interactionState.mu` including `Append`; different interactions only share the map lock; `getState` double-check prevents two installed states.
R4 GAP — normal fake-store restart rebuilds sequence/status/results, but JetStream replay errors/partials can rebuild empty/partial state, and ambiguous publish success is not reconciled.
R5 GAP — client `.log` publish and `_INBOX` publish are denied by ACL; payload tenant and context `UserID` author checks exist, but deployed router passes empty identity so actor fallback is not safe.
R6 GAP — subjects/envelope mostly align, but `CommandResult.caused_by` contract is implemented/commented as `event_id` instead of `command_id`; stream name also drifts from design/tasks.
R7 GAP — deterministic JetStream `MsgId` exists, but replayed entries lose payload hash, rejected commands are not memoized, and duplicate publish acks do not advance state.
R8 GAP — 10 `// @spec:` ids exist, but `result-transport`, conflict-after-rebuild, no-log-on-reject, replay failure, `_INBOX` publish denial, and concurrency edges are not genuinely proven.
LOOSE-COUPLING OK — `router.go` imports no NATS and depends on `LogStore`; NATS is confined to `store.go` adapter and `cmd/router/main.go`; `fakeStore` unit tests prove no live NATS is required.

FINDINGS:
- [BLOCKER] internal/signaling/router.go:211 — Accepted `CommandResult.CausedBy` is set to `ev.EventID`, and rebuilt results use `e.EventID`; spec requires `caused_by = command_id`, while `envelope.go:37` also documents the wrong event-id meaning — set accepted/rebuilt result `CausedBy` from `cmd.CommandID` or `e.CausedBy`, and assert it in tests — R6/R8
- [BLOCKER] internal/signaling/router.go:183 — Rebuilt idempotency entries have empty `payloadHash`, so any post-restart/post-eviction reuse of the same `command_id` with a different payload replays the accepted result instead of returning conflict — persist command payload hash in the durable fact or a command-result ledger and rebuild it — R7
- [HIGH] internal/signaling/router.go:206 — If JetStream stores a fact but `Publish` returns an ambiguous error, the router returns rejected and leaves state stale; a later duplicate ack skips `seq/status` update, so following commands can be rejected against the wrong state — reconcile append errors/duplicate acks by replaying the persisted command fact before replying — R4/R7
- [HIGH] internal/signaling/store.go:45 — `Replay` treats any `Fetch` error as end-of-log and can return partial/empty facts; `rebuild` then installs that as authoritative state — return non-timeout errors, distinguish EOF from failure, and make `HandleCommand` reject/block when replay fails — R4/R7
- [HIGH] cmd/router/main.go:38 — production wiring always passes empty `Identity`, so the router does not enforce `actor_id == authenticated user`; this fallback is documented but not safe beyond dev/shared-client mode — require a trusted identity for deployed mode or gate empty identity behind explicit dev configuration — R5
- [MEDIUM] internal/signaling/router.go:191 — Rejected parsed commands are not memoized by `command_id`; the same key can later be reused with a different payload and produce an effect — store rejection result+payload hash or narrow the spec to accepted commands only — R7
- [MEDIUM] internal/signaling/router.go:73 — `message.updated` and `message.deleted` are legal while started but `RefID` and target existence are never validated, allowing unlinked edit/delete facts — require non-empty valid `ref_id` or remove these event types from the chat-subset until implemented — R1/R6
- [LOW] internal/signaling/store.go:63 — stream name is `INTERACTION_LOG`, while design/tasks name `INTERACTION_LOGS`; this is contract drift for deploy/ops references — align implementation or update the OpenSpec/design artifacts — R6

MISSING_TESTS:
- `signaling.cmd.result-transport`: assert accepted `caused_by == command_id`, rejected result includes reason, and result is not persisted.
- `signaling.cmd.command-id-conflict`: divergent reuse after restart/ended-eviction must reject as conflict.
- `signaling.cmd.idempotent-command-id`: ambiguous publish success/duplicate ack must not leave state stale.
- `signaling.security.payload-tenant-match`: assert no `.log` fact is written and subject/auth-tenant mismatch is covered.
- ACL: explicit client publish to `_INBOX.>` denied.
- Concurrency: simultaneous first commands for one interaction prove one installed state and monotonic sequence.
- JetStream replay: partial/error replay must fail closed; terminal `ended` rebuild rejects late commands.

NOTES:
- Deferred lifecycle/auth-callout/per-connection inbox isolation/presence/notifications/TS SDK were not counted as missing.
- Tests and `openspec validate --strict` were not run because this review was read-only.

## agy
VERDICT: BLOCKED
RUBRIC:
R1 OK — Thoughtful design; explicitly handles Phase-1 identity fallback and isolates NATS to edges.
R2 OK — Simple code, no speculative features.
R3 OK — Surgical changes focused exactly on the router refactor.
R4 OK — Unit and integration tests cover the scenarios cleanly.
R5 OK — N/A (single agent context).
R6 OK — See LOOSE-COUPLING below.
R7 OK — Comments explain why (e.g., Phase-1 fallback, lazy rebuild).
R8 OK — 10 spec tags exist and align with test scenarios.
LOOSE-COUPLING OK — The router core (`router.go`) depends strictly on the `LogStore` interface with no NATS imports. NATS is correctly confined to the adapter (`store.go`) and edges (`main.go`), proven by `fakeStore` unit tests without live NATS.
FINDINGS:
- [BLOCKER] internal/signaling/router.go:103 — Concurrency edge: `getState` has a double-checked locking race with eviction. If two callers miss the cache, both call `rebuild()` concurrently outside the global lock. If Caller A completes, processes `interaction.ended`, and evicts the state from `r.inter` BEFORE Caller B finishes `rebuild()`, Caller B will insert its stale state (missing the "ended" fact) back into `r.inter`. This resurrects the interaction and allows illegal commands to be appended. — Hold a per-interaction loading lock (e.g., `golang.org/x/sync/singleflight`) during `rebuild()` to prevent concurrent stale inserts. — Concurrency edge.
- [BLOCKER] internal/signaling/router.go:212 — In-memory state permanent desync on ambiguous JetStream publish. If `r.store.Append()` times out but succeeds in JetStream, the router returns an error without updating in-memory state. On retry, JetStream returns `dup=true`. The router skips incrementing `st.seq` and `applyTransition` because `!dup` is false, leaving the memory permanently out of sync with the durable log. — If `dup` is true but the command is missing from `st.results`, reload the interaction state from the log before continuing to sync memory with JetStream. — Correctness / exactly-once across crash.
- [MEDIUM] internal/signaling/router.go:191 — Unresolved prior defect: Rejections are not memoized. If a command is rejected (e.g., due to an illegal transition or payload mismatch), it is never stored in `st.results`. The client can retry the exact same `command_id` with a different payload and succeed, breaking the "key bound to original request" rule. — Store `payloadHash` and the rejection result in `st.results` for command-level rejections to enforce strict command-id conflict tracking. — Router-authoritative security / prior fixes.
- [LOW] internal/signaling/router.go:184 — After a restart, rebuilt states have an empty `payloadHash`. A retry with a reused `command_id` but a different payload will bypass the conflict check (`prev.payloadHash == ""`) and return the old result instead of a conflict error. — While it doesn't corrupt the log, it returns a false success. Document this as an acceptable limitation of lazy rebuilds, or structurally enforce consistent replays. — Correctness.
MISSING_TESTS: None functionally missing, though the test for `// @spec:signaling.unified-interaction` only verifies envelope fields (`Medium`, `CausedBy`) rather than actively proving chat/call shared logging (acceptable for now since call lifecycle is explicitly deferred in `tasks.md`).
NOTES: The ports-and-adapters refactor successfully isolates the core from NATS, which is a great architectural win. The two concurrency/state blockers must be fixed to ensure the router truly behaves as the authoritative, durable state-machine owner.
