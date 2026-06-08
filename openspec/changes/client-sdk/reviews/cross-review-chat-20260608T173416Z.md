# Cross-review — client-sdk (chat subset implementation)

- Builder: claude  ·  Reviewers: codex, agy (independent, parallel, read-only)
- Base: change/signaling-core  ·  HEAD: 5972741  ·  20260608T173416Z
- Scope: clients/typescript (@relaypoint/client chat subset), ADR-0001, CI, tasks
- Review tier: T1 (foundational shared lib) — fix bar: BLOCKER/HIGH to zero; MEDIUM fixed or logged-with-justification

## Verdicts
- codex: BLOCKED
- agy: PASS_WITH_FINDINGS

Both rated LOOSE_COUPLING = OK (core imports only the Transport port; nats.ws confined to
src/adapters/nats.ts; tests run against FakeTransport — no live NATS).

## Findings + resolutions
| # | Sev | Finding | Resolution |
|---|-----|---------|-----------|
| 1 | BLOCKER (codex) / HIGH (agy) | connect() awaited transport.connect() with no catch → stuck in "connecting" if it rejects | FIXED — connect() catches, sets "disconnected" (non-auth) and rethrows; test `leaves connecting for disconnected…` |
| 2 | HIGH (agy) | reconnect() halted forever if transport.connect() threw (nats auto-reconnect disabled) | FIXED — shared establish() retries transport.connect() with connectBackoffMs; test `retries transport.connect on reconnect…` |
| 3 | BLOCKER (codex) | media_profile dropped from LogEvent/decoder (normative wire mapping) | FIXED — added mediaProfile to LogEvent + decode media_profile; asserted in wire-field-mapping test |
| 4 | HIGH (codex) / MEDIUM (agy) | Mailbox single-consumer not enforced → silent fact-split | FIXED — second concurrent iterator throws; test `refuses a second concurrent consumer` |
| 5 | MEDIUM (codex) | NatsWsTransport.publish is a generic write path (could target .log) | ACCEPTED-WITH-JUSTIFICATION — the Transport is a generic port *by design* (loose coupling); the no-log-write guarantee is enforced by the server NATS ACL (deploy/nats/nats-server.conf) + the SDK command API (InteractionHandle has no log-write). Documented in adapters/nats.ts. |
| 6 | LOW (codex, R2) | empty authBackoffMs → zero getToken attempts | FIXED — tokenWithRetry guarantees ≥1 attempt |

## Contract note (both reviewers)
Spec text "dedup on Nats-Msg-Id = event_id" is stale vs the server (it sets Nats-Msg-Id to the
command-derived publish-dedup id). The SDK dedups/orders by the authoritative router `sequence`
and treats `event_id` as fact identity — both reviewers judged this sound. Clarified in
design.md ("Implementation note (chat subset)") so the event-id-header dedup is not reintroduced.

## Outcome
All BLOCKER/HIGH fixed to zero; the one accepted MEDIUM is confined to the adapter/edge with a
documented justification (loose-coupling exception). Suite: 20 tests / 21 @spec scenarios green;
typecheck + build clean; openspec validate --strict passes.

---
### codex (raw)
VERDICT: BLOCKED
RUBRIC:
R1 GAP — ConnectionState: disconnected→connecting→connected, connected→reconnecting→connected, connected→disconnected(final), any→closed, getToken exhaustion→failed; blank: initial `transport.connect()` rejection leaves `connecting` with no exit. DeliveryState: live→replaying on gap, replaying/degraded→live when filled, degraded→failed on exhaustion, failed terminal.
R2 GAP — reconnect/token-expiry, retry, duplicate same sequence, out-of-order/gap, replay exhaustion covered; blanks: initial connect transport failure, empty `authBackoffMs`, multiple event consumers, and `media_profile` max-contract field.
R3 GAP — live offers during replay resolve via `pending` keyed by sequence; overlapping reconnects via `reconnecting`; send retry via same `command_id`; recovery via `recovering`; blank: Mailbox claims single-consumer but does not enforce it.
R4 GAP — replay unreachable is bounded/fail-closed; getToken failure bounded; send drop retries bounded/idempotent; blank: initial transport connect failure can strand state.
R5 GAP — `send()` targets `.cmd`; `signal()` targets `.signal.<self>`; no sequence setter on handle; gap: public `NatsWsTransport.publish(subject, data)` export exposes arbitrary subject publish, including `.log`, relying only on server ACL.
R6 GAP — command/result/log camelCase mapping mostly precise and `LogEvent` carries `causedBy`, not `commandId`; blank: `mediaProfile↔media_profile` is required by spec but absent from type and decoder.
R7 OK — command retry reuses `command_id`; delivery orders/dedups by router `sequence`; `occurredAt` display-only; known `Nats-Msg-Id=event_id` deviation is sound if sequence remains SSoT.
R8 GAP — implemented `[x]` scenario ids all have `// @spec:` tests and vitest uses `FakeTransport`; `pnpm typecheck`, `pnpm test`, and build-config `tsc --noEmit` pass; `openspec validate` not runnable because CLI is unavailable in PATH.

LOOSE_COUPLING:
OK — core files import only `Transport`/local modules; `nats.ws` import is confined to `clients/typescript/src/adapters/nats.ts:8`; tests use `FakeTransport`.

FINDINGS:
- [BLOCKER] clients/typescript/src/client.ts:65 — `connect()` sets `connecting`, then awaits `transport.connect(token)` without catch; if the transport connect rejects, state remains `connecting` indefinitely — catch non-auth connect failure, transition to a defined state (`disconnected` or `failed`), emit it, rethrow, and add a FakeTransport test — R1/R4
- [BLOCKER] clients/typescript/src/codec.ts:57 — wire `media_profile` is required by the normative mapping, but `WireEvent`, `decodeLogEvent()`, and `LogEvent` omit `mediaProfile` — add `media_profile?: string` decoding to `mediaProfile?: string` in `types.ts`, and test it under `clientsdk.cmd.wire-field-mapping` — R6
- [HIGH] clients/typescript/src/mailbox.ts:33 — `Mailbox` is documented as single-consumer but every `events()` iterator can wait on the same queue, so concurrent consumers split facts silently — enforce one active iterator or implement broadcast semantics, and test concurrent consumers — R3/R7
- [MEDIUM] clients/typescript/src/index.ts:8 — public export of `NatsWsTransport` exposes generic `publish(subject, data)`, so the SDK surface still has an arbitrary subject write path despite “no public log-write path” — keep the raw adapter internal or expose a constrained adapter API; server ACL should remain defense-in-depth — R5

MISSING_TESTS:
none for implemented `[x]` scenario ids; coverage gaps are listed in FINDINGS

CONTRACT_RISKS:
`Nats-Msg-Id = event_id` text is stale versus server command-based publish id; sequence-based SDK dedup/order is sound, but spec/test wording should be updated to avoid future reviewers reintroducing event-id dedup. `media_profile` drift is a real contract failure.

QUESTIONS:
Is `ConnectionState.failed` intended to be terminal against manual `connect()` calls, or only terminal for automatic retry loops?

---
### agy (raw)
VERDICT: PASS_WITH_FINDINGS
RUBRIC:
R1 GAP — ConnectionState `connecting` has no failure sibling transition if `transport.connect()` throws; it becomes an orphaned state.
R2 OK — all lifecycle edges (reconnect, token-expiry, duplicate, gap, exhaustion, empty/zero/max) are handled properly.
R3 OK — resolvers: Delivery singleflight/concurrent offers (`recovering` boolean lock); overlapping reconnects (`reconnecting` boolean lock); send retry (router `commandId` reuse); Mailbox single-consumer (JS single-threading run-to-completion + waiters array).
R4 GAP — `transport.connect()` is not retried in `reconnect()` if it throws; a sustained network drop will permanently disconnect the client after the first failed reconnection attempt.
R5 OK — send only targets `.cmd`, signal hardcodes own `actorId` via context, no public log-write path exposed.
R6 OK — camelCase <-> snake_case is 1:1, `causedBy` is mapped without promoting `commandId` to logs, and data fields are kept internal.
R7 OK — dedup by payload `sequence` deviation is sound (the router sequence is strict and monotonic per interaction, making it a perfect dedup/ordering key).
R8 OK — all `[x]` checked spec tasks have corresponding `// @spec:<id>` vitest tests executing against the `FakeTransport`.
LOOSE_COUPLING: OK — the core logic imports only the `Transport` interface from `transport.ts`. `nats.ws` is strictly confined to `src/adapters/nats.ts`, and the core tests successfully run entirely against `FakeTransport`.
FINDINGS:
- [HIGH] clients/typescript/src/client.ts:320 — `transport.connect()` can throw, leaving the client permanently in the "connecting" state with no exit or failure sibling. — Wrap in try/catch, call `setState("disconnected")` or `"failed"`, and rethrow. — R1
- [HIGH] clients/typescript/src/client.ts:372 — `transport.connect()` throws if the network is still down during `reconnect()`, halting the reconnection loop forever because NATS auto-reconnect is disabled. — Wrap the transport connection attempt in a backoff loop so sustained network drops are gracefully recovered. — R4
- [MEDIUM] clients/typescript/src/interaction.ts:829 — `InteractionHandle.events()` returns the exact same `Mailbox` instance on every call; multiple UI consumers will steal ordered facts from each other. — Return a broadcast/multicast stream wrapper or document the handle as strictly single-consumer. — R3
MISSING_TESTS: none
CONTRACT_RISKS: none
QUESTIONS: none
