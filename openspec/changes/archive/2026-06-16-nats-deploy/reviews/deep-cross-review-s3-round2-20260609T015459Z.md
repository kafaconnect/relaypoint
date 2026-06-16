# Deep cross-review — S3 (round 2, post-debate state)

Independent codex + agy over the full S3 surface AFTER the debate-round fixes. Both BLOCKED;
all findings real (no rubber-stamp). Resolutions:

| Sev | Finding | Fix |
|---|---|---|
| BLOCKER | router dup-append returned accepted + clobbered state without checking the committed fact's payload_hash | compare committed payload_hash → conflict on divergence, replay on match, never clobber (TestCore_DupPathChecksPayloadHash) |
| BLOCKER/HIGH | nats replay idle timer raced consumer-setup/first-message → premature empty on a slow network | terminate by consumer num_pending (empty → return at once; non-empty → wait, no timer) |
| HIGH | public connect() didn't guard re-entry → two connect() race establish() | guard on busy/already-connected |
| MEDIUM | payload_hash on the wire but undefined in TS codec / spec / docs | documented as router-internal idempotency metadata (persisted on the fact, clients ignore); TS WireEvent acknowledges command_id/payload_hash without projecting |
| LOW | event_id was ev_<hex>, not UUID/ULID | proper UUIDv4 |
| LOW | stale `Nats-Msg-Id = event_id` in client-sdk design/tasks | corrected to the command-derived key |
| MEDIUM | deploy scenarios had no executable @spec trace | // @spec:deploy.* tags in verify.sh / compose / nats-conf |

LOOSE_COUPLING: both OK (router→LogStore, SDK→Transport, fake-tested).

Both rounds confirm: the nats.ws replay adapter path still needs the live-NATS integration test
(tracked as relaypoint#12) — consumerInfo()/ordered-consumer behaviour can't be unit-tested here.

---
### codex (raw)
```
VERDICT: BLOCKED

RUBRIC:
R1 GAP — Router states `""→started→ended` and SDK connection/delivery states are present, but ack-loss reconcile and concurrent `connect()` leave invalid transitions.
R2 GAP — Restart, duplicate, closed-handle, and replay edges are covered; ack-lost terminal retry and NATS replay idle edge are not.
R3 GAP — Router has per-interaction lock/singleflight/poison and SDK has reconnect busy guard, but public `connect()` is not serialized.
R4 GAP — Replay/JetStream failure paths fail closed in several places, but append ack-loss and adapter idle replay can return a false success.
R5 OK — `.log` forge boundary is enforced by NATS ACL + SDK no-write surface + router sole writer; Phase-1 actor/signal forge is documented as auth-callout-deferred.
R6 GAP — Wire envelope and dedup-key SSoT still drift across Go/TS/spec/docs.
R7 GAP — Command-derived `Nats-Msg-Id` exists, but ack-loss reconcile does not preserve command result/hash in cached state.
R8 GAP — Core/SDK tests are tagged for the chat slice; deploy scenarios and new failure edges lack trace tests.

CROSS_ARTIFACT:
stream — OK: `INTERACTION_LOGS` aligns in Go store, TS adapter, deploy verify.
subjects — OK: tenant-prefixed `.cmd/.log/.signal.*` align; Phase-1 wildcards are documented.
security-boundary — OK: client `.log` publish denied, SDK never writes `.log`, router writes facts.
envelope — DRIFT: Go emits `payload_hash`; spec/docs/TS do not define/project it, and architecture envelope omits `media_profile/command_id/caused_by`.
dedup-key — DRIFT: code/spec mostly use `<tenant>.<interaction>.<command_id>`, but `client-sdk/design.md` and `client-sdk/tasks.md` still say `Nats-Msg-Id=event_id`.
deploy — OK: healthcheck, env expansion, router dependency, and verify endpoints match the deploy spec; trace tags missing under R8.

NEW_CODE_AUDIT:
router poison flag + ack-loss reconcile + payload_hash — BUG: committed-append reconcile updates seq/status but not cached `results`, so retries can reject or reappend incorrectly.
random event_id — BUG: random avoids restart collision, but `ev_<hex>` is not the ULID/UUID format documented for event ids.
SDK Mailbox/reconnect/closed-handle/prime — BUG: Mailbox and handle recreation are OK; `connect()` itself ignores the `busy` guard, so concurrent connects race.
nats adapter bindStream + Promise.race replay — BUG: idle can win before first delayed replay message and return a successful empty/partial replay; `fromSequence` is ignored.

LOOSE_COUPLING:
OK — Router logic depends on `LogStore` and is fake-tested; SDK core depends on `Transport` and is fake-tested. NATS imports are confined to adapters/edges.

FINDINGS:
- [BLOCKER] internal/signaling/router.go:247 — Ack-loss reconcile returns the committed result but does not merge `fresh.results` into the cached state; retrying an ack-lost `interaction.ended` is then rejected as illegal, and other commands can rely only on broker dedup window — replace/merge the rebuilt state before returning and evict terminal states consistently — R4/R7
- [HIGH] clients/typescript/src/client.ts:61 — `connect()` sets `busy=true` but never checks an existing busy/connected state; two public `connect()` calls can run concurrent `establish()` calls, fetch multiple tokens, and close/supersede connections nondeterministically — serialize `connect()` with an in-flight promise or explicit idempotent connected/busy handling — R1/R3
- [MEDIUM] clients/typescript/src/adapters/nats.ts:74 — replay treats `replayIdleMs` as end-of-stream and can return success before a slow first message, especially on initial history load where `Delivery` has no pending gap to force retry — use an explicit bounded replay/end condition or throw on uncertain idle instead of silently completing — R4/R7
- [MEDIUM] internal/signaling/router.go:176 — pre-state validation rejections are not memoized with `payload_hash`; a reused `command_id` with a corrected/different payload can later be accepted, so the rejected-command fix only covers illegal transitions — memoize scoped parsed rejections or narrow the command-id contract and tests — R7
- [MEDIUM] internal/signaling/envelope.go:18 — `payload_hash` is now a top-level persisted wire field, but the authoritative envelope/spec/docs/TS codec do not define it — either add `payload_hash?/payloadHash?` to SSoT + codec/tests or move it to an internal ledger not emitted as `.log` wire — R6
- [LOW] internal/signaling/router.go:50 — generated event ids are `ev_` + hex, while docs/spec examples require ULID/UUID-shaped ids — generate UUID/ULID or relax the documented format and add a test — R6
- [LOW] openspec/changes/client-sdk/design.md:34 — stale `Nats-Msg-Id = event_id` text remains despite the command-derived dedup key fix — update design/tasks comments to prevent reintroducing header-based event-id dedup — R6/R7
- [MEDIUM] deploy/verify.sh:1 — deploy scenarios have no executable `// @spec:deploy.*` trace; tags exist only in tasks, not in tests/verify script — add trace comments or a deploy test harness covering all five deploy scenario ids — R8

MISSING_TESTS:
- `signaling.cmd.idempotent-command-id`: committed append + lost ack reconcile, including terminal command retry.
- `signaling.cmd.command-id-conflict`: command-id reuse after validation rejection, if the contract remains broad.
- `clientsdk.connection.state-observable`: concurrent public `connect()` calls.
- `clientsdk.delivery.gap-replay` / `clientsdk.handle.stream-and-send`: NATS adapter replay idle/partial behavior, not just `FakeTransport`.
- `deploy.bringup.one-command`, `deploy.health.router-waits-for-nats`, `deploy.creds.env-externalised`, `deploy.security.client-log-write-denied`, `deploy.verify.smoke-check`: no executable `@spec` trace.

QUESTIONS:
- Is `payload_hash` intended to be public `.log` wire contract, or router-internal idempotency metadata?
```
### agy (raw)
```
VERDICT: BLOCKED
RUBRIC:
R1: OK — router and SDK state machines are properly defined and transitioned.
R2: OK — SDK correctly handles lifecycle edges like late close after connect.
R3: OK — singleflight, Mailbox single-consumer release, and busy guards are race-free.
R4: GAP — nats adapter replay premature `idle` timeout permanently fails Delivery on slow networks.
R5: OK — Phase-1 security posture via NATS ACLs correctly restricts `.log` writing.
R6: GAP — wire envelope drift: `WireEvent` in TS codec is missing `command_id` and `payload_hash`.
R7: GAP — payload_hash cross-restart conflict: `dup=true` path in router clobbers memory state without verifying the log's payload_hash.
R8: GAP — no tests included in the patch to verify new complex logic (poison flag, Mailbox release, etc).
CROSS_ARTIFACT:
stream: OK
subjects: OK
security-boundary: OK
envelope: DRIFT
dedup-key: OK
deploy: OK
NEW_CODE_AUDIT:
router poison flag + ack-loss reconcile + payload_hash: BUG — `dup=true` path clobbers memory state (`st.results`) with the new payload's hash and returns `accepted` without checking if the already-committed log event matches the new payload hash, violating SSoT.
random event_id: OK — safe, `sequence` is used for ordering and NATS `MsgId` for dedup.
SDK Mailbox try/finally release; connect/reconnect close-late-connection; closed-handle drop + re-create; delivery.prime() initial replay: OK — `this.recovering` effectively prevents `prime()`/`offer()` re-entrancy.
nats adapter bindStream + Promise.race(it.next/idle/closed) replay: BUG — `idle` uses a fixed 1000ms timeout per iteration, which includes consumer creation and first-message latency. A slow connection will hit `idle` before any facts arrive, permanently failing Delivery. Also creates dangling `setTimeout` promises for fast messages.
LOOSE_COUPLING: OK
FINDINGS:
- [BLOCKER] internal/signaling/router.go:261 — Router overwrites memory state with uncommitted payload hash on duplicate append — The `dup=true` branch must check if the `payloadHash` of the existing committed event in `fresh.results` matches the current command's hash. If they differ, it must return a conflict error, rather than blindly returning `accepted` and clobbering `st.results[cmd.CommandID]`. — R7
- [HIGH] clients/typescript/src/adapters/nats.ts:237 — Premature idle timeout drops facts on slow connections — The 1000ms `idle` timer races against consumer creation and first message delivery. If the network takes >1s, `replay` returns empty, and `Delivery` will retry 5 times and permanently fail. Increase the timeout, or only apply the idle timeout after the first message is received. — R4
- [MEDIUM] clients/typescript/src/codec.ts:54 — Wire envelope drift in TypeScript codec — `WireEvent` interface is missing the `command_id` and `payload_hash` fields that are written by the router and documented in the design. Add these fields to the interface. — R6
- [LOW] clients/typescript/src/adapters/nats.ts:237 — Promise.race leaks setTimeout promises — Every fast message iteration creates a dangling 1000ms `setTimeout` promise that resolves later, causing unnecessary CPU wakeups. Wrap the timer so it can be `clearTimeout`'d on success. — R4
MISSING_TESTS:
- Router `HandleCommand` duplicate append (`dup=true`) with matching and mismatched payload hashes.
- Router `HandleCommand` append failure resulting in state poison and eviction.
- SDK `Delivery` replaying with a simulated slow network to verify it doesn't fail closed prematurely.
- SDK `Mailbox` single-consumer release validation.
QUESTIONS:
none
```
