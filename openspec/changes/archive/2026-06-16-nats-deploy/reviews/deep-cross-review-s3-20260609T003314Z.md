# Deep cross-review — S3 (signaling-core + client-sdk + nats-deploy)

- Builder: claude · Reviewers: codex, agy (independent, parallel, read-only)
- Surface: an ephemeral integration of S3.1 + S3.2 + S3.3 (review/s3-integration); base 772c5f3
- Review tier: T1 deep — per-change R1..R8 depth + cross-artifact consistency (B1..B6)
- 20260609T003314Z

## Verdicts (initial): codex BLOCKED · agy BLOCKED. Both rated LOOSE_COUPLING = OK
(router core → LogStore port; SDK core → Transport port; both fake-testable, no live infra).

## Findings + resolutions

### Fixed (code)
| Sev | Finding | Fix (branch) |
|---|---|---|
| BLOCKER×2 | router `dup`-append ignored rebuild error / didn't merge results / left `seq` stale | router.go: on dup+rebuild-fail evict (next cmd rebuilds); on success adopt only unseen results; test TestCore_DupRebuildFailEvicts (signaling-core) |
| HIGH | `Replay` skipped malformed JSON → partial state (violates fail-closed) | store.go: return error on a corrupt fact (signaling-core) |
| HIGH/MED | Go `Event` envelope missing `media_profile` (drift vs TS/docs) | envelope.go: add MediaProfile (signaling-core) |
| BLOCKER | SDK `events()` never loaded existing/missed history on open (only on a live gap) | delivery.prime() initial replay; open()+resubscribe() prime; test (client-sdk) |
| BLOCKER | SDK `connect()` could go closed→connected | guard `if(closed) return` after establish; test (client-sdk) |
| HIGH | SDK connect/reconnect could run establish() concurrently | single `busy` guard across both; test (client-sdk) |
| HIGH | SDK stale old-connection status leaked after reconnect | nats adapter: gate emits on `this.nc === nc` (client-sdk) |
| LOW | empty authBackoffMs → zero getToken attempts | tokenWithRetry guarantees ≥1 (client-sdk, prior) |

### Fixed (contract / docs)
| Sev | Finding | Fix |
|---|---|---|
| MED | spec/design said `Nats-Msg-Id = event_id`; impl uses `<tenant>.<iid>.<command_id>` (a fresh event_id per retry would DEFEAT dedup — impl is correct) | signaling-core spec+design corrected; clients order/dedup by router `sequence` |

### Accepted with justification (Phase-1 auth-callout deferral — documented, not silently dropped)
| Sev | Finding | Disposition |
|---|---|---|
| HIGH | shared `client` user + empty identity ⇒ a client can forge `actor_id` on `.cmd`, publish any `.signal.*`, subscribe `_INBOX.>`/cross-tenant | The ACL backstop blocks direct `.log` writes today; full authorship/tenant/signal authenticity REQUIRES the deferred auth-callout (NKEY/JWT) per-connection identity. Scoped honestly: signaling-core spec "Phase-1 security posture" + nats-server.conf comment + runbook caveat. A blocking precondition for production, tracked with the auth-callout change. The "no-forge" claim must not be read as production authorship security. |

### Logged (deferred, tracked)
| Sev | Finding | Note |
|---|---|---|
| MED | post-restart divergent `command_id` reuse replays accepted instead of conflict (rebuilt facts carry no payload hash) | Acceptable: command_ids are unique-per-command; replaying the prior result is idempotent-safe. Cross-restart conflict-detection is best-effort (facts don't persist the payload). Documented limitation. |
| MED | `message.updated/deleted` only require non-empty `ref_id` (target existence / tombstone vs redaction not enforced) | Chat-subset scope; full redaction semantics deferred. |
| LOW | router evicts interaction state only on `ended` → abandoned interactions leak memory | The design's orphaned-reaper is deferred; tracked there. |

## Outcome
All BLOCKER/HIGH code defects fixed to zero with tests; the contract drift corrected; the
security cluster is the explicitly-deferred auth-callout, now scoped honestly across spec +
deploy + runbook rather than overclaimed. Suites green: Go build/vet/unit (signaling-core),
23 vitest (client-sdk), openspec --strict (all three), docker compose config + verify.sh.

---
### codex (raw)
VERDICT: BLOCKED
RUBRIC:
R1 GAP — Router states enumerated: `"" -> started -> ended`, with invalid/bad-payload/replay-fail/append-fail rejection paths; spec states `new/routing/active/transferring/ended` plus abandon/orphan/offline, mostly absent. SDK states enumerated: Connection `disconnected/connecting/connected/reconnecting/closed/failed`, Delivery `live/replaying/degraded/failed`; `closed` can be exited by in-flight connect/reconnect, and delivery has no initial replay path.
R2 GAP — Token retry, connect retry, send retry, duplicate/out-of-order/gap handling exist; missing lifecycle cases: open/reconnect with missed durable facts and no later live event, close-during-connect, stale adapter status after reconnect, malformed replay fact, max payload/schema limits.
R3 GAP — Router resolvers: `inter` mutex, `singleflight`, per-interaction mutex, JetStream `Nats-Msg-Id`; SDK resolvers: `recovering`, `reconnecting`, single-consumer `Mailbox`. Unresolved shared-state pairs: client `connect/close`, stale old-connection status vs new connection, initial replay vs live subscribe, multi-router sequence authority deferred.
R4 GAP — NATS append failure rejects; getToken failure bounded; transport drop retries. Gaps: `Replay` silently skips malformed facts, SDK reconnect can miss history, deploy/router lacks authenticated identity so fail-closed security recovery is not real.
R5 GAP — Direct client `.log` publish is denied, but clients can forge actor/tenant facts through `.cmd` because deployed router passes empty identity; clients can publish any `.signal.*`, subscribe all tenant logs/signals, and subscribe `_INBOX.>`.
R6 GAP — Contract drift exists; see B3/B4/B5.
R7 GAP — Router dedups append by `tenant.iid.command_id`; SDK orders/drops by `sequence`. Gaps: post-restart divergent command replay returns accepted instead of conflict; docs still require `Nats-Msg-Id = event_id`; SDK does not actually dedup by `event_id`.
R8 GAP — `pnpm test`, `pnpm typecheck`, `pnpm build` passed; `docker compose config` passed. `go test ./...` blocked by read-only `/tmp`; `openspec` CLI not found. Several implemented behaviors lack `// @spec` tests.

CROSS_ARTIFACT:
B1 OK — Active artifacts use `INTERACTION_LOGS`: `internal/signaling/store.go:68`, `cmd/router/main.go:47`, `deploy/verify.sh:9`, runbook line 86; stale `INTERACTION_LOG` only appears in historical review/proposal text.
B2 OK — SDK `cmd/log/signal` subjects, router parse/log subject, stream subject, and ACL subject shapes align on `tenant.<id>.interaction.<id>.*`; tenant isolation is not enforced, covered by B3.
B3 DRIFT — Boundary is only partial: SDK code does not write `.log` and ACL denies direct `.log`, but empty deployed identity plus wildcard client ACLs allow forged actor/tenant commands and signal/inbox/log overreach.
B4 DRIFT — `caused_by = command_id` aligns in router/SDK, but Go `Event` has no `media_profile` while TS/docs do; router also persists `command_id` on facts while SDK `LogEvent` deliberately ignores it.
B5 DRIFT — Code uses publish dedup id `tenant.iid.command_id`, and client design documents the deviation, but signaling-core design/spec and architecture docs still require `Nats-Msg-Id = event_id`.
B6 OK — Static/doc-verified: `/healthz`, `/jsz?streams=1`, `/connz?auth=1` are valid NATS monitoring endpoints; unquoted `$ROUTER_PASSWORD/$CLIENT_PASSWORD` env expansion matches NATS config docs; router uses `nats.Name("relaypoint-router")`. Runtime stack not executed because Docker API access was denied. Sources: https://docs.nats.io/running-a-nats-service/nats_admin/monitoring and https://docs.nats.io/running-a-nats-service/configuration

LOOSE_COUPLING:
OK — Router core depends on `LogStore` and NATS is confined to `store.go`/`cmd/router`; SDK core depends on `Transport` and `nats.ws` is confined to `src/adapters/nats.ts`; both have fake/in-memory tests.

FINDINGS:
- [BLOCKER] clients/typescript/src/interaction.ts:41 — `events()` only subscribes live; it never replays existing/missed durable `.log` facts on open or reconnect unless a later live gap exposes them — run replay from `lastApplied+1` on open/resubscribe and test missed-history with no subsequent live event — R2/R4/R7
- [BLOCKER] cmd/router/main.go:38 — deployed router passes `Identity{}`; `router.go:176` falls back to subject tenant and skips actor check, so any shared `client` can forge another `actor_id`/tenant fact through `.cmd` — require authenticated per-connection identity or fail closed outside explicit dev mode; add end-to-end forge-denial tests — R5/B3
- [HIGH] deploy/nats/nats-server.conf:41 — client ACLs are wildcard across tenants/users and subscribe `_INBOX.>`, allowing cross-tenant log/signal reads, forged `.signal.<userId>`, and CommandResult snooping — mint per-tenant/per-user/per-interaction ACLs and scoped inbox prefixes — R5/B3
- [HIGH] clients/typescript/src/adapters/nats.ts:71 — old connection `closed()` status can emit final disconnected after a new connection succeeds; client also lacks close/connect generation guards — add connection generation/abort checks and ignore stale status events — R1/R3/R4
- [HIGH] internal/signaling/router.go:154 — rebuild stores accepted command results with empty `payloadHash`, so post-restart divergent reuse of `command_id` replays accepted instead of conflict — persist payload hash/command ledger or narrow the contract — R4/R7
- [HIGH] internal/signaling/store.go:55 — JetStream replay ignores JSON unmarshal failures and returns partial state despite the port contract saying fail closed — return an error on malformed facts and test replay corruption — R4/R7
- [MEDIUM] internal/signaling/envelope.go:7 — Go wire envelope omits `media_profile` while SDK/spec/docs define `media_profile/mediaProfile` as envelope-level — align envelope structs/codecs/docs before call/media facts land — R6/B4
- [MEDIUM] openspec/changes/signaling-core/specs/signaling-core/spec.md:455 — normative docs still require `Nats-Msg-Id = event_id` while router uses `tenant.iid.command_id` — update signaling-core spec/design/architecture/tasks or change implementation — R6/R7/B5
- [MEDIUM] internal/signaling/router.go:84 — `message.updated/deleted` only require non-empty `ref_id`; target existence and tombstone/redaction semantics are not enforced — track message refs or remove these events from chat subset until defined — R1/R6/R7

MISSING_TESTS:
`clientsdk.delivery.initial-replay-on-open`, reconnect with missed facts and no later live event, close-during-connect/reconnect stale-status race, router replay corruption fail-closed, post-restart divergent `command_id` conflict, `signaling.interaction.context-updated`, `signaling.delivery.ref-id-update-delete`, deploy smoke checks as executable `@spec` tests. `go test ./...` and `openspec validate --strict` not verified in this environment.

CONTRACT_RISKS:
`.log` security claim is stronger than deployed ACL/identity reality; `Nats-Msg-Id` identity differs across specs/docs/code; `media_profile` is not represented in Go envelope; SDK delivery can miss durable history on open/reconnect.

QUESTIONS:
none

### agy (raw)
VERDICT: BLOCKED
RUBRIC:
R1 GAP — Client ConnectionState transitions from `closed` to `connected` if `close()` is called while `connect()` is awaiting `establish()`.
R2 GAP — interactions in the router are evicted only on `ended`; abandoned interactions leak memory forever.
R3 GAP — Client `connect()` and `reconnect()` lack mutual exclusion; concurrent `establish()` calls will clobber the transport connection.
R4 GAP — `Router.HandleCommand` duplicate append (`dup=true`) silently ignores a `rebuild` error, leaving `st.seq` stale and breaking monotonic sequence ordering on the next command.
R5 GAP — NATS ACL allows clients to publish to `tenant.*.interaction.*.signal.*` with any `actor_id`, allowing a client to forge another user's signal.
R6 GAP — see CROSS_ARTIFACT B4.
R7 GAP — `Router.HandleCommand` duplicate append (`dup=true`) fails to merge `fresh.results` into `st.results`, leaving the idempotency cache partial for newly discovered facts.
R8 OK — all specified scenarios have corresponding tests; tests and builds are green.
CROSS_ARTIFACT:
B1 OK — `INTERACTION_LOGS` stream name is used consistently across store, main, docs, and verify.sh.
B2 OK — Subjects match consistently across TS client, Go router, and NATS ACLs.
B3 DRIFT — Security boundary is not airtight for signals: NATS ACL allows the `client` user to publish to ANY `signal.*` subject, enabling forged signals.
B4 DRIFT — `media_profile` exists in TS `codec.ts`/`types.ts` envelope projection but is completely missing from the Go `envelope.go` `Event`/`Command` structs.
B5 OK — Idempotency deviation (SDK dedups by sequence instead of header) is explicitly documented in `design.md` and implemented securely end-to-end.
B6 OK — NATS `/healthz` works for alpine, env-expansion works natively, and `verify.sh` grep patterns correctly match the endpoints.
LOOSE_COUPLING:
OK — Router core depends only on `LogStore` interface with NATS confined to `store.go`. SDK core depends only on `Transport` interface with nats.ws confined to `adapters/nats.ts`. Both have unit tests using fake in-memory adapters (`fakeStore`, `FakeTransport`).
FINDINGS:
- [BLOCKER] clients/typescript/src/client.ts:68 — `connect()` transitions from `closed` to `connected` if `close()` is called during `establish()` — abort `connect()` or skip `setState("connected")` if `this.closed` is true — R1
- [BLOCKER] internal/signaling/router.go:253 — `Router.HandleCommand` duplicate append ignores `rebuild` error and doesn't advance `st.seq`, causing the next command to reuse an existing sequence number — return an error if `dup=true` and `rebuild` fails — R4/R7
- [HIGH] clients/typescript/src/client.ts:384 — `connect()` and `reconnect()` can run `establish()` concurrently, clobbering the transport connection — guard `connect()` against concurrent `reconnect()` — R3
- [HIGH] internal/signaling/envelope.go:4 — `media_profile` is missing from the Go `Event` and `Command` structs, breaking the wire envelope contract — add `MediaProfile string json:"media_profile,omitempty"` to Go envelope structs — B4
- [HIGH] deploy/nats/nats-server.conf:45 — NATS ACL allows clients to publish to ANY `signal.*` subject, enabling forged signals — restrict publish ACL or document as a Phase-1 known limitation pending auth-callout — R5/B3
- [MEDIUM] internal/signaling/router.go:253 — `Router.HandleCommand` duplicate append updates `st.seq` and `st.status` from `fresh` but ignores `fresh.results` — merge `fresh.results` into `st.results` on `dup=true` — R7
- [LOW] internal/signaling/router.go:217 — interactions in `r.inter` are only evicted on `ended`, causing a memory leak for abandoned interactions — add an LRU cache or TTL eviction — R2
MISSING_TESTS:
none
CONTRACT_RISKS:
Go envelope drift (`media_profile` missing) drops media capabilities from the protocol. NATS ACL gap allows spoofed signals, posing a trust risk for clients.
QUESTIONS:
Should signals be routed through the server to enforce authorship, or is a stricter NATS ACL pattern planned for Phase 2 when per-connection identities are minted?
