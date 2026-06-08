# Cross-review (concise, parallel) — signaling-core S3.1 chat-subset
codex+agy BLOCKED → fixed: state rebuild-from-log on restart; deterministic JetStream
MsgId (exactly-once across crash); per-interaction lock (no global bottleneck); evict
on ended; _INBOX publish tightened. Plus owner architecture asks: ports-and-adapters
(router core depends only on LogStore, NATS is an adapter) + identity-via-context
(auth is the trust anchor; unlocked forged-author).

## codex
VERDICT: BLOCKED
FINDINGS:
- [BLOCKER] deploy/nats/nats-server.conf:51 — Shared client ACL can subscribe to `_INBOX.>`, so any client can snoop another issuer's `CommandResult`; line 39 also lets clients spoof inbox replies — restrict inbox subjects per connection/user or auth-callout-minted prefix, and add a second-client no-leak/no-spoof test.
- [HIGH] internal/signaling/router.go:142 — Exactly-once can double-append on ambiguous JetStream publish failure because each retry gets a new `event_id`/`Nats-Msg-Id` and `command_id` is memoized only after publish success — make the stream write idempotent on `tenant/interaction/command_id` or reconcile stored facts before retrying.
- [MEDIUM] internal/signaling/router.go:125 — Rejected parsed commands are not memoized, so the same `command_id` can later be reused with a different payload instead of conflict, weakening “key bound to original request” — store payload hash/result for command-level rejections and replay/conflict consistently.
NOTES: 9 claimed `// @spec:` tags are present; forged-author and signal-rate-limit are clearly deferred. `openspec validate --strict` was not run because `openspec` is unavailable in PATH.

## agy
VERDICT: BLOCKED
FINDINGS:
- [BLOCKER] internal/signaling/router.go:392 — The router fails to rebuild `interactionState` (sequence, status, results) from the durable `.log` on first access. After a restart, existing interactions reset to seq=0 and status="", causing valid messages to be rejected as illegal transitions and breaking sequence monotonicity. — Query JetStream to load the interaction's previous events when `st == nil`.
- [HIGH] internal/signaling/router.go:389 — `r.mu.Lock()` holds a single global mutex during the synchronous `r.js.Publish()` network call, blocking all other tenants/interactions and creating a severe bottleneck. — Scope the lock per-interaction (e.g., using lock striping or a sync.Map of mutexes).
- [HIGH] internal/signaling/router.go:423 — Random `ev.EventID` is used for `nats.MsgId`, defeating JetStream deduplication. If a publish times out but succeeds, memory isn't updated and a client retry will append a duplicate fact. — Use a deterministic ID for `nats.MsgId` (e.g., `cmd.CommandID`).
- [MEDIUM] internal/signaling/router.go:394 — In-memory `interactionState` entries in `r.inter` are never evicted, causing an unbounded memory leak over time. — Implement an eviction strategy (e.g., LRU cache or evict after `interaction.ended`).
NOTES: The structure and tests are clean, but missing state reconstruction on restart breaks the "durable source of truth" guarantee. Concurrency and JS dedup issues must be fixed for reliability under load.
