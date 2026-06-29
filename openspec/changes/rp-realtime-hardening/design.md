# Design: rp-realtime-hardening

Mechanics + the non-obvious decisions only; the log-authoritative model (ADR-0002 OCC, ADR-0003
fan-out) and the subject layout are unchanged. Each decision below maps to the RH task that
implements it and the spec id that makes it verifiable.

## (a) OCC token = broker-committed stream sequence, never `prev+1` (RH-01)

`INTERACTION_LOGS` is a SHARED stream (`tenant.*.interaction.*.log`, store.go:101-107). JetStream's
`ExpectLastSequencePerSubject` is checked against the **per-subject view of the GLOBAL stream
sequence** — the stream sequence of the last message ON THAT SUBJECT, which is a global counter, not
a dense per-subject one. After interaction A appends (global seq 100) and interaction B interleaves
(global seq 101), A's next real last-subject sequence is 100, but a router that does `streamSeq++`
predicts 101. The prediction is only correct when nothing else writes the stream between two appends
on the same subject — false under any concurrent load. So `++` (router.go:402, :626) manufactures a
stale token → a spurious `ErrOCCConflict` that forces a rebuild+retry on nearly every append, and —
worse — consumes the single retry budget, so a genuinely concurrent writer that should have been
arbitrated is instead rejected with `lost concurrent append — retry`.

The committed sequence is already available: `PublishMsg` returns `ack.Sequence` (store.go:53) and
`Replay` already returns `lastSubjSeq` (store.go:88). The fix threads the **committed** value out of
`Append` and the router sets `st.streamSeq = <returned committed seq>` after a clean append, instead
of guessing. The dup/error paths already self-correct via `Replay`/rebuild, so only the clean-append
assignment changes. The router never interprets the value — it only echoes the last-seen one — so
the dense per-interaction `sequence` is untouched; this is purely the OCC token. The regression test
must interleave TWO interactions appending alternately on the shared stream and assert NO spurious
conflict + a monotonic dense per-interaction sequence (the current fake models OCC as a per-subject
count, which structurally cannot exhibit the bug).

## (b) Lease-fencing budget math + stop-the-world on overdue renew (RH-02)

A leased single-active worker is safe only while its fenced (lease-held) window strictly contains
its data-path activity. The deployed pin breaks that: `Renew(_ context.Context)` (nats.go:273)
discards `ctx` and rides the NATS default ~5s request timeout; `renewWithRetry` does
`leaseRenewAttempts=3` with `leaseRenewRetryBackoff=300ms`, so a stuck broker costs ≈ 3×5s + 2×0.3s
≈ **15.6s** before the run loop is even told the lease is in doubt — against `TTL=5s`
(main.go:53) and `renewInterval=2s`. A standby can re-`Create` the expired lease at ~`TTL` while the
old holder keeps Delivering/fanning-out for another ~13s; both then `kv.Put("latest", …)`
(nats.go:316, last-writer-wins, NO CAS) → snapshot corruption.

The budget invariant: **a renew must conclusively succeed-or-fail before the lease can expire**, i.e.

```
attempts × (per_attempt_timeout + backoff) < TTL − renewInterval
```

With `TTL=5s`, `renewInterval=2s` the slack is 3s. A per-attempt `ctx` timeout of ~700ms gives
`3×0.7 + 2×0.3 = 2.7s < 3s`. So: (1) `Renew` HONOURS `ctx`; the caller passes a per-attempt
`context.WithTimeout`; (2) the attempts × (timeout+backoff) are DERIVED from the configured TTL, not
hardcoded, so a TTL change can't silently re-open the gap; (3) on an **overdue** renew (the renew
did not conclude within the budget) the worker **pauses `process`/`Deliver` immediately**
(stop-the-world) rather than after 3 failed attempts — it must stop touching the data path the
instant it can no longer prove it holds the lease, BEFORE a standby could have taken over. A briefly
slow-but-still-held lease costs at most a paused beat; correctness beats the ~18s availability gain
the un-reviewed perf commit chased.

This is recorded as **ADR-0007 (Proposed)** because it constrains the ADR-0003 leased-worker model.
The concurrent fan-out (`1f4309b`) is orthogonal and PRESERVED — ack only after ALL recipient
publishes succeed (`RDL-01`/`RDL-02`); the serial `MaxAckPending=1` fold is unchanged.

## (c) Nak-vs-DLQ policy: transient→retry, exhausted→DLQ-with-record, roster→unbounded (RH-04/RH-05)

`process` (projector.go) has three `Nak` branches — fan-out fail (:309), tombstone fail (:316),
roster fail (:292) — none of which check `Delivered(f) >= MaxDeliver`, unlike `poison()` (:382)
which routes to the DLQ + acks. With `MaxDeliver=5` exhausting in ~1.75s, a longer outage either
**silently terminates** the fact (at-least-once violated, no DLQ, no alert) or **wedges** the single
active consumer (`MaxAckPending=1` → total stall). Policy:

- **Transient (`Delivered < MaxDeliver`):** `Nak` for redelivery (unchanged); the per-`(agent, iid,
  sequence)` dedup id makes an already-published feed a no-op.
- **Exhausted (`Delivered >= MaxDeliver`):** route to the DLQ (`tenant.<tid>.agent.dlq.feed`) with
  the failure reason + source `event_id`/`sequence` and ack, exactly like `poison()` — never a
  silent terminal `Nak`, never a wedge.
- **Roster:** prefer **UNBOUNDED retry/backoff** — a desk blip must NOT cost a fact its delivery; do
  NOT DLQ a fact because the roster source is momentarily down. An **empty** roster (HTTP 200, empty
  agents) is a SOFT failure: `Nak` (retry), unless a tenant legitimately has zero agents, and it is
  **NOT cached** (cache only non-empty results, mirroring the existing "errors are not cached"
  intent) so a one-minute cache window can't dark a tenant.

## (d) Auth-callout fail-closed-on-unknown-role + least-privilege JS.API (RH-08)

The auth-callout is defense-in-depth (the router has none for visitor scope), so its grants must
fail closed and grant the minimum:

- **Role:** `switch RoleOf(id)` (grants.go:44) defaults to the AGENT grant, and `RoleOf` maps an
  empty role to `RoleAgent` (identity.go) — an **unknown role mints an agent grant** (fail-open).
  Make `RoleAgent` an EXPLICIT case and `default: return Grant{}, error` (deny). An unknown/unmapped
  role must authorize NOTHING.
- **HMAC:** one process-wide `AUTH_TOKEN_SECRET` lets a holder self-assert ANY tenant + the
  `trusted-backend` role (token.go), wired always-on in prod (cmd/authcallout/main.go). Gate the HMAC
  link behind a dev flag (omit it when JWKS is configured), or bind the secret to a fixed
  tenant/role; at minimum **forbid `role=trusted-backend` over HMAC in production**.
- **JS.API:** the trusted-backend `PubAllow` includes account-wide `$JS.API.>` (grants.go:74).
  Scope it to the specific JS API subjects actually used
  (`$JS.API.CONSUMER.CREATE.<stream>.>`, `$JS.API.STREAM.INFO.<stream>`), or move JS admin to a
  static, non-minted identity. A minted connection must never hold account-wide JS admin.
- **Presence:** the agent grants tail-wildcard `presence.<self>.>` (pub, grants.go:98) and
  `presence.*.>` (sub, grants.go:114) — broader than documented. Tighten to the literal
  `presence.<self>.state` / `presence.<self>.typing.>` (pub) and `presence.*.state` /
  `presence.*.typing.>` (sub).

The new cross-TENANT visitor-denial integration test closes the gap that today only cross-CONVERSATION
is asserted.

## Capability grouping

- **signaling-core** — RH-01 (OCC token), RH-03 (transfer subset), RH-07 (rebuild fetch + state
  TTL), and the router-side LOWs in RH-11.
- **projector** (new capability for the fan-out service) — RH-02 (`RDL-01`/`02`/`03` lease + fan-out),
  RH-04 (DLQ guard), RH-05 (roster), RH-10 (HA replicas), RH-11i (`fanoutConcurrency` config).
- **authcallout** (new capability) — RH-08.
- **observability** — RH-06 (health/readiness), RH-09 (OTLP + metrics).
- **deploy** — RH-06 (`projector` NATS user), RH-10 (immutable image tags / delete stale defs).

## Loose coupling

Every fix stays on an owned port or its adapter: the returned committed sequence is on the
`LogStore` port (the JetStream adapter is the only place that reads `ack.Sequence`); the lease budget
+ ctx honouring are on the `LeaseStore` port + its KV adapter; the DLQ/roster policy is in the
projector core driven through the `FeedSink`/`Roster` ports; the health surface reads liveness from
the same ports. No core package gains a concrete NATS import (HARD RULE).
