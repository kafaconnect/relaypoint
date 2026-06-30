# Change: rp-realtime-hardening

## From

A deep review of the deployed pin (`467c1c8` = `main` + two un-reviewed perf commits) across the
router/signaling write plane, the projector/fan-out service, the auth-callout, and the HA/deploy
wiring surfaced **2 CRITICAL, 4 HIGH, ~6 MED, ~10 LOW** issues. The headline defects:

- **OCC token is guessed (`++`), not the broker-committed stream sequence.** On the SHARED
  `INTERACTION_LOGS` stream, `ExpectLastSequencePerSubject` compares the GLOBAL stream sequence, so
  after another interaction interleaves, the real last-subject sequence jumps by >1 and the guessed
  token is stale → spurious `ErrOCCConflict` on ~every concurrent-load append; the spurious conflict
  also burns the single-retry budget so a coincident GENUINE concurrent write is wrongly rejected.
  Tests miss it (the fake models OCC as a per-subject count; the integration test uses one
  interaction on a reset stream). **[CRITICAL]**
- **Lease-renew retry widens the unfenced-processing window to ~3× the lease TTL.** The renew
  adapter ignores `ctx` (rides the NATS 5s default), and `renewWithRetry` (~15.6s) dwarfs the 5s
  TTL, so the run loop keeps Delivering/fanning-out ~13s after a standby could have re-`Create`d the
  lease — two folders both `kv.Put("latest", …)` (last-writer-wins, no CAS) → latent snapshot
  corruption. The riskiest path has zero coverage (`fakeLease.Renew` always returns nil).
  **[CRITICAL]**

The **two perf commits** on this pin — concurrent per-fact fan-out (`1f4309b`) and lease-renew
tolerance (`467c1c8`) — shipped to dev **without a change or cross-review**. The lease-renew commit
is the direct cause of the second CRITICAL. Both are **folded into this epic for proper review**:
their behaviour is locked behind spec scenarios (`RDL-01`/`RDL-02` already tagged in tests but
**dangling** — defined nowhere) and corrected (`RDL-03`).

The HIGH/MED/LOW set covers a partially-applied transfer that becomes permanently un-retryable,
transient publish/tombstone/roster failures that `Nak` without the `MaxDeliver`→DLQ guard (silent
drop or single-consumer wedge after a ~2s blip), an empty roster cached 60s + acked-and-dropped,
absent liveness/readiness probes, a diverged NATS user model (`NATS_USER=projector` with no
`projector` user defined), a 250ms rebuild fetch tail + unbounded in-process router state,
auth-callout fail-open-on-unknown-role + over-broad `$JS.API.>`/presence grants, dormant OTLP
export + no metrics, unused HA replicas + duplicated mutable-tag deploy defs, and a cluster of
low-severity contract/comment/config gaps.

## To

The hardened end state:

- The OCC token is the **broker-committed stream sequence** returned by `Append`, not `prev+1`, so a
  shared-stream interleave never produces a spurious conflict and the retry budget protects only
  genuine races (RH-01).
- Lease renewal is **fenced within the TTL budget** — the adapter honours `ctx` with a per-attempt
  timeout so total retry time < `TTL − renewInterval`, and an OVERDUE renew **pauses the data path
  immediately** (stop-the-world) instead of after all retries fail (RH-02); the concurrent fan-out
  is locked under `RDL-01`/`RDL-02`.
- Participation transfers re-drive idempotently after a partial apply (subset check, RH-03);
  exhausted publish/tombstone deliveries go to the **DLQ with a record**, roster failures **retry
  unbounded** (RH-04), and an empty roster **soft-fails (Nak) and is never cached** (RH-05).
- Every RP binary exposes a **health surface** (NATS+JetStream reachable; the projector reports
  lease-held for readiness) wired to liveness/readiness probes, and the NATS user model gains a
  least-privilege **`projector`** identity (RH-06).
- Router rebuild uses a **no-wait fetch** (no 250ms tail) and **idle-evicts** in-process state
  (RH-07); the auth-callout **fails closed on an unknown role** and scopes `$JS.API`/presence grants
  to least privilege (RH-08); OTLP export and **metrics** are wired so DLQ/lease/roster/fan-out are
  alertable (RH-09); HA replicas are enabled relying on the queue group (router) and the KV lease
  (projector), and the duplicated mutable-tag deploy defs are removed (RH-10). The LOW cluster
  (RH-11) and the DoD closeout incl. spec-tree sync + ADRs + cross-reviews (RH-12) close out the
  epic.

## Reason

The realtime plane is now load-bearing in production (web realtime rides the per-agent feed). The
two CRITICALs are correctness/HA defects that current tests structurally cannot catch, and the two
un-reviewed perf commits put unspecified behaviour on the critical path — both violate the DoD
(every scenario has a `// @spec:` test; no architecture change without an ADR; independent
cross-review before archive). This epic brings the deployed pin back under the spec-driven gate.

## Impact

- **Components:** `internal/signaling/{store.go,router.go}` (OCC token, transfer subset, rebuild
  fetch, in-process state TTL, stream ceiling, fail-loud password); `internal/projector/{projector.go,
  nats.go,roster_http.go,ports.go}` (lease fencing, Nak→DLQ guard, roster soft-fail + no-cache-empty,
  `fanoutConcurrency` config); `internal/authcallout/{grants.go,token.go,identity.go}`
  (fail-closed role, HMAC gate, JS.API/presence scoping); `internal/obs/otel.go` + a new metrics
  surface; `cmd/{router,projector,authcallout}` (health listeners, config defaults); `deploy/`
  (NATS user model, compose probes, image tags) and — **cross-repo** — desk's
  `deploy/helm/desk/templates/relaypoint.yaml` + `deploy/k8s/50-52-rp-*.yaml` (probe wiring, HA
  replicas, delete stale defs) tracked as desk follow-ups.
- **Streams:** `INTERACTION_LOGS` (the SHARED `tenant.*.interaction.*.log` stream at the heart of
  RH-01; gains a `MaxBytes`/`MaxAge` ceiling, RH-11c); `AGENT_FEED`
  (`tenant.<tid>.agent.<aid>.feed.>`); the DLQ subject `tenant.<tid>.agent.dlq.feed` (RH-04); the KV
  **lease** bucket (RH-02) and the KV snapshot bucket.
- **Grants/identities:** `$JS.API.>` narrowed (RH-08c); `presence.<self>` / `presence.*` tightened
  (RH-08d); the `_INBOX_<conn>.>` minted prefix unchanged; a new least-privilege **`projector`**
  NATS user (`.log` read + `AGENT_FEED` write + KV lease perms) added to the local
  `deploy/nats/nats-server.conf` reference and the production desk Helm values (RH-06).
- **Wire/contract:** no protobuf change beyond an OPTIONAL participation-payload contract note
  (RH-11b); the `Append` port signature gains a returned committed sequence (RH-01) — internal port,
  no cross-repo contract impact. The desk repo is untouched except the noted deploy follow-ups.

## Status — closeout (RH-12)

All tasks RH-01..RH-12 are **done** (see `tasks.md`). The DoD closeout (RH-12):

- **Spec tree materialized.** This change's per-capability deltas are synced into the canonical
  `openspec/specs/{observability,projector,authcallout,deploy,signaling-core}/spec.md` (the FIRST
  materialization of the tree). Every `@spec:` id this epic introduces — `RDL-01/02/03` and all
  RH-06..11 ids — now resolves to a requirement in `openspec/specs/`.
- **ADR-0007** (projector lease-fencing, RH-02) is **Accepted**; ADR-0003 carries a reciprocal
  amendment noting the fence constrains its Decision 4 leased-worker model.
- **Pre-existing debt flagged, not fixed here:** `@spec:` ids from the 8 already-archived changes
  (signaling-core, agent-feed-fanout, router-occ(-dedup-ordering), wire-protobuf, nats-deploy,
  trace-continuity) plus a set of orphan tags (`authcallout.visitor.*`, several `obs.*`, `web-call.*`,
  `f1.*`) do not resolve — the canonical tree was never materialized for those changes. That backfill
  is outside this change's deltas; tracked separately.
