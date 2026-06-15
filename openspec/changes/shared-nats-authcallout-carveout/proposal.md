# Change: shared-nats-authcallout-carveout

The **RelayPoint-repo companion** to desk's `m1_5-f1-shared-nats-authcallout` (M1.5 F1). It is the
gating cross-repo design (ADR-0004): RP is the SOLE auth-callout responder on the shared infra NATS,
trusted backends connect via a least-privilege static-user carve-out (no anonymous, no shared
`client`), and the cutover is staged with a no-lockout census. The responder mechanism already exists
(`cmd/authcallout`, `internal/authcallout/*`); this change is the design + the RP-side hardening +
the gate for the desk infra flip.

This is an **active proposal**.

## Charter
Constrained by §6.7 (per-tenant isolation on the shared bus). ADR-0004 is the decision record;
ADR-0003 (per-agent feed) is the consumer model the visitor/agent grants slot into.

## Review tier
**T1** — re-asserts a §6.7 hard rule on a SHARED bus whose cutover is destructive. The no-lockout
census + the live T1/T2 isolation proof are BLOCKER-bar gates (owned by the desk story's V3).

## From → To
- From: the responder existed but (a) minted a non-expiring visitor credential, (b) used `Subscribe`
  (no HA), (c) the shared-`client` bypass user was still in the reference config, and (d) the carve-out
  design was not recorded on the RP repo (the desk story's gate).
- To: ADR-0004 records the coexistence design; the responder caps the visitor credential TTL and
  `QueueSubscribe`s under `authsvc` (shipped in `m1_5-f1-rp-visitor-ttl-cap`); the carve-out drops the
  `client` user; the no-lockout census + staged cutover are the desk story's V2/V3 (coordinated).

## Impact
- `docs/architecture/decisions/0004-shared-bus-rp-desk-coexistence.md` (this change).
- The responder code hardening landed in `m1_5-f1-rp-visitor-ttl-cap` (visitor TTL cap + QueueSubscribe).
- The `deploy/nats/nats-server.conf` reference topology drops the dev `client` user before the flip.
- No live infra change here — the flip is the desk story's gated maintenance-window step.
