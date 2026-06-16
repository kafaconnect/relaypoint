# ADR-0004: RelayPoint is the sole auth-callout responder on the shared infra NATS

- Status: Accepted
- Date: 2026-06-15
- Scope: how RelayPoint and desk coexist on the **one shared infra NATS** when `auth_callout` is
  enabled (M1.5 F1). Relates to: ADR-0003 (per-agent feed), the desk canonical change
  `m1_5-f1-shared-nats-authcallout`, desk ADR-0010/0012 (auth-callout + visitor trust). This is the
  **gating cross-repo design** the desk story requires reviewed on the RP repo BEFORE any infra flip.

## Context

The shared infra NATS (Helm release `nats`, ns `infra`) is **anonymous** today — every desk + RP
client connects without credentials. Enabling per-tenant `auth_callout` (ADR-0008 §6.7 isolation) on
that bus changes how EVERY client authorizes, including RelayPoint's own router/projector. Because the
bus is shared and the cutover is destructive (an anonymous client is rejected on reconnect once
callout is on), the design must be agreed on the RP repo first, with a no-lockout census.

## Decision

1. **RelayPoint owns the SOLE auth-callout responder** (`cmd/authcallout`). It authorizes ALL
   signaling clients: desk agents (Zitadel-derived dev token), web-chat **visitors** (desk-minted
   `vis_` EdDSA JWT, verified against desk's JWKS — RP verifies but can never mint one), and the desk
   trusted-backend producer. **Desk runs NO responder** and its stopgap `conversation.*.events` plane
   retires (desk becomes a producer into RP + a consumer of the per-agent feed).

2. **One application account `RP` + `SYS`.** Minted users land in `RP`. `system_account: SYS`.
   Callout is scoped to `RP` only, never `SYS`.

3. **Static-user carve-out = `auth_users` (callout-exempt), least-privilege, NO anonymous, NO shared
   `client`.** Every trusted backend connects as its OWN enumerated user; the dev shared-`client`
   bypass user is **removed** (it is a callout bypass — a T1 finding). The enumerated identities:
   - `router` — sole `.log` writer; subscribes `.cmd`.
   - `projector` — the fan-out worker (own identity, not shared with `router`).
   - `authsvc` — the responder host (exempt so it is never locked out); pub/sub only
     `$SYS.REQ.USER.AUTH` + `$SYS._INBOX.>` + `_INBOX.>`.
   - `desk-rp` — desk's trusted-backend producer (`rpdelivery`), publishes `…cmd.desk-svc`.
   - `desk-api` — desk's connector/JetStream identity.
   - `connector-zalo` — the connector sidecar (must NOT stay anonymous at the flip).
   Browsers/visitors have **no** static user — they are minted per-connection by the responder.

   **Scope of "no `client`":** this topology is the PRODUCTION shared `nats` Helm release. Those
   values are owned by the desk repo (`deploy/nats/shared-infra-authcallout-values.yaml` in the
   canonical `m1_5-f1-shared-nats-authcallout` change) — they enumerate the service identities above
   and OMIT `client`. The RP repo's `deploy/nats/nats-server.conf` is a SEPARATE artifact: the
   single-node local dev reference the SDK integration suite boots and connects to **as `client`**.
   It is an ephemeral local NATS, never the shared production bus, so it retains the dev `client`
   user (and its `.log`-write deny). The callout-bypass removal applies to the production bus only.

4. **Minted credentials are short-lived where they must be.** A **visitor** credential is capped at
   `min(vis_.exp, an RP ceiling)` (implemented: `WithVisitorTTLCap`, default 1h) — NATS drops the
   connection at expiry and the client re-exchanges a fresh `vis_` (ADR-0012 §4 revocable). The
   responder runs `replicas≥2` under the `authsvc` **queue group** (implemented) so it is HA without
   double-minting and is not a single connect-time point of failure.

5. **No-lockout census is the gate before the flip.** Before `auth_callout` is added, enumerate EVERY
   live connection (`/connz?auth=1&subs=1` on the `nats` release) → map each to a Deployment + env
   Secret + the `auth_users` row it will use, and confirm the `auth_users` set ⊇ {live connz users} ∪
   {manifest NATS clients}. A missing row = an instant lockout of a live workload. Provision every
   client's static credential BEFORE the accounts land; treat accounts+callout as one maintenance
   window for the browser-serving bus (browsers have no static-user interim).

## Migration / rollback order

S1 desk code prep on the still-anonymous bus → S2 provision secrets → **[S3+S4 = ONE atomic
maintenance window]** S3 apply accounts+users + flip each service client to its static cred (verify
via `/connz`) **immediately followed in the same window by** S4 deploy the responder (replicas≥2,
queue) + apply the `auth_callout` block (the destructive line) → verify the live-bus T1/T2 isolation +
no-lockout + visitor-receives-a-message → S5 retire desk's stopgap.

> **Why S3+S4 are one window (not sequential):** the instant `accounts`+`users` exist (S3) WITHOUT
> the `auth_callout` block, a connection presenting no/non-matching credentials is REJECTED — and
> browsers/visitors have no static user and nothing mints them until the responder + callout land
> (S4). So S3 alone would drop every browser. The **service** identities (router/projector/authsvc/
> desk-rp/desk-api/connector-zalo) CAN be validated under S3 (they have static creds), but the
> callout (S4) MUST land in the same window so the browser-serving bus is never in an accounts-only
> interim. Plan accordingly (off-peak; brief realtime blip; REST keeps working).

**Rollback at each step reverts the matching service Deployments AND the NATS config together**
(`helm rollback nats` to the prior rev); the pre-callout image is a safe rollback target until the
desk responder is deleted in S5.

## Consequences

- Per-tenant subscribe isolation is enforced on the real shared bus (a T2 credential cannot subscribe
  `tenant.T1.>`), proven server-side before prod.
- RP becomes the single trust root for the bus; desk holds only the `vis_` private key (RP holds the
  public JWKS) — the owner-required asymmetry.
- The cutover is destructive and must run in a maintenance window with the census + rollback ready.
