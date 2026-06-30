---
id: RH-08
slice: RH
title: MED — auth-callout fail-closed unknown role plus least-privilege JS.API/presence/HMAC
status: done
specs: [authcallout.role.fail-closed-unknown, authcallout.hmac.no-trusted-backend-prod, authcallout.jsapi.least-privilege, authcallout.presence.scoped-subjects, authcallout.tenant.cross-denied]
---

## Goal
Defense-in-depth at the NATS edge (the router has none for visitor scope). Four fail-open/over-broad
grants: (a) `switch RoleOf(id)` default → AGENT grant + empty role → `RoleAgent` = fail-open on an
unknown role; (b) the process-wide `AUTH_TOKEN_SECRET` HMAC self-asserts ANY tenant + role=trusted-
backend, wired always-on in prod; (c) trusted-backend `PubAllow` includes account-wide `$JS.API.>`;
(d) presence grants are tail-wildcards broader than documented.

## Success criteria (test-first)
- Tests first for each arm + a cross-TENANT visitor-denial integration test (today only cross-
  CONVERSATION is asserted).
- (a) `RoleAgent` is EXPLICIT; `default: return Grant{}, error` (deny) — unknown role grants nothing.
- (b) Gate the HMAC link behind a dev flag (omit when JWKS configured) or bind secret→fixed
  tenant/role; at minimum forbid `role=trusted-backend` over HMAC in production.
- (c) Scope `$JS.API.>` to the specific subjects used (`$JS.API.CONSUMER.CREATE.<stream>.>`,
  `$JS.API.STREAM.INFO.<stream>`) or move JS admin to a static non-minted identity.
- (d) Tighten to `presence.<self>.state` / `presence.<self>.typing.>` (pub) and `presence.*.state` /
  `presence.*.typing.>` (sub).

## Files
- `internal/authcallout/grants.go` (role default deny; `$JS.API` scope; presence literals)
- `internal/authcallout/identity.go` (empty-role no longer maps to `RoleAgent`)
- `internal/authcallout/token.go` + `cmd/authcallout/main.go` (HMAC dev-gate / no trusted-backend in prod)
- `internal/authcallout/grants_test.go` + a cross-tenant visitor-denial integration test

## Spec
`// @spec:authcallout.role.fail-closed-unknown`, `// @spec:authcallout.hmac.no-trusted-backend-prod`, `// @spec:authcallout.jsapi.least-privilege`, `// @spec:authcallout.presence.scoped-subjects`, `// @spec:authcallout.tenant.cross-denied`

## Log
- **done** — all four arms + cross-tenant integration test landed, fail-closed verified end-to-end on embedded NATS.
- (a) role fail-closed: `internal/authcallout/grants.go` now switches on `id.Role` directly (was `signaling.RoleOf(id)`) with an EXPLICIT `case RoleAgent` and `default: return Grant{}, error`. Unknown/empty role authorizes nothing. Left `signaling.RoleOf` UNTOUCHED on purpose: its empty→agent default is the *strict* direction for the router's actor/participation gates (router.go:223/238/248) — changing it there would skip the membership gate (fail-OPEN) for empty-role identities; the grant layer no longer inherits the lenient default. (Task listed `internal/authcallout/identity.go`, which does not exist; `RoleOf`/`Role` live in `internal/signaling/identity.go`.) Test: `TestGrantsForUnknownRoleFailsClosed`.
- (b) HMAC no-trusted-backend-prod: `token.go` `HMACVerifier` rejects `role=trusted-backend` unless the opt-in `AllowHMACTrustedBackend()` option is set (secure default). `cmd/authcallout/main.go` derives posture from the EXISTING `DESK_INGRESS_JWKS_URL`: set ⇒ prod (secure HMAC, no trusted-backend; agents+visitors via JWKS), unset ⇒ dev (HMAC may self-assert trusted-backend for local wiring). NO new env var. Tests: `TestHMACRejectsTrustedBackendByDefault`, `TestAuthCalloutHMACDeniesTrustedBackendInProd`.
- (c) JS.API least-privilege: trusted-backend `PubAllow` `$JS.API.>` replaced by consumer-lifecycle + stream-info subjects scoped to the one log stream (`STREAM.INFO`/`CONSUMER.{CREATE,DURABLE.CREATE,INFO,MSG.NEXT}` on `signaling.LogStreamName`). Exported `LogStreamName` from `internal/signaling/store.go` (single source, was a repeated literal) so the scope can't drift. Routers/projector own JS admin via STATIC creds, not the minted identity. Test: `TestGrantsForTrustedBackendJSAPILeastPrivilege`.
- (d) presence scoped: agent pub tightened to `presence.<self>.state` + `presence.<self>.typing.>`, sub to `presence.*.state` + `presence.*.typing.>` (was tail-wildcards). Test: `TestGrantsForAgentPresenceScoped`.
- cross-tenant: `TestAuthCalloutVisitorCrossTenantDenied` proves a T1 visitor cannot sub/pub any `tenant.T2.*` (distinct from the existing cross-conversation case) while same-tenant own conversation still reads.
- HMAC-gating decision: secret→role gate via an opt-in `AllowHMACTrustedBackend()` option (default = SECURE/deny), enabled in main.go ONLY in the dev posture (DESK_INGRESS_JWKS_URL unset). No new flag/env required.
- Verify (all pass): `go build ./...` OK; `go vet ./...` OK; `gofmt -l .` clean; `go test ./...` OK; `go test -tags integration ./...` OK (authcallout 8.9s, full ~10s). Same-tenant/agent/trusted-backend/visitor legitimate grants all still pass (no darkened traffic).
