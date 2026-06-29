---
id: RH-08
slice: RH
title: MED — auth-callout fail-closed unknown role plus least-privilege JS.API/presence/HMAC
status: todo
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
- todo
