# Delta for Auth-callout — realtime hardening

Hardens the NATS auth-callout responder's grant policy (defense-in-depth: the router has NO
defense-in-depth for visitor scope, so the callout's fail-closed + least-privilege posture is the
backstop). Grants are minted in `internal/authcallout/grants.go` from the authenticated `Identity`;
this delta tightens the role default, the HMAC link, the `$JS.API` scope, and the presence
wildcards, and adds a cross-TENANT visitor-denial guarantee.

## ADDED Requirements

### Requirement: An unknown role mints no permissions (fail closed)

The grant `switch RoleOf(id)` MUST make `RoleAgent` an EXPLICIT case and its `default` arm MUST
**deny** (`return Grant{}, error`). An unknown or unmapped role MUST authorize NOTHING — it MUST NOT
fall through to the agent grant. (Today the default arm mints the agent grant and an empty role maps
to `RoleAgent`, so an unrecognized role is silently granted agent permissions — fail-open.)

#### Scenario: An unrecognized role is denied, not granted agent permissions
- **id:** `authcallout.role.fail-closed-unknown`
- **GIVEN** an authenticated `Identity` whose role is unknown/unmapped (neither visitor, agent, nor trusted-backend)
- **WHEN** `GrantsFor` mints its permissions
- **THEN** it returns an error and an empty `Grant` (the connection authorizes nothing) — it does NOT fall through to the `RoleAgent` grant

### Requirement: The HMAC self-assertion link cannot mint a trusted backend in production

The process-wide `AUTH_TOKEN_SECRET` HMAC link lets a holder self-assert ANY tenant and the
`trusted-backend` role. In PRODUCTION the responder MUST NOT honour `role=trusted-backend` over the
HMAC path: the HMAC link MUST be gated behind a dev flag (and OMITTED when JWKS is configured) or the
secret MUST be bound to a fixed tenant/role. At minimum, a `trusted-backend` role asserted over HMAC
MUST be rejected in production.

#### Scenario: HMAC cannot self-assert trusted-backend in production
- **id:** `authcallout.hmac.no-trusted-backend-prod`
- **GIVEN** a production responder with JWKS configured and an HMAC token asserting `role=trusted-backend`
- **WHEN** the responder verifies it
- **THEN** the HMAC link is absent (omitted when JWKS is configured) or the `trusted-backend` assertion is rejected — no minted connection gains trusted-backend authority from a process-wide HMAC secret

### Requirement: A minted connection holds least-privilege JetStream API scope, never $JS.API.>

The trusted-backend `PubAllow` MUST NOT include account-wide `$JS.API.>`. It MUST be scoped to the
specific JS API subjects actually used (e.g. `$JS.API.CONSUMER.CREATE.<stream>.>`,
`$JS.API.STREAM.INFO.<stream>`), or JS admin MUST be moved to a STATIC, non-minted identity. No
minted (callout-issued) connection may hold account-wide JetStream administration.

#### Scenario: Trusted-backend JS.API grant is subject-scoped, not account-wide
- **id:** `authcallout.jsapi.least-privilege`
- **GIVEN** a trusted-backend connection minted by the callout
- **WHEN** its `PubAllow` is computed
- **THEN** it lists only the specific `$JS.API.CONSUMER.*`/`$JS.API.STREAM.INFO.*` subjects it needs for its streams, NOT the account-wide `$JS.API.>` (or JS admin is held by a static non-minted identity)

### Requirement: Presence grants are tightened to literal state/typing subjects

The agent presence grants MUST be tightened from tail-wildcards to the documented literals: publish
`presence.<self>.state` and `presence.<self>.typing.>` (identity-pinned), subscribe
`presence.*.state` and `presence.*.typing.>` (tenant-scoped) — NOT the broader `presence.<self>.>`
(pub) / `presence.*.>` (sub), which grant more than the presence state + per-conversation typing the
console reads.

#### Scenario: Presence pub/sub grants are scoped to state and typing only
- **id:** `authcallout.presence.scoped-subjects`
- **GIVEN** an agent connection minted by the callout
- **WHEN** its presence grants are computed
- **THEN** publish is limited to `presence.<self>.state` + `presence.<self>.typing.>` and subscribe to `presence.*.state` + `presence.*.typing.>` — not the tail-wildcard `presence.<self>.>` / `presence.*.>`

### Requirement: A visitor is denied any other tenant's subjects

A minted visitor connection MUST be confined to its own tenant: it MUST NOT read or write any
subject of a DIFFERENT tenant. This is asserted by a cross-TENANT denial test, not only the existing
cross-CONVERSATION test.

#### Scenario: A visitor cannot reach another tenant's interaction
- **id:** `authcallout.tenant.cross-denied`
- **GIVEN** a visitor minted for tenant `T1` (bound to its conversation)
- **WHEN** it attempts to subscribe or publish any subject under `tenant.T2.*` (a different tenant)
- **THEN** the minted grant denies it (the tenant prefix is ACL-fixed) — cross-tenant reach is impossible, proven by an integration test distinct from the cross-conversation case
