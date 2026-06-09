# Delta for Deploy (Phase-1 single-node)

The runnable single-node signaling deployment. Subjects/streams are owned by `signaling-core`;
this capability owns how the plane is **stood up, secured at the NATS edge, and verified**.
Scenario ids are `deploy.<area>.<case>`; each is satisfied by `deploy/verify.sh` and/or the
committed compose/config.

## ADDED Requirements

### Requirement: Single-node signaling plane brought up by one command
The deployment SHALL stand up the full Phase-1 signaling plane — NATS (JetStream + `nats.ws`
websocket + `$SYS`), the router service, and coturn — from `deploy/` with a single
`docker compose up`. JetStream storage SHALL be durable across container restarts (a named
volume), so the `interaction.<id>.log` facts survive a NATS restart.

#### Scenario: Compose brings up the whole plane
- **id:** `deploy.bringup.one-command`
- **GIVEN** a checkout with Docker available
- **WHEN** an operator runs `docker compose up` in `deploy/`
- **THEN** NATS, the router, and coturn all start, NATS exposes the client (4222), monitoring
  (8222), and websocket (8088) ports, and JetStream persists to a named volume that survives a
  container restart

### Requirement: The router starts only after NATS is healthy
The compose SHALL gate the router on a NATS **health check** (the monitoring `/healthz`
endpoint), so the router connects only once NATS is ready — a cold `up` MUST NOT depend on the
router crash-looping until NATS happens to accept connections. Every long-lived service SHALL
declare a restart policy.

#### Scenario: Router waits for NATS readiness
- **id:** `deploy.health.router-waits-for-nats`
- **GIVEN** a cold `docker compose up`
- **WHEN** the stack starts
- **THEN** the router container starts only after the NATS health check passes
  (`depends_on: { condition: service_healthy }`), not merely after NATS is created

### Requirement: Dev credentials are externalised, never committed as secrets
Credentials SHALL be supplied via an untracked `.env` (interpolated into the compose), with a
committed `deploy/.env.example` documenting every variable. No real secret value SHALL be
committed; the Phase-1 dev defaults in `.env.example` SHALL be clearly marked dev-only.

#### Scenario: Credentials come from .env, not tracked files
- **id:** `deploy.creds.env-externalised`
- **GIVEN** the compose
- **WHEN** the router and coturn credentials are resolved
- **THEN** they are read from `.env` (`${ROUTER_PASSWORD}`, `${COTURN_USER}`,
  `${COTURN_PASSWORD}`), a committed `.env.example` lists them, and `.env` is gitignored

### Requirement: The NATS edge enforces the router-authoritative security boundary
The NATS account ACLs SHALL enforce that browser/app `client` users are READ-only on
`interaction.*.log` (publish denied) and WRITE-only on `.cmd`/`.signal.*`, while the `router`
user is the sole writer of `.log`. `$SYS` SHALL be the system account. This is the edge backstop
for the router-authoritative model — a client MUST NOT be able to forge a `.log` fact even if
the router has a bug.

#### Scenario: Clients are denied publish on .log at the NATS edge
- **id:** `deploy.security.client-log-write-denied`
- **GIVEN** the NATS account config
- **WHEN** a `client`-user connection attempts to publish on `tenant.*.interaction.*.log`
- **THEN** the account ACL denies it (publish allow-list excludes `.log`; an explicit deny is
  present), while `.cmd` and `.signal.*` publishes are allowed and the `router` user may write `.log`

### Requirement: A smoke check verifies the plane is actually up
The deployment SHALL ship `deploy/verify.sh` that asserts, and exits non-zero on any failure:
NATS reports healthy, JetStream is enabled, the `INTERACTION_LOGS` stream exists, and the router
is connected. "Up" SHALL be a one-command, machine-checkable answer, not a visual guess.

#### Scenario: verify.sh asserts health, JetStream, stream, and router
- **id:** `deploy.verify.smoke-check`
- **GIVEN** the plane is running
- **WHEN** an operator runs `deploy/verify.sh`
- **THEN** it confirms NATS `/healthz`, JetStream enabled (`/jsz`), the `INTERACTION_LOGS`
  stream present, and the router connected (`/connz`), exiting non-zero if any check fails
