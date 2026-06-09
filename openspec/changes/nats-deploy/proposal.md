# Change: nats-deploy

## From

The signaling-core change (S3.1) introduced the deploy artifacts — `deploy/docker-compose.yml`,
`deploy/nats/nats-server.conf`, `deploy/router.Dockerfile` — but as a side effect of the
protocol work: there is no documented, health-gated, verifiable bring-up, no externalised dev
credentials, and no smoke check. "It runs on my machine" is not a deployment.

## To

A self-contained, **runnable and verifiable** Phase-1 single-node signaling deployment: NATS
(JetStream durable + `nats.ws` websocket + `$SYS` + MQTT-configured-unused) + the router service
+ coturn, brought up with one command. The compose is **health-gated** (the router starts only
after NATS reports healthy), dev credentials are **externalised** to `.env` (with a committed
`.env.example`, no secrets in git), every long-lived service has a **restart policy**, and a
`deploy/verify.sh` smoke check asserts the plane is actually up (NATS healthy, JetStream enabled,
the `INTERACTION_LOGS` stream present, the router connected). A bring-up **runbook** documents
ports, credentials, persistence, and the verification steps.

## Reason

S3.3 (board issue #4) is the deployment story for the signaling backbone. The protocol is only
useful if the plane it runs on can be stood up reproducibly and *checked*. Two latent defects
make the current compose unreliable: `router depends_on: [nats]` does not wait for NATS to be
*ready* (only started), so on a cold `up` the router races NATS and crash-loops until it happens
to connect; and credentials are hard-coded inline, so the compose cannot be promoted toward a
real environment without editing tracked files. This change closes those and makes "is it up?"
a one-command answer.

## Impact

- `deploy/docker-compose.yml`: NATS `healthcheck` on the monitoring `/healthz`; `router`
  `depends_on: { nats: { condition: service_healthy } }`; `restart` policies; credentials read
  from `.env` (`${ROUTER_PASSWORD}`, `${COTURN_USER}`, `${COTURN_PASSWORD}`).
- New `deploy/.env.example` (committed) + `.env` (gitignored) — Phase-1 dev credentials.
- New `deploy/verify.sh` — health/JetStream/stream/router smoke check (exit non-zero on failure).
- New `docs/runbooks/nats-single-node-deploy.html` — bring-up + verify + ports/creds/persistence.
- `cmd/router/main.go`: fix the cosmetic startup log (`INTERACTION_LOG` → the actual
  `INTERACTION_LOGS` stream name).
- Deferred (Non-goals): the 3-node JetStream RAFT HA cluster + router HA, NKEY/JWT, TLS at the
  NATS edge (terminated upstream in dev), and Kubernetes/k3s manifests — all later phases.

## Non-goals

- **No HA** — single-node NATS + a single router instance; the RAFT cluster is deferred.
- **No TLS termination** at NATS in dev (upstream-terminated); `no_tls` websocket stays Phase-1.
- **No secret manager / NKEY-JWT** — `.env` user/pass is the documented Phase-1 dev posture.
- **No k8s manifests** — the dev cluster apply is tracked separately (desk infra).

## Review tier

T2 (core) — a normal story with some dependents (anyone running the backbone), reversible. One
cross-review round (codex+agy); BLOCKER+HIGH+MEDIUM fixed, LOW logged.
