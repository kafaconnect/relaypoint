# Tasks: nats-deploy (S3.3)

## Compose hardening
- [x] NATS `healthcheck` on the monitoring `/healthz` endpoint — `// @spec:deploy.health.router-waits-for-nats`
- [x] `router` `depends_on: { nats: { condition: service_healthy } }` — `// @spec:deploy.health.router-waits-for-nats`
- [x] `restart` policy on nats / router / coturn
- [x] One-command bring-up; JetStream durable on a named volume — `// @spec:deploy.bringup.one-command`

## Credentials
- [x] Externalise router + coturn creds to `.env` interpolation — `// @spec:deploy.creds.env-externalised`
- [x] Commit `deploy/.env.example`; `.env` gitignored — `// @spec:deploy.creds.env-externalised`

## Security boundary (NATS edge)
- [x] Account ACLs deny `client` publish on `.log`, allow `.cmd`/`.signal.*`; `router` sole `.log` writer; `$SYS` system account — `// @spec:deploy.security.client-log-write-denied`

## Verification
- [x] `deploy/verify.sh` — health + JetStream + `INTERACTION_LOGS` stream + router-connected smoke check (exit non-zero on failure) — `// @spec:deploy.verify.smoke-check`

## Polish
- [x] Fix the cosmetic startup log in `cmd/router/main.go` (`INTERACTION_LOG` → `INTERACTION_LOGS`)

## Docs
- [x] `docs/runbooks/nats-single-node-deploy.html` — bring-up, verify, ports/creds/persistence

## Validation (Definition of Done)
- [x] `openspec validate nats-deploy --strict` passes
- [x] `docker compose config` parses; `deploy/verify.sh` is executable and self-contained
- [x] Independent cross-review recorded (builder ≠ reviewer)

## Deferred (own changes / later phases)
- 3-node JetStream RAFT HA cluster + router HA
- NKEY/JWT auth + auth-callout-minted per-connection identity; TLS at the NATS edge
- Kubernetes / k3s manifests (tracked in desk infra)
