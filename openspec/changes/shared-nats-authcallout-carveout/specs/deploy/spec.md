# deploy (delta) — shared-nats-authcallout-carveout

## REMOVED Requirements

### Requirement: Account ACLs deny the shared `client` user publish on `.log`
**Reason:** the dev shared `client` user is REMOVED entirely (ADR-0004 §3) — a callout-exempt static
user is an auth bypass on a per-tenant bus. With no `client` user, the phase-1 scenario
`deploy.security.client-log-write-denied` is obsolete (vacuously satisfied — the identity no longer
exists). Browsers/app/visitors are minted per-connection by the responder; `router` remains the sole
`.log` writer (unchanged).
