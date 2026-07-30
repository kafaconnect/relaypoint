# Delta for Auth Callout

## ADDED Requirements

### Requirement: Signed admin-operation observer profile

For a Desk-signed agent token, RelayPoint SHALL retain the scalar `cap` claim in the verified
identity. The exact profile `agent-feed-admin-operation-observer` SHALL extend the existing agent
grant without replacing or reducing it. A missing, `agent-feed`, or unknown profile SHALL NOT gain
admin-operation access. The dev HMAC token grammar SHALL remain unchanged.

#### Scenario: Signed profile survives verification
- **id:** `admin-operation.profile-verifies`
- **GIVEN** a valid Desk EdDSA agent token for tenant T with
  `cap=agent-feed-admin-operation-observer`
- **WHEN** RelayPoint verifies the token
- **THEN** the trusted identity contains that exact capability and the verified tenant and subject

### Requirement: Tenant-scoped subscribe-only operation hints

The exact combined profile SHALL retain every existing agent permission and add only subscribe
access to the ephemeral core-NATS subject
`tenant.<tid>.admin.operation.<channel_account_id>.changed`, represented by the grant pattern
`tenant.<tid>.admin.operation.*.changed`. It SHALL add no publish permission. The tenant token SHALL
come from the verified identity. RelayPoint SHALL forward the payload opaquely and SHALL NOT parse
the Desk AsyncAPI body `{"channel_account_id":"<UUIDv7>"}`.

#### Scenario: Grant is additive, subscribe-only, and tenant-isolated
- **id:** `admin-operation.grant-isolated`
- **GIVEN** an agent identity for tenant T with the exact combined profile
- **WHEN** RelayPoint constructs its grant
- **THEN** same-tenant operation-hint subscribe and the existing own agent feed are allowed
- **AND** cross-tenant subscribe, browser hint publish, and normal-agent hint subscribe are denied

#### Scenario: NATS enforces the operation-hint ACL
- **id:** `admin-operation.nats-enforced`
- **GIVEN** an embedded NATS server using RelayPoint auth callout and a Desk-signed combined-profile
  token for tenant T
- **WHEN** the browser subscribes or publishes operation-hint subjects
- **THEN** same-tenant subscribe succeeds while cross-tenant subscribe and browser publish fail

