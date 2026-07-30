# Design — Agent-feed admin operation observer

## Baseline

Desk commit `4f55fbfe2d06ed63a17103b9ac2f35a0cf2b9f05` fixes the cross-repo contract:

- normal agent: scalar `cap=agent-feed`;
- verified admin: scalar `cap=agent-feed-admin-operation-observer`;
- subject: `tenant.<tid>.admin.operation.<channel_account_id>.changed`;
- body: only `channel_account_id`;
- Desk API is the publisher; browsers are subscribe-only.

RelayPoint baseline `b36a42814bdc12e5b74b9d3d991729fbce5499e0` verifies Desk EdDSA tokens but discards
their `cap` claim. `GrantsFor` already constructs tenant-pinned agent permissions.

## Decision

Add `Capability string` to the verified `signaling.Identity`. The Desk EdDSA verifier copies the
signed scalar `cap` claim into that field for agent tokens. Visitor and trusted-backend behavior is
unchanged. The dev HMAC token grammar is unchanged; this change does not add a new self-assertion
path.

Build the existing agent grant first. Only when capability equals the exact fixed profile, append:

```text
tenant.<verified-tenant>.admin.operation.*.changed
```

to `SubAllow`. Do not append any publish permission. An absent, normal, or unknown capability keeps
the existing agent grant and receives no admin-operation subject. Tenant interpolation continues to
use the verified and subject-token-validated identity.

The message is ephemeral core NATS. RelayPoint authorizes and forwards opaque bytes; it does not
decode `channel_account_id` or branch on channel/provider state. Desk AsyncAPI owns the payload
contract and must use the exact subject and account-id-only body pinned in this change.

## Pseudocode

```text
verify Desk JWT:
  validate signature, issuer, audience, expiry, tenant, subject
  if role == agent:
    return identity(tenant, subject, role, signed cap string)

grant agent:
  grant = existing agent grant
  if identity.capability == agent-feed-admin-operation-observer:
    grant.subscribe += tenant.<identity.tenant>.admin.operation.*.changed
  return grant
```

## Failure and compatibility

- Missing or `agent-feed` capability: existing agent permissions only.
- Unknown capability: existing agent permissions only; no privilege expansion.
- Cross-tenant subscribe: denied because the grant contains only the verified tenant.
- Browser publish: denied because no matching publish permission is added.
- Capability on a visitor or trusted backend: ignored by their role-specific grant.
- Existing feeds, commands, presence, inboxes, router, projector, streams, and clients are unchanged.

