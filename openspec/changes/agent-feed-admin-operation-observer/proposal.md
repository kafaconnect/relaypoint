# Change: agent-feed-admin-operation-observer

Companion to Desk change `zalo-personal-onboarding-realtime` at
`4f55fbfe2d06ed63a17103b9ac2f35a0cf2b9f05` and Desk issue #601.

## From

Desk agent connect tokens already carry a signed scalar `cap=agent-feed` profile, but RelayPoint
currently ignores `cap` and grants only from the verified `role=agent`. There is no browser grant for
the ephemeral account-change subject required by the approved onboarding design.

## To

RelayPoint recognizes the exact signed profile
`cap=agent-feed-admin-operation-observer` as an additive extension of the existing agent grant. It
preserves every current agent permission and adds only:

```text
subscribe tenant.<tid>.admin.operation.*.changed
```

It adds no publish permission. The profile tenant comes from the verified Desk JWT, so another
tenant remains unreachable. The opaque core-NATS payload coordinated with Desk AsyncAPI is exactly:

```json
{"channel_account_id":"<UUIDv7>"}
```

RelayPoint does not parse that payload.

## Reason

The onboarding modal needs signaling-first invalidation without widening the normal agent profile or
turning RelayPoint into a Zalo state authority. A fixed combined profile fits the existing scalar
claim grammar and avoids a new list claim, protocol, service, environment variable, or persistence.

## Impact

- `internal/signaling`: retain the verified capability on the connection identity.
- `internal/authcallout`: extract the signed `cap` claim and extend the agent subscribe grant for the
  exact combined profile.
- Subject: ephemeral core NATS
  `tenant.<tid>.admin.operation.<channel_account_id>.changed`.
- Streams, router, projector, persistence, schema, environment, and Desk files: unchanged.

