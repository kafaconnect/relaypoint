# Cross-repo T1 review: RelayPoint capability boundary

- Reviewer: `agy`, independent read-only reviewer
- Reviewed head: `70d77a05f663f8b005b17a106b2514214727b22e`
- Base: `origin/main`
- Desk authority: `5b454f646f9a4dc85d6c7b5769b167049947551c` (read-only)
- Verdict: `PASS`

## R1 State machines

Authentication resolves to exactly one of visitor role, trusted-backend role, known subscriber
capability, or rejection. Subscriber commands require an open participation interval except the
existing interaction-start transition. Closed or absent participation rejects the effect.

## R2 Lifecycle edges

Role-only subscriber tokens, role-plus-capability ambiguity, capability-plus-conversation
ambiguity, unknown capability, missing tenant/subject, expiry, malformed claims, and JWKS failure
all fail closed. Visitor and trusted-backend protocols remain separate and unchanged.

## R3 Races

The router re-evaluates participation from folded durable facts before subscriber effects.
Concurrent participant-left and subscriber command ordering is resolved by the interaction log
sequence and the open membership interval. No roster or product-domain census overrides it.

## R4 Failure and recovery

Token verification dependency failure denies access. Projector restart resumes from its durable
cursor; duplicate participation facts fold idempotently. Desk durable participation publication is
a coordinated dependency and must be merged before the combined cutover.

## R5 Authority, security, and tenancy

Subscriber grants require a closed known capability and carry no role or conversation id. Grants
pin tenant and subscriber identity. Role-only, unknown, forged-author, cross-tenant, broad inbox,
JetStream API, and feed publication attempts are denied by unit and real-NATS tests.

## R6 Contract integrity

The legacy `.agent.` feed namespace is explicitly retained as a wire namespace, not an
authorization role. The change has three stable scenarios:
`substrate.gate-authenticated-subscriber`, `substrate.token-capability-not-role`, and
`substrate.no-domain-branch-static-review`.

## R7 Idempotency, ordering, and delivery

Existing router command-id conflict handling, interaction sequence ordering, projector cursor,
fan-out deduplication, and crash replay remain intact. Participation remains the sole recipient and
command eligibility structure.

## R8 Traceability and DoD

All three scenarios have tagged tests. OpenSpec strict, full race tests, build-test CI, auth-callout
real-NATS coverage, formatting, and diff checks pass.

## Findings

None.

## Dependency

RelayPoint PR #44 may merge independently, but coordinated activation depends on Desk SB-3 durable
participation and capability minting. Desk `5b454f6` was compatibility authority only and did not
receive final review in this round.
