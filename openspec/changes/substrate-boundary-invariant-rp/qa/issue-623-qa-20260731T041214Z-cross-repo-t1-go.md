# QA: RelayPoint capability-only subscriber boundary

- Tracking issue: `kafaconnect/desk#623`
- Reviewed head: `70d77a05f663f8b005b17a106b2514214727b22e`
- Verdict: `GO`

## Scenarios

| Scenario | Test evidence |
|---|---|
| `substrate.gate-authenticated-subscriber` | capability-only JWKS/HMAC grant and participation-gated router tests |
| `substrate.token-capability-not-role` | role-only, ambiguous, and unknown capability denial tests |
| `substrate.no-domain-branch-static-review` | static authorization branch guard |

## Verification matrix

| Surface | Result |
|---|---|
| OpenSpec strict | PASS |
| Full race suite | PASS |
| Auth-callout and signaling targeted tests | PASS |
| Real NATS tenant/ACL denial | PASS |
| Capability-only opaque grant | PASS |
| Participation gate | PASS |
| Visitor/trusted-backend regression | PASS |
| Logging and trace propagation | PASS |

Commands rerun independently:

```text
npx -y @fission-ai/openspec@latest validate substrate-boundary-invariant-rp --strict
GOWORK=off go test -race ./... -count=1
```

Both passed. No container or test process remained. Coordinated live cutover remains gated by the
unfinished Desk SB-3 and is not part of this standalone RelayPoint QA verdict.
