# Cross-review: capability-only subscriber cutover

## Scope

- Change: `substrate-boundary-invariant-rp`
- Task: `SBI-RP-03`
- Reviewer: Gemini CLI (`agy`)
- Builder: Codex
- Mode: independent read-only review

## Verdict

`PASS_WITH_FINDINGS`

## Findings

No BLOCKER, HIGH, or MEDIUM findings.

### LOW: subscriber capability missing from allowed-connection log

`internal/authcallout/responder.go` logged an empty role for capability-authorized
subscribers without logging the capability that granted access.

## Resolution

`authcallout.allow` now includes `capability` for capability-authorized identities and
omits it for role-authorized identities. `TestAuthAllowAttrsIncludesSubscriberCapability`
and `TestAuthAllowAttrsOmitsCapabilityForRoleIdentity` cover both paths.

## Evidence

- `go test ./internal/authcallout -count=1`: PASS
- Final zero-finding review is recorded separately.
