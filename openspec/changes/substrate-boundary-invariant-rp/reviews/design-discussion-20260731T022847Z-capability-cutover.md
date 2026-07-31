# Capability cutover design discussion

The cross-repo frame and independent raw verdict are recorded in Desk at
`openspec/changes/substrate-boundary-invariant/reviews/design-discussion-20260731T022847Z-boundary-completion.md`.

- Discussant: `agy` using `gemini-3.6-flash-high`, read-only, 10-minute bound
- Standalone `gemini`: unavailable in `PATH`
- Verdict: `AGREE_WITH_CHANGES`

Agreed corrections:

1. Subscriber tokens omit `role` and `cid`; capability combined with either is rejected.
2. JWKS and HMAC paths both enforce the closed capability set.
3. `GrantsFor` and ordinary command authorization never branch on `RoleAgent`.
4. The current `.agent.` subject is compatibility-only and remains unchanged.

Rejected: role fallback, unknown-capability base grants, and an immediate wire subject rename.
