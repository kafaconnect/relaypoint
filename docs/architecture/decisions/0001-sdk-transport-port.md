# ADR-0001 — The client SDK core depends on a Transport port, not on nats.ws

- Status: Accepted
- Date: 2026-06-08
- Scope: `@relaypoint/client` (TS browser SDK), chat subset

## Context

RelayPoint's backbone is NATS (a locked project invariant). The browser SDK talks to it over
`nats.ws`. The loose-coupling HARD RULE (see `AGENTS.md`) requires that core logic depend on
abstractions it owns, not on a concrete client — so the backbone can be swapped and the core
can be unit-tested without live infrastructure.

The SDK core is non-trivial: connection lifecycle with token-refresh reconnect, command
idempotency over request/reply, and ordered/deduped/gap-replayed `.log` delivery. That logic
must be testable deterministically, without standing up NATS + JetStream.

## Decision

The SDK core depends only on a `Transport` port the SDK owns
(`connect` / `publish` / `request` / `subscribe` / `replay` / `onStatus`). Concrete transports
are adapters at the edge:

- `NatsWsTransport` (`src/adapters/nats.ts`) is the production adapter and the **only** file
  that imports `nats.ws`.
- `FakeTransport` (`src/testing/fake-transport.ts`) is an in-memory adapter used by every unit
  test — no live NATS.

`RelayPointClient`, `InteractionHandle`, `Delivery`, and the codec import `transport.ts` only.
Reconnection and token refresh live in the core (not the transport) so the SDK can mint a fresh
scoped token per connection; the adapter therefore disables nats.ws auto-reconnect and surfaces
a transient drop as a non-final `disconnected` status the core reacts to.

## Consequences

- The whole core is exercised against the port — 18 chat-subset scenarios run with no infra.
- A different backbone (or a non-NATS test double) needs only a new `Transport` implementation;
  no core rewrite.
- The adapter is thin and infra-specific; bugs there are caught by integration tests against a
  live server (added when the chat router is deployed), not by the core unit suite.
- `Transport.replay` is defined to **throw** when the durable store is unreachable, so the
  delivery plane fails closed over a sequence gap rather than resuming silently.
