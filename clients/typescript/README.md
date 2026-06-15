# @relaypoint/client

Browser SDK over the RelayPoint **signaling-core** router-authoritative protocol.

> **Status: chat subset.** This package currently implements the chat-buildable slice of the
> [`client-sdk` design](../../openspec/changes/client-sdk/design.md) — connection lifecycle,
> the command plane, ordered/deduped/gap-replayed `.log` delivery, the per-agent feed consumer,
> and the interaction handle (`send` / `signal` / `metadata`). Offer-ring, call/media, recording, transfer, and the
> credential ticket-exchange remain design-deferred until the matching server features land.

## Loose coupling

The SDK **core** (`RelayPointClient`, `InteractionHandle`, delivery, codec) depends only on the
[`Transport`](src/transport.ts) port. `nats.ws` is a swappable adapter
([`NatsWsTransport`](src/adapters/nats.ts)) — the **only** file importing it — and the core is
fully unit-tested against an in-memory [`FakeTransport`](src/testing/fake-transport.ts), no live
NATS. A different backbone needs only a new adapter.

## Use

```ts
import { createRelayPointClient } from "@relaypoint/client";

const client = createRelayPointClient({
  servers: ["wss://signal.example.com"],
  tenantId, selfUserId,
  getToken: async () => mintScopedToken(), // refreshed on reconnect
});
await client.connect();

const chat = client.interaction(interactionId);
for await (const fact of chat.events()) render(fact);          // ordered, deduped, gap-healed
const result = await chat.send({ type: "message.created", commandId, data: { text } });

for await (const item of client.agentFeed().events()) {
  switch (item.kind) {
    case "event":
      renderInboxEvent(item.interactionId, item.event);
      break;
    case "revoked":
      closeRevokedThread(item.interactionId, item.atSequence);
      break;
    case "control":
      ignoreUnknownFeedControl(item.control, item.interactionId);
      break;
    case "decode_error":
      reportFeedDecodeError(item.subject, item.error);
      break;
  }
}
```

`send()` is the **only** way to record a fact: it publishes a command on `interaction.<id>.cmd.<self>`
and the router writes the authoritative `.log` fact. The SDK never writes `.log` nor assigns
`sequence`. A retry reuses the same `commandId` so the router dedups.

`agentFeed()` / `inbox()` subscribes only to `tenant.<tenant>.agent.<self>.feed.>` and decodes
projected `Event` copies plus `feed.revoked` tombstones. Conversation history remains Desk REST;
the feed is live delivery only.

## Develop

```sh
pnpm install
pnpm typecheck
pnpm test       # vitest, against the FakeTransport — no live NATS
pnpm build
```
