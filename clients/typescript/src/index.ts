export { RelayPointClient } from "./client.js";
export type { RelayPointClientOptions, RelayPointClientDeps } from "./client.js";
export { InteractionHandle } from "./interaction.js";
export type { InteractionConfig } from "./interaction.js";
export { AgentFeed } from "./agent-feed.js";
export { NatsWsTransport } from "./adapters/nats.js";
// For consumers that do a LIVE core subscribe to `.log` (no JetStream replay — e.g. the desk
// visitor widget, core-subscribe-only) to decode a fact without the high-level InteractionHandle.
export { decodeLogEvent } from "./codec.js";
export { CommandRejectedError, AuthFailedError, DeliveryFailedError } from "./errors.js";
export type {
  Transport,
  TransportMsg,
  TransportStatus,
  Subscription,
  RequestOptions,
} from "./transport.js";
export type {
  ConnectionState,
  DeliveryState,
  ChatPayload,
  LogEvent,
  Command,
  CommandResult,
  SignalEvent,
  AgentFeedItem,
} from "./types.js";
export { SCHEMA_V1 } from "./types.js";

import { RelayPointClient, type RelayPointClientOptions } from "./client.js";
import { NatsWsTransport } from "./adapters/nats.js";

// Wires the default nats.ws adapter; use the constructor with a custom Transport to swap it.
export function createRelayPointClient(options: RelayPointClientOptions): RelayPointClient {
  return new RelayPointClient(options, { transport: new NatsWsTransport(options.servers) });
}
