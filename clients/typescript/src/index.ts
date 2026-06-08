// @relaypoint/client — chat subset. Public surface over the signaling-core router-authoritative
// protocol. The core depends only on the Transport port; nats.ws is a swappable adapter.

export { RelayPointClient } from "./client.js";
export type { RelayPointClientOptions, RelayPointClientDeps } from "./client.js";
export { InteractionHandle } from "./interaction.js";
export type { InteractionConfig } from "./interaction.js";
export { NatsWsTransport } from "./adapters/nats.js";
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
  LogEvent,
  Command,
  CommandResult,
  SignalEvent,
} from "./types.js";
export { SCHEMA_V1 } from "./types.js";

import { RelayPointClient, type RelayPointClientOptions } from "./client.js";
import { NatsWsTransport } from "./adapters/nats.js";

// Convenience constructor that wires the default nats.ws adapter. Use the RelayPointClient
// constructor directly with a custom Transport to swap the backbone.
export function createRelayPointClient(options: RelayPointClientOptions): RelayPointClient {
  return new RelayPointClient(options, { transport: new NatsWsTransport(options.servers) });
}
