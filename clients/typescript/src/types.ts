// Public chat-subset surface of @relaypoint/client. camelCase projection of the
// signaling-core snake_case wire envelope; the normative field mapping lives in
// openspec/changes/client-sdk/design.md ("Wire-field naming"). Only the chat-buildable
// types live here — call/media/recording/transfer remain design-deferred (no server yet).

export const SCHEMA_V1 = "relaypoint.interaction.v1";

export type ConnectionState =
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "closed"
  | "failed";

// A router-written `.log` fact (read-only for clients). Projects the envelope fields that
// apply to FACTS: it carries `causedBy` (the command_id that produced it) but NOT
// `commandId` (command-only). Event-specific fields ride inside `data`, never promoted.
export interface LogEvent {
  readonly schema: string;
  readonly tenantId: string;
  readonly eventType: string;
  readonly eventId: string;
  readonly sequence: number; // router-assigned; the client never sets it
  readonly occurredAt: string; // display-only — never used for ordering/dedup/security
  readonly actorId: string;
  readonly medium: string;
  readonly causedBy?: string; // = the command_id that produced this fact
  readonly refId?: string;
  readonly data: unknown;
}

// A client intent published on `interaction.<id>.cmd`. `commandId` is the client-generated
// idempotency key, REUSED across retries so the router dedups. It is command-only.
export interface Command {
  readonly type: string;
  readonly commandId: string;
  readonly refId?: string; // required by the router for message.updated/deleted
  readonly data?: unknown;
}

// The router's ephemeral reply (core NATS, never persisted). An ack/correlation — the
// authoritative effect is the `.log` fact whose `causedBy = commandId`.
export interface CommandResult {
  readonly commandId: string;
  readonly status: "accepted" | "rejected";
  readonly causedBy?: string;
  readonly reason?: string;
}

export interface SignalEvent {
  readonly type: string; // typing.* | webrtc.ice | ...
  readonly data?: unknown;
}

// Delivery-plane state of an interaction's `.log` stream (ordered-by-sequence consumer):
//  live      — applying facts as they arrive
//  replaying — a sequence gap was detected; live apply paused, replaying from JetStream
//  degraded  — replay could not reach JetStream; retrying with backoff (no facts dropped)
//  failed    — replay retries exhausted; the gap is unrecoverable (terminal)
export type DeliveryState = "live" | "replaying" | "degraded" | "failed";
