// camelCase projection of the signaling-core wire envelope; mapping in
// openspec/changes/client-sdk/design.md ("Wire-field naming").

export const SCHEMA_V1 = "relaypoint.interaction.v1";

export type ConnectionState =
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "closed"
  | "failed";

export interface LogEvent {
  readonly schema: string;
  readonly tenantId: string;
  readonly eventType: string;
  readonly eventId: string;
  readonly sequence: number;
  readonly occurredAt: string; // display-only — never order/dedup/secure by this
  readonly actorId: string;
  readonly medium: string;
  readonly mediaProfile?: string;
  readonly causedBy?: string;
  readonly refId?: string;
  readonly data: unknown;
}

export interface Command {
  readonly type: string;
  readonly commandId: string; // reused across retries so the router dedups
  readonly refId?: string;
  readonly data?: unknown;
}

export interface CommandResult {
  readonly commandId: string;
  readonly status: "accepted" | "rejected";
  readonly causedBy?: string;
  readonly reason?: string;
}

export interface SignalEvent {
  readonly type: string;
  readonly data?: unknown;
}

export type DeliveryState = "live" | "replaying" | "degraded" | "failed";
