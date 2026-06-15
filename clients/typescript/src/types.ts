// camelCase public surface of the signaling wire. The bytes on the wire are protobuf
// (ADR-0002); these interfaces are a thin projection over the generated messages, so the SDK's
// external API stays camelCase while only the encoding changed.

export const SCHEMA_V1 = "relaypoint.interaction.v1";

export type ConnectionState =
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "closed"
  | "failed";

// The first registry payload (medium='chat', message.*): the decoded `data` of a chat fact.
// Non-chat payloads stay opaque `Uint8Array` until their own registry message is added.
export interface ChatPayload {
  readonly text: string;
  readonly attachmentRefs: string[];
}

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
  readonly data: unknown; // ChatPayload for chat message.* facts; raw Uint8Array otherwise
}

export interface Command {
  readonly type: string;
  readonly commandId: string; // reused across retries so the router dedups
  readonly refId?: string;
  readonly data?: unknown; // ChatPayload for chat message.*; Uint8Array passes through as-is
}

export interface CommandResult {
  readonly commandId: string;
  readonly status: "accepted" | "rejected";
  readonly causedBy?: string;
  readonly reason?: string;
}

export interface SignalEvent {
  readonly type: string;
  readonly data?: unknown; // Uint8Array on the wire (opaque per type)
}

export type DeliveryState = "live" | "replaying" | "degraded" | "failed";

export type AgentFeedItem =
  | {
      readonly kind: "event";
      readonly interactionId: string;
      readonly event: LogEvent;
    }
  | {
      readonly kind: "revoked";
      readonly interactionId: string;
      readonly atSequence: number;
    }
  | {
      readonly kind: "control";
      readonly control: string;
      readonly interactionId: string;
      readonly atSequence: number;
    }
  | {
      readonly kind: "decode_error";
      readonly subject?: string;
      readonly error: unknown;
    };
