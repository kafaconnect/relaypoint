// Protobuf wire codec (ADR-0002). The SDK's public API stays camelCase (LogEvent/Command/
// CommandResult); this module is the only place that touches the generated messages and the
// payload registry — chat `data` is a ChatMessage, everything else is opaque bytes.

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ChatMessageSchema,
  CommandResultSchema,
  CommandResult_Status,
  CommandSchema,
  EventSchema,
  FeedControlSchema,
  SignalEventSchema,
} from "./gen/relaypoint/interaction/v1/interaction_pb.js";
import {
  SCHEMA_V1,
  type AgentFeedItem,
  type ChatPayload,
  type Command,
  type CommandResult,
  type LogEvent,
} from "./types.js";
import { interactionIdFromFeedSubject } from "./subjects.js";

export interface CommandContext {
  readonly tenantId: string;
  readonly actorId: string;
  readonly medium: string;
}

// chat message.* facts/commands carry a ChatMessage in `data`; other payloads stay opaque bytes.
function isChatMessage(medium: string, eventOrType: string): boolean {
  return medium === "chat" && eventOrType.startsWith("message.");
}

function encodePayload(medium: string, type: string, data: unknown): Uint8Array {
  if (data === undefined || data === null) return new Uint8Array();
  if (data instanceof Uint8Array) return data; // opaque blob (e.g. SDP) — pass through
  if (isChatMessage(medium, type)) {
    const p = data as Partial<ChatPayload>;
    return toBinary(ChatMessageSchema, create(ChatMessageSchema, { text: p.text ?? "", attachmentRefs: p.attachmentRefs ?? [] }));
  }
  return data as Uint8Array; // non-registry typed payload: caller supplies the encoded bytes
}

function decodePayload(medium: string, eventType: string, data: Uint8Array): unknown {
  if (isChatMessage(medium, eventType)) {
    const m = fromBinary(ChatMessageSchema, data);
    return { text: m.text, attachmentRefs: m.attachmentRefs } satisfies ChatPayload;
  }
  return data; // opaque per registry — surfaced as raw bytes
}

export function encodeCommand(cmd: Command, ctx: CommandContext): Uint8Array {
  const msg = create(CommandSchema, {
    commandId: cmd.commandId,
    tenantId: ctx.tenantId,
    actorId: ctx.actorId,
    type: cmd.type,
    medium: ctx.medium,
    refId: cmd.refId ?? "",
    data: encodePayload(ctx.medium, cmd.type, cmd.data),
  });
  return toBinary(CommandSchema, msg);
}

export function decodeCommandResult(bytes: Uint8Array): CommandResult {
  const w = fromBinary(CommandResultSchema, bytes);
  const status = w.status === CommandResult_Status.ACCEPTED ? "accepted" : "rejected";
  return {
    commandId: w.commandId,
    status,
    ...(w.causedBy !== "" ? { causedBy: w.causedBy } : {}),
    ...(w.reason !== "" ? { reason: w.reason } : {}),
  };
}

export function decodeLogEvent(bytes: Uint8Array): LogEvent {
  const w = fromBinary(EventSchema, bytes);
  return {
    schema: w.schema,
    tenantId: w.tenantId,
    eventType: w.eventType,
    eventId: w.eventId,
    sequence: Number(w.sequence),
    occurredAt: w.occurredAt ? timestampToIso(w.occurredAt.seconds, w.occurredAt.nanos) : "",
    actorId: w.actorId,
    medium: w.medium,
    ...(w.mediaProfile !== "" ? { mediaProfile: w.mediaProfile } : {}),
    ...(w.causedBy !== "" ? { causedBy: w.causedBy } : {}),
    ...(w.refId !== "" ? { refId: w.refId } : {}),
    data: decodePayload(w.medium, w.eventType, w.data),
  };
}

export function decodeAgentFeedItem(bytes: Uint8Array, subject: string | undefined): AgentFeedItem {
  const ctrl = fromBinary(FeedControlSchema, bytes);
  if (ctrl.schema === SCHEMA_V1 && ctrl.control.startsWith("feed.")) {
    const item = { interactionId: ctrl.interactionId, atSequence: Number(ctrl.atSequence) };
    if (ctrl.control === "feed.revoked") return { kind: "revoked", ...item };
    return { kind: "control", control: ctrl.control, ...item };
  }
  const interactionId = interactionIdFromFeedSubject(subject);
  if (!interactionId) throw new Error("agent feed Event missing concrete feed subject");
  return { kind: "event", interactionId, event: decodeLogEvent(bytes) };
}

export function encodeSignal(type: string, actorId: string, data: unknown): Uint8Array {
  const msg = create(SignalEventSchema, {
    schema: SCHEMA_V1,
    type,
    actorId,
    data: data instanceof Uint8Array ? data : new Uint8Array(),
  });
  return toBinary(SignalEventSchema, msg);
}

function timestampToIso(seconds: bigint, nanos: number): string {
  const ms = Number(seconds) * 1000 + Math.floor(nanos / 1e6);
  return new Date(ms).toISOString();
}
