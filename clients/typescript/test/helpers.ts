import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ChatMessageSchema,
  CommandResultSchema,
  CommandResult_Status,
  CommandSchema,
  EventSchema,
} from "../src/gen/relaypoint/interaction/v1/interaction_pb.js";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import type { ChatPayload, LogEvent } from "../src/types.js";

// Encode a chat payload into the `data` bytes (registry: medium=chat, message.*).
export function chatData(p: ChatPayload): Uint8Array {
  return toBinary(ChatMessageSchema, create(ChatMessageSchema, p));
}

// Build a protobuf `.log` fact (as the router would write it).
export function wireEvent(e: {
  sequence: number;
  eventType?: string;
  eventId?: string;
  occurredAt?: string;
  causedBy?: string;
  mediaProfile?: string;
  data?: Uint8Array; // already-encoded payload bytes (use chatData() for chat)
}): Uint8Array {
  return toBinary(
    EventSchema,
    create(EventSchema, {
      schema: "relaypoint.interaction.v1",
      eventType: e.eventType ?? "message.created",
      eventId: e.eventId ?? `ev-${e.sequence}`,
      sequence: BigInt(e.sequence),
      occurredAt: timestampFromDate(new Date(e.occurredAt ?? "2026-06-08T00:00:00Z")),
      tenantId: "t1",
      actorId: "alice",
      medium: "chat",
      mediaProfile: e.mediaProfile ?? "",
      causedBy: e.causedBy ?? "",
      data: e.data ?? new Uint8Array(),
    }),
  );
}

export function wireResult(r: {
  commandId: string;
  status: "accepted" | "rejected";
  causedBy?: string;
  reason?: string;
}): Uint8Array {
  return toBinary(
    CommandResultSchema,
    create(CommandResultSchema, {
      commandId: r.commandId,
      status: r.status === "accepted" ? CommandResult_Status.ACCEPTED : CommandResult_Status.REJECTED,
      causedBy: r.causedBy ?? "",
      reason: r.reason ?? "",
    }),
  );
}

// Decode a wire Command back to a plain object (what the router would receive).
export function readCommand(bytes: Uint8Array): Record<string, unknown> {
  const c = fromBinary(CommandSchema, bytes);
  return {
    command_id: c.commandId,
    tenant_id: c.tenantId,
    actor_id: c.actorId,
    type: c.type,
    medium: c.medium,
    ref_id: c.refId,
    data: c.data,
  };
}

// A plain LogEvent (for unit-testing the Delivery plane directly).
export function logEvent(sequence: number, over: Partial<LogEvent> = {}): LogEvent {
  return {
    schema: "relaypoint.interaction.v1",
    tenantId: "t1",
    eventType: "message.created",
    eventId: `ev-${sequence}`,
    sequence,
    occurredAt: "2026-06-08T00:00:00Z",
    actorId: "alice",
    medium: "chat",
    data: null,
    ...over,
  };
}

// Pull up to `n` items from an async iterable, stopping if it ends.
export async function take<T>(it: AsyncIterable<T>, n: number): Promise<T[]> {
  const out: T[] = [];
  for await (const v of it) {
    out.push(v);
    if (out.length >= n) break;
  }
  return out;
}

export const immediate = (): Promise<void> => Promise.resolve();
