import type { LogEvent } from "../src/types.js";

const enc = new TextEncoder();
const dec = new TextDecoder();

// Build a snake_case wire `.log` fact (as the router would write it).
export function wireEvent(e: {
  sequence: number;
  eventType?: string;
  eventId?: string;
  occurredAt?: string;
  causedBy?: string;
  data?: unknown;
}): Uint8Array {
  return enc.encode(
    JSON.stringify({
      schema: "relaypoint.interaction.v1",
      event_type: e.eventType ?? "message.created",
      event_id: e.eventId ?? `ev-${e.sequence}`,
      sequence: e.sequence,
      occurred_at: e.occurredAt ?? "2026-06-08T00:00:00Z",
      tenant_id: "t1",
      actor_id: "alice",
      medium: "chat",
      ...(e.causedBy !== undefined ? { caused_by: e.causedBy } : {}),
      ...(e.data !== undefined ? { data: e.data } : {}),
    }),
  );
}

export function wireResult(r: {
  commandId: string;
  status: "accepted" | "rejected";
  causedBy?: string;
  reason?: string;
}): Uint8Array {
  return enc.encode(
    JSON.stringify({
      command_id: r.commandId,
      status: r.status,
      ...(r.causedBy !== undefined ? { caused_by: r.causedBy } : {}),
      ...(r.reason !== undefined ? { reason: r.reason } : {}),
    }),
  );
}

export function readJson(bytes: Uint8Array): Record<string, unknown> {
  return JSON.parse(dec.decode(bytes)) as Record<string, unknown>;
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
