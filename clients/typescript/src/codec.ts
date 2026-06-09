// camelCase(TS) <-> snake_case(wire) projection; mapping table in design.md.

import { SCHEMA_V1, type Command, type CommandResult, type LogEvent } from "./types.js";

const enc = new TextEncoder();
const dec = new TextDecoder();

export interface CommandContext {
  readonly tenantId: string;
  readonly actorId: string;
  readonly medium: string;
}

interface WireCommand {
  command_id: string;
  tenant_id: string;
  actor_id: string;
  type: string;
  medium: string;
  ref_id?: string;
  data?: unknown;
}

export function encodeCommand(cmd: Command, ctx: CommandContext): Uint8Array {
  const wire: WireCommand = {
    command_id: cmd.commandId,
    tenant_id: ctx.tenantId,
    actor_id: ctx.actorId,
    type: cmd.type,
    medium: ctx.medium,
    ...(cmd.refId !== undefined ? { ref_id: cmd.refId } : {}),
    ...(cmd.data !== undefined ? { data: cmd.data } : {}),
  };
  return enc.encode(JSON.stringify(wire));
}

interface WireCommandResult {
  command_id: string;
  status: "accepted" | "rejected";
  caused_by?: string;
  reason?: string;
}

export function decodeCommandResult(bytes: Uint8Array): CommandResult {
  const w = JSON.parse(dec.decode(bytes)) as WireCommandResult;
  return {
    commandId: w.command_id,
    status: w.status,
    ...(w.caused_by !== undefined ? { causedBy: w.caused_by } : {}),
    ...(w.reason !== undefined ? { reason: w.reason } : {}),
  };
}

interface WireEvent {
  schema: string;
  event_type: string;
  event_id: string;
  sequence: number;
  occurred_at: string;
  tenant_id: string;
  actor_id: string;
  medium: string;
  media_profile?: string;
  command_id?: string; // router-internal; not projected onto LogEvent (caused_by is the public link)
  payload_hash?: string; // router-internal idempotency metadata; clients ignore it
  caused_by?: string;
  ref_id?: string;
  data?: unknown;
}

export function decodeLogEvent(bytes: Uint8Array): LogEvent {
  const w = JSON.parse(dec.decode(bytes)) as WireEvent;
  return {
    schema: w.schema,
    tenantId: w.tenant_id,
    eventType: w.event_type,
    eventId: w.event_id,
    sequence: w.sequence,
    occurredAt: w.occurred_at,
    actorId: w.actor_id,
    medium: w.medium,
    ...(w.media_profile !== undefined ? { mediaProfile: w.media_profile } : {}),
    ...(w.caused_by !== undefined ? { causedBy: w.caused_by } : {}),
    ...(w.ref_id !== undefined ? { refId: w.ref_id } : {}),
    data: w.data ?? null,
  };
}

interface WireSignal {
  schema: string;
  type: string;
  actor_id: string;
  data?: unknown;
}

export function encodeSignal(type: string, actorId: string, data: unknown): Uint8Array {
  const wire: WireSignal = {
    schema: SCHEMA_V1,
    type,
    actor_id: actorId,
    ...(data !== undefined ? { data } : {}),
  };
  return enc.encode(JSON.stringify(wire));
}
