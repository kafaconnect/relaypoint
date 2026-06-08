// InteractionHandle (chat subset): a read-only ordered `.log` stream, a write-only `.cmd`
// command path, own-author `.signal` publish, and the latest opaque interaction context
// (metadata). It NEVER writes `.log` and NEVER assigns `sequence` — the router is the sole
// authoritative writer. The handle depends only on the Transport port.

import { decodeLogEvent, encodeCommand, encodeSignal, decodeCommandResult } from "./codec.js";
import { Delivery } from "./delivery.js";
import { Emitter } from "./emitter.js";
import { CommandRejectedError } from "./errors.js";
import { cmdSubject, logSubject, signalSubject } from "./subjects.js";
import type { Transport, Subscription } from "./transport.js";
import type { Command, CommandResult, DeliveryState, LogEvent, SignalEvent } from "./types.js";

export interface InteractionConfig {
  readonly requestTimeoutMs: number;
  readonly sendRetries: number; // retries reuse the same command_id (router dedups)
  readonly medium: string;
}

interface HandleEvents extends Record<string, (...args: never[]) => void> {
  metadata: (context: unknown) => void;
  delivery: (state: DeliveryState) => void;
}

export class InteractionHandle {
  readonly id: string;
  private readonly emitter = new Emitter<HandleEvents>();
  private readonly delivery: Delivery;
  private liveSub?: Subscription;
  private _metadata: unknown = null;
  private opened = false;

  constructor(
    private readonly transport: () => Transport,
    private readonly ctx: { tenantId: string; selfUserId: string; interactionId: string },
    private readonly cfg: InteractionConfig,
  ) {
    this.id = ctx.interactionId;
    this.delivery = new Delivery({
      replay: (from) => this.replayFrom(from),
      onState: (s) => this.emitter.emit("delivery", s),
      onApplied: (ev) => this.onApplied(ev),
    });
  }

  // Lazily open the live `.log` subscription on first use; the client re-invokes this after a
  // reconnect so delivery resumes (a gap during the drop is healed by replay).
  open(): void {
    if (this.opened) return;
    this.opened = true;
    this.subscribeLive();
  }

  private subscribeLive(): void {
    this.liveSub?.unsubscribe();
    this.liveSub = this.transport().subscribe(logSubject(this.ctx.tenantId, this.id), (msg) => {
      this.delivery.offer(decodeLogEvent(msg.data));
    });
  }

  // Called by the client after a reconnect: re-attach the live subscription on the new
  // connection. Delivery's gap-replay fills anything missed while disconnected.
  resubscribe(): void {
    if (this.opened) this.subscribeLive();
  }

  private async *replayFrom(from: number): AsyncIterable<LogEvent> {
    for await (const msg of this.transport().replay(logSubject(this.ctx.tenantId, this.id), from)) {
      yield decodeLogEvent(msg.data);
    }
  }

  private onApplied(ev: LogEvent): void {
    if (ev.eventType === "interaction.context.updated") {
      this._metadata = ev.data; // opaque — the SDK never parses it
      this.emitter.emit("metadata", ev.data);
    }
  }

  events(): AsyncIterable<LogEvent> {
    this.open();
    return this.delivery.events();
  }

  async send(command: Command): Promise<CommandResult> {
    const subject = cmdSubject(this.ctx.tenantId, this.id);
    const payload = encodeCommand(command, {
      tenantId: this.ctx.tenantId,
      actorId: this.ctx.selfUserId,
      medium: this.cfg.medium,
    });
    let lastErr: unknown;
    for (let attempt = 0; attempt <= this.cfg.sendRetries; attempt++) {
      try {
        const reply = await this.transport().request(subject, payload, {
          timeoutMs: this.cfg.requestTimeoutMs,
        });
        const result = decodeCommandResult(reply.data);
        if (result.status === "rejected") {
          throw new CommandRejectedError(result.commandId, result.reason ?? "rejected");
        }
        return result;
      } catch (err) {
        if (err instanceof CommandRejectedError) throw err; // a verdict, not a transport failure
        lastErr = err; // transport timeout / no-responder — retry with the SAME command_id
      }
    }
    throw lastErr;
  }

  async signal(s: SignalEvent): Promise<void> {
    const subject = signalSubject(this.ctx.tenantId, this.id, this.ctx.selfUserId);
    this.transport().publish(subject, encodeSignal(s.type, this.ctx.selfUserId, s.data));
  }

  get metadata(): unknown {
    return this._metadata;
  }

  on<E extends keyof HandleEvents>(event: E, cb: HandleEvents[E]): () => void {
    return this.emitter.on(event, cb);
  }

  close(): void {
    this.liveSub?.unsubscribe();
    this.delivery.close();
  }
}
