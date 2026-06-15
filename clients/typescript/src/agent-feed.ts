import { decodeAgentFeedItem } from "./codec.js";
import { Mailbox } from "./mailbox.js";
import { agentFeedSubject } from "./subjects.js";
import type { Subscription, Transport } from "./transport.js";
import type { AgentFeedItem } from "./types.js";

export class AgentFeed {
  private readonly out = new Mailbox<AgentFeedItem>();
  private sub: Subscription | undefined;
  private opened = false;
  private _closed = false;

  constructor(private readonly transport: () => Transport, private readonly ctx: { tenantId: string; selfUserId: string }) {}

  events(): AsyncIterable<AgentFeedItem> {
    this.open();
    return this.out;
  }

  resubscribe(): void {
    if (!this.opened || this._closed) return;
    this.subscribe();
  }

  close(): void {
    if (this._closed) return;
    this._closed = true;
    this.sub?.unsubscribe();
    this.sub = undefined;
    this.out.close();
  }

  get isClosed(): boolean {
    return this._closed;
  }

  private open(): void {
    if (this.opened || this._closed) return;
    this.opened = true;
    this.subscribe();
  }

  private subscribe(): void {
    this.sub?.unsubscribe();
    this.sub = this.transport().subscribe(agentFeedSubject(this.ctx.tenantId, this.ctx.selfUserId), (msg) => {
      try {
        this.out.push(decodeAgentFeedItem(msg.data, msg.subject));
      } catch (err) {
        this.out.push(feedDecodeError(msg.subject, err));
      }
    });
  }
}

function feedDecodeError(subject: string | undefined, error: unknown): AgentFeedItem {
  return subject === undefined ? { kind: "decode_error", error } : { kind: "decode_error", subject, error };
}
