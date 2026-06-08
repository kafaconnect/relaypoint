// In-memory Transport implementation for unit tests — no live NATS. Proves the loose-coupling
// HARD RULE: the SDK core is fully exercisable against the port alone. Also published under the
// `@relaypoint/client/testing` entrypoint so consumers can test their own integrations.

import type { RequestOptions, Subscription, Transport, TransportMsg, TransportStatus } from "../transport.js";

export interface RecordedRequest {
  readonly subject: string;
  readonly data: Uint8Array;
}

export interface RecordedPublish {
  readonly subject: string;
  readonly data: Uint8Array;
}

// A responder decides the reply to a `.cmd` request. Throw to simulate a timeout / no-responder
// (the SDK then retries with the same command_id).
export type Responder = (subject: string, data: Uint8Array) => Uint8Array | Promise<Uint8Array>;

export class FakeTransport implements Transport {
  readonly requests: RecordedRequest[] = [];
  readonly publishes: RecordedPublish[] = [];
  connectTokens: string[] = [];

  private responder: Responder = () => {
    throw new Error("no responder configured");
  };
  private readonly subs = new Map<string, Set<(msg: TransportMsg) => void>>();
  private readonly durable = new Map<string, Array<{ sequence: number; data: Uint8Array }>>();
  private readonly statusCbs = new Set<(s: TransportStatus) => void>();
  private replayFailures = 0;

  async connect(token: string): Promise<void> {
    this.connectTokens.push(token);
  }

  async close(): Promise<void> {}

  publish(subject: string, data: Uint8Array): void {
    this.publishes.push({ subject, data });
  }

  async request(subject: string, data: Uint8Array, _opts: RequestOptions): Promise<TransportMsg> {
    this.requests.push({ subject, data });
    const reply = await this.responder(subject, data);
    return { data: reply };
  }

  subscribe(subject: string, cb: (msg: TransportMsg) => void): Subscription {
    let set = this.subs.get(subject);
    if (!set) {
      set = new Set();
      this.subs.set(subject, set);
    }
    set.add(cb);
    return { unsubscribe: () => set!.delete(cb) };
  }

  async *replay(subject: string, fromSequence: number): AsyncIterable<TransportMsg> {
    if (this.replayFailures > 0) {
      this.replayFailures--;
      throw new Error("durable store unreachable");
    }
    const log = this.durable.get(subject) ?? [];
    for (const e of log) {
      if (e.sequence >= fromSequence) yield { data: e.data };
    }
  }

  onStatus(cb: (s: TransportStatus) => void): Subscription {
    this.statusCbs.add(cb);
    return { unsubscribe: () => this.statusCbs.delete(cb) };
  }

  // --- test controls ---------------------------------------------------------

  setResponder(fn: Responder): void {
    this.responder = fn;
  }

  // Push a message to live subscribers of `subject` (as the server would).
  deliverLive(subject: string, data: Uint8Array): void {
    for (const cb of this.subs.get(subject) ?? []) cb({ data });
  }

  // Record a durable fact for replay (and optionally deliver it live too).
  appendDurable(subject: string, sequence: number, data: Uint8Array, live = false): void {
    let log = this.durable.get(subject);
    if (!log) {
      log = [];
      this.durable.set(subject, log);
    }
    log.push({ sequence, data });
    log.sort((a, b) => a.sequence - b.sequence);
    if (live) this.deliverLive(subject, data);
  }

  // Make the next `count` replay() calls throw (durable store unreachable).
  failReplay(count: number): void {
    this.replayFailures = count;
  }

  emitStatus(status: TransportStatus): void {
    for (const cb of [...this.statusCbs]) cb(status);
  }
}
