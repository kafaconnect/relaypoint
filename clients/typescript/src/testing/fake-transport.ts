// In-memory Transport for unit tests (no live NATS); also published under `/testing`.

import type { RequestOptions, Subscription, Transport, TransportMsg, TransportStatus } from "../transport.js";

export interface RecordedRequest {
  readonly subject: string;
  readonly data: Uint8Array;
}

export interface RecordedPublish {
  readonly subject: string;
  readonly data: Uint8Array;
}

// Throw to simulate a timeout / no-responder.
export type Responder = (subject: string, data: Uint8Array) => Uint8Array | Promise<Uint8Array>;

export class FakeTransport implements Transport {
  readonly requests: RecordedRequest[] = [];
  readonly publishes: RecordedPublish[] = [];
  connectTokens: string[] = [];
  closeCount = 0;

  private responder: Responder = () => {
    throw new Error("no responder configured");
  };
  private readonly subs = new Map<string, Set<(msg: TransportMsg) => void>>();
  private readonly durable = new Map<string, Array<{ sequence: number; data: Uint8Array }>>();
  private readonly statusCbs = new Set<(s: TransportStatus) => void>();
  private replayFailures = 0;
  private connectFailures = 0;
  private connectGate: Promise<void> | undefined;

  async connect(token: string): Promise<void> {
    if (this.connectFailures > 0) {
      this.connectFailures--;
      throw new Error("transport connect failed");
    }
    if (this.connectGate) await this.connectGate;
    if (this.connectTokens.length > 0) this.subs.clear();
    this.connectTokens.push(token);
  }

  async close(): Promise<void> {
    this.closeCount++;
  }

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
      if (e.sequence >= fromSequence) yield { data: e.data, subject };
    }
  }

  onStatus(cb: (s: TransportStatus) => void): Subscription {
    this.statusCbs.add(cb);
    return { unsubscribe: () => this.statusCbs.delete(cb) };
  }

  setResponder(fn: Responder): void {
    this.responder = fn;
  }

  deliverLive(subject: string, data: Uint8Array): void {
    for (const [pattern, cbs] of this.subs) {
      if (!subjectMatches(pattern, subject)) continue;
      for (const cb of cbs) cb({ data, subject });
    }
  }

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

  failReplay(count: number): void {
    this.replayFailures = count;
  }

  failConnect(count: number): void {
    this.connectFailures = count;
  }

  // Block subsequent connect() calls until the returned function is called.
  gateConnect(): () => void {
    let release!: () => void;
    this.connectGate = new Promise((r) => (release = r));
    return () => {
      this.connectGate = undefined;
      release();
    };
  }

  emitStatus(status: TransportStatus): void {
    for (const cb of [...this.statusCbs]) cb(status);
  }
}

function subjectMatches(pattern: string, subject: string): boolean {
  if (pattern === subject) return true;
  const pp = pattern.split(".");
  const sp = subject.split(".");
  for (let i = 0; i < pp.length; i++) {
    if (pp[i] === ">") return i < sp.length;
    if (sp[i] === undefined) return false;
    if (pp[i] !== "*" && pp[i] !== sp[i]) return false;
  }
  return pp.length === sp.length;
}
