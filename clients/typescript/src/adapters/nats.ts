// The only file that imports nats.ws (the Transport adapter). nats.ws auto-reconnect is disabled
// so the core can refresh the token per connection. The raw publish(subject) is generic plumbing;
// "clients never write .log" is enforced by the server NATS ACL (deploy/nats/nats-server.conf),
// not by this adapter.

import {
  connect,
  consumerOpts,
  type JetStreamClient,
  type NatsConnection,
} from "nats.ws";
import type { RequestOptions, Subscription, Transport, TransportMsg, TransportStatus } from "../transport.js";

export class NatsWsTransport implements Transport {
  private nc?: NatsConnection;
  private js?: JetStreamClient;
  private readonly statusCbs = new Set<(s: TransportStatus) => void>();
  private closed = false;
  private resolveClosed!: () => void;
  private readonly closedSignal: Promise<void>;

  // streamName is bound on replay so JetStream needs no stream-discovery API call (which the
  // client ACL forbids — it only grants $JS.API.CONSUMER.>). replayIdleMs ends a replay that has
  // gone quiet (an empty/caught-up stream) so it never hangs.
  constructor(
    private readonly servers: string[],
    private readonly streamName = "INTERACTION_LOGS",
    private readonly replayIdleMs = 1000,
  ) {
    this.closedSignal = new Promise((r) => (this.resolveClosed = r));
  }

  async connect(token: string): Promise<void> {
    if (this.nc && !this.nc.isClosed()) await this.nc.close();
    const nc = await connect({ servers: this.servers, token, reconnect: false });
    this.nc = nc;
    this.js = nc.jetstream();
    this.watchStatus(nc);
  }

  async close(): Promise<void> {
    this.closed = true;
    this.resolveClosed(); // wake any suspended replay so it unsubscribes (no leaked consumer)
    await this.nc?.close();
  }

  publish(subject: string, data: Uint8Array): void {
    this.conn().publish(subject, data);
  }

  async request(subject: string, data: Uint8Array, opts: RequestOptions): Promise<TransportMsg> {
    const m = await this.conn().request(subject, data, { timeout: opts.timeoutMs });
    return toMsg(m.data, m.headers);
  }

  subscribe(subject: string, cb: (msg: TransportMsg) => void): Subscription {
    const sub = this.conn().subscribe(subject, {
      callback: (_err, m) => cb(toMsg(m.data, m.headers)),
    });
    return { unsubscribe: () => sub.unsubscribe() };
  }

  // Replays the whole (small) per-interaction log; the delivery plane drops already-applied
  // facts. `fromSequence` is unused. Throws if JetStream is unreachable (delivery fails closed).
  // Terminates on the stream head (pending == 0), on inactivity (an empty/caught-up stream — no
  // hang), or on close (the consumer is unsubscribed, never leaked).
  async *replay(subject: string, _fromSequence: number): AsyncIterable<TransportMsg> {
    const opts = consumerOpts();
    opts.bindStream(this.streamName); // bound → no stream-discovery (works under the client ACL)
    opts.orderedConsumer();
    opts.deliverAll();
    const sub = await this.jsClient().subscribe(subject, opts);
    const it = sub[Symbol.asyncIterator]();
    try {
      for (;;) {
        if (this.closed) return;
        const step = await Promise.race([
          it.next().then((r) => ({ kind: "msg" as const, r })),
          this.delay(this.replayIdleMs).then(() => ({ kind: "idle" as const })),
          this.closedSignal.then(() => ({ kind: "closed" as const })),
        ]);
        if (step.kind !== "msg") return; // idle (caught up / empty) or closed → stop
        if (step.r.done) return;
        const m = step.r.value;
        yield toMsg(m.data, m.headers ?? undefined);
        if (m.info.pending === 0) return; // caught up to the stream head
      }
    } finally {
      sub.unsubscribe();
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }

  onStatus(cb: (s: TransportStatus) => void): Subscription {
    this.statusCbs.add(cb);
    return { unsubscribe: () => this.statusCbs.delete(cb) };
  }

  private watchStatus(nc: NatsConnection): void {
    // A superseded connection (after a reconnect created a new nc) must not emit stale status —
    // e.g. the old nc's closed() firing a final "disconnected" over a healthy new connection.
    const current = () => this.nc === nc;
    void (async () => {
      for await (const s of nc.status()) {
        if (!current()) return;
        if (s.type === "disconnect") this.emit({ type: "disconnected", final: false, reason: String(s.data) });
        else if (s.type === "reconnecting") this.emit({ type: "reconnecting" });
        else if (s.type === "reconnect") this.emit({ type: "connected" });
      }
    })();
    void nc.closed().then(() => {
      if (current()) this.emit({ type: "disconnected", final: true });
    });
  }

  private emit(s: TransportStatus): void {
    for (const cb of [...this.statusCbs]) cb(s);
  }

  private conn(): NatsConnection {
    if (!this.nc) throw new Error("transport not connected");
    return this.nc;
  }

  private jsClient(): JetStreamClient {
    if (!this.js) throw new Error("transport not connected");
    return this.js;
  }
}

function toMsg(data: Uint8Array, h: { keys(): string[]; get(k: string): string } | undefined): TransportMsg {
  if (!h) return { data };
  const headers: Record<string, string> = {};
  for (const k of h.keys()) headers[k] = h.get(k);
  return { data, headers };
}
