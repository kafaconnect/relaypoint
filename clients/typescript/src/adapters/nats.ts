// The nats.ws adapter — the ONLY file in the SDK that imports nats.ws. It implements the
// Transport port so the core stays decoupled from the backbone (loose-coupling HARD RULE).
// Auto-reconnect is DISABLED here on purpose: the SDK core owns reconnection so it can refresh
// the scoped token via getToken() on each reconnect (a connection is authorized at CONNECT
// time by the auth-callout). A transient drop surfaces as a non-final "disconnected" status;
// the client then re-invokes connect() with a fresh token.
//
// Trust model: this is a GENERIC transport with a raw publish(subject) — that genericity is the
// price of the loose-coupling port (the core must not know NATS). The "clients never write .log"
// guarantee does NOT rest on this adapter: it is enforced server-side by the NATS account ACL
// (deploy/nats/nats-server.conf denies clients publish on interaction.*.log) and by the SDK's
// own command API (InteractionHandle), which exposes no log-write path. The raw adapter is
// plumbing, not the app-facing surface.

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

  constructor(private readonly servers: string[]) {}

  async connect(token: string): Promise<void> {
    if (this.nc && !this.nc.isClosed()) await this.nc.close();
    const nc = await connect({ servers: this.servers, token, reconnect: false });
    this.nc = nc;
    this.js = nc.jetstream();
    this.watchStatus(nc);
  }

  async close(): Promise<void> {
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

  // Ordered JetStream replay. `fromSequence` is an app-level hint; this adapter replays the
  // durable subject from the start and lets the delivery plane drop already-applied facts
  // (the per-interaction log is small). It throws if JetStream cannot be reached, so the
  // delivery plane fails closed over a gap rather than resuming silently.
  async *replay(subject: string, _fromSequence: number): AsyncIterable<TransportMsg> {
    const opts = consumerOpts();
    opts.orderedConsumer();
    opts.deliverAll();
    const sub = await this.jsClient().subscribe(subject, opts);
    try {
      for await (const m of sub) {
        yield toMsg(m.data, m.headers ?? undefined);
        if (m.info.pending === 0) break; // caught up to the stream head
      }
    } finally {
      sub.unsubscribe();
    }
  }

  onStatus(cb: (s: TransportStatus) => void): Subscription {
    this.statusCbs.add(cb);
    return { unsubscribe: () => this.statusCbs.delete(cb) };
  }

  private watchStatus(nc: NatsConnection): void {
    void (async () => {
      for await (const s of nc.status()) {
        if (s.type === "disconnect") this.emit({ type: "disconnected", final: false, reason: String(s.data) });
        else if (s.type === "reconnecting") this.emit({ type: "reconnecting" });
        else if (s.type === "reconnect") this.emit({ type: "connected" });
      }
    })();
    void nc.closed().then(() => this.emit({ type: "disconnected", final: true }));
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
