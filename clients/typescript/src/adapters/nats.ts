// The only file that imports nats.ws. Auto-reconnect is disabled so the core refreshes the token
// per connection. "Clients never write .log" is enforced by the server NATS ACL, not this adapter.

import {
  connect,
  consumerOpts,
  type ConnectionOptions,
  type JetStreamClient,
  type JsMsg,
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

  // streamName is bound on replay so JetStream needs no stream-discovery API call (the client ACL
  // grants only $JS.API.CONSUMER.>). connectOptions carries Phase-1 user/pass auth (the
  // auth-callout token model isn't live yet); token-based auth still works.
  constructor(
    private readonly servers: string[],
    private readonly streamName = "INTERACTION_LOGS",
    private readonly connectOptions: Partial<ConnectionOptions> = {},
    private readonly emptyProbeMs = 300,
  ) {
    this.closedSignal = new Promise((r) => (this.resolveClosed = r));
  }

  private delay(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }

  async connect(token: string): Promise<void> {
    if (this.nc && !this.nc.isClosed()) await this.nc.close();
    const nc = await connect({
      servers: this.servers,
      reconnect: false,
      ...this.connectOptions,
      ...(token ? { token } : {}),
    });
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
    return toMsg(m.data, m.headers, m.subject);
  }

  subscribe(subject: string, cb: (msg: TransportMsg) => void): Subscription {
    const sub = this.conn().subscribe(subject, {
      callback: (_err, m) => cb(toMsg(m.data, m.headers, m.subject)),
    });
    return { unsubscribe: () => sub.unsubscribe() };
  }

  // `fromSequence` is unused (the whole small log is replayed; delivery drops applied facts).
  // An ordered push consumer (ackNone — the client ACL grants no ack subject) delivers every fact
  // immediately; the last carries pending==0 which ends the replay. The only ambiguous case is an
  // EMPTY interaction (no fact ever arrives): a one-shot consumerInfo probe distinguishes truly
  // empty (num_pending 0 AND nothing delivered) from facts-still-coming, so a slow network is
  // waited out rather than mistaken for empty. Throws if JetStream is unreachable (fail closed);
  // close aborts the wait so the consumer is never leaked.
  async *replay(subject: string, _fromSequence: number): AsyncIterable<TransportMsg> {
    if (this.closed) return;
    const opts = consumerOpts();
    opts.bindStream(this.streamName); // bound → no stream-discovery (works under the client ACL)
    opts.orderedConsumer();
    opts.deliverAll();
    const sub = await this.jsClient().subscribe(subject, opts);
    const it = sub[Symbol.asyncIterator]();
    let nextMsg = it.next();
    let probed = false;
    type Step = { kind: "msg"; r: IteratorResult<JsMsg> } | { kind: "closed" } | { kind: "probe" };
    try {
      for (;;) {
        if (this.closed) return;
        const racers: Array<Promise<Step>> = [
          nextMsg.then((r): Step => ({ kind: "msg", r })),
          this.closedSignal.then((): Step => ({ kind: "closed" })),
        ];
        if (!probed) racers.push(this.delay(this.emptyProbeMs).then((): Step => ({ kind: "probe" })));
        const step = await Promise.race(racers);
        if (step.kind === "closed") return;
        if (step.kind === "probe") {
          probed = true; // probe once; afterwards just wait — the facts are coming
          const ci = await sub.consumerInfo();
          if (ci.num_pending === 0 && ci.delivered.consumer_seq === 0) return; // truly empty
          continue; // facts pending → keep awaiting nextMsg with no timeout
        }
        probed = true;
        if (step.r.done) return;
        const m = step.r.value;
        yield toMsg(m.data, m.headers ?? undefined, m.subject);
        if (m.info.pending === 0) return; // caught up to the stream head
        nextMsg = it.next();
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

function toMsg(data: Uint8Array, h: { keys(): string[]; get(k: string): string } | undefined, subject?: string): TransportMsg {
  const base = subject === undefined ? { data } : { data, subject };
  if (!h) return base;
  const headers: Record<string, string> = {};
  for (const k of h.keys()) headers[k] = h.get(k);
  return { ...base, headers };
}
