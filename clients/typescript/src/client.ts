// RelayPointClient (chat subset): owns the connection lifecycle and token refresh. It connects
// via the injected Transport using a token from getToken(); on a transient drop (or the
// server-enforced max connection lifetime / token expiry) it re-fetches a token and reconnects
// transparently, then re-attaches each open interaction's subscriptions. getToken() failure is
// retried with backoff and becomes a fatal auth_failed after the schedule is exhausted — it
// never loops forever. Connection state is observable via `state` and on("state").

import { Emitter } from "./emitter.js";
import { AuthFailedError } from "./errors.js";
import { InteractionHandle } from "./interaction.js";
import type { Subscription, Transport } from "./transport.js";
import type { ConnectionState } from "./types.js";

export interface RelayPointClientOptions {
  readonly servers: string[];
  readonly selfUserId: string;
  readonly tenantId: string;
  readonly getToken: () => Promise<string>;
  readonly requestTimeoutMs?: number;
  readonly sendRetries?: number;
  readonly medium?: string;
  // backoff schedule for getToken() retries; its length bounds attempts before auth_failed
  readonly authBackoffMs?: number[];
}

export interface RelayPointClientDeps {
  readonly transport: Transport;
  readonly wait?: (ms: number) => Promise<void>;
}

interface ClientEvents extends Record<string, (...args: never[]) => void> {
  state: (s: ConnectionState) => void;
  reconnecting: () => void;
  reconnected: () => void;
  disconnected: () => void;
  auth_failed: (err: AuthFailedError) => void;
}

const DEFAULT_AUTH_BACKOFF = [200, 500, 1000, 2000, 4000];

export class RelayPointClient {
  private readonly emitter = new Emitter<ClientEvents>();
  private readonly transport: Transport;
  private readonly wait: (ms: number) => Promise<void>;
  private readonly authBackoff: number[];
  private readonly handles = new Map<string, InteractionHandle>();
  private _state: ConnectionState = "disconnected";
  private statusSub?: Subscription;
  private closed = false;
  private reconnecting = false;

  constructor(
    private readonly options: RelayPointClientOptions,
    deps: RelayPointClientDeps,
  ) {
    this.transport = deps.transport;
    this.wait = deps.wait ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.authBackoff = options.authBackoffMs ?? DEFAULT_AUTH_BACKOFF;
  }

  get state(): ConnectionState {
    return this._state;
  }

  async connect(): Promise<void> {
    if (this.closed) throw new Error("client is closed");
    this.setState("connecting");
    const token = await this.tokenWithRetry();
    await this.transport.connect(token);
    this.setState("connected");
    this.statusSub ??= this.transport.onStatus((s) => {
      if (s.type === "disconnected" && !this.closed) {
        if (s.final) this.setState("disconnected");
        else void this.reconnect();
      }
    });
  }

  interaction(id: string): InteractionHandle {
    let handle = this.handles.get(id);
    if (!handle) {
      handle = new InteractionHandle(
        () => this.transport,
        { tenantId: this.options.tenantId, selfUserId: this.options.selfUserId, interactionId: id },
        {
          requestTimeoutMs: this.options.requestTimeoutMs ?? 5000,
          sendRetries: this.options.sendRetries ?? 2,
          medium: this.options.medium ?? "chat",
        },
      );
      this.handles.set(id, handle);
    }
    return handle;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.statusSub?.unsubscribe();
    for (const h of this.handles.values()) h.close();
    await this.transport.close();
    this.setState("closed");
  }

  on<E extends keyof ClientEvents>(event: E, cb: ClientEvents[E]): () => void {
    return this.emitter.on(event, cb);
  }

  private setState(s: ConnectionState): void {
    if (s === this._state) return;
    this._state = s;
    this.emitter.emit("state", s);
  }

  private async reconnect(): Promise<void> {
    if (this.reconnecting || this.closed) return;
    this.reconnecting = true;
    this.setState("reconnecting");
    this.emitter.emit("reconnecting");
    try {
      const token = await this.tokenWithRetry();
      await this.transport.connect(token);
      this.setState("connected");
      for (const h of this.handles.values()) h.resubscribe();
      this.emitter.emit("reconnected");
    } catch (err) {
      if (!(err instanceof AuthFailedError)) this.setState("disconnected");
    } finally {
      this.reconnecting = false;
    }
  }

  // Resolve a token, retrying getToken() failures with backoff. Exhaustion is fatal: emit
  // auth_failed, transition to "failed", and throw — the SDK never silently loops forever.
  private async tokenWithRetry(): Promise<string> {
    let lastErr: unknown;
    for (let attempt = 0; attempt < this.authBackoff.length; attempt++) {
      try {
        return await this.options.getToken();
      } catch (err) {
        lastErr = err;
        if (attempt + 1 < this.authBackoff.length) await this.wait(this.authBackoff[attempt] ?? 0);
      }
    }
    this.setState("failed");
    const fail = new AuthFailedError(lastErr);
    this.emitter.emit("auth_failed", fail);
    throw fail;
  }
}
