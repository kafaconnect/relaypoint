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
  readonly authBackoffMs?: number[]; // length bounds getToken retries before auth_failed
  readonly connectBackoffMs?: number[]; // length bounds transport.connect retries
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
const DEFAULT_CONNECT_BACKOFF = [200, 500, 1000, 2000, 4000];

export class RelayPointClient {
  private readonly emitter = new Emitter<ClientEvents>();
  private readonly transport: Transport;
  private readonly wait: (ms: number) => Promise<void>;
  private readonly authBackoff: number[];
  private readonly connectBackoff: number[];
  private readonly handles = new Map<string, InteractionHandle>();
  private _state: ConnectionState = "disconnected";
  private statusSub?: Subscription;
  private closed = false;
  private busy = false; // an establish() (connect or reconnect) is in flight — serialise them

  constructor(
    private readonly options: RelayPointClientOptions,
    deps: RelayPointClientDeps,
  ) {
    this.transport = deps.transport;
    this.wait = deps.wait ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.authBackoff = options.authBackoffMs ?? DEFAULT_AUTH_BACKOFF;
    this.connectBackoff = options.connectBackoffMs ?? DEFAULT_CONNECT_BACKOFF;
  }

  get state(): ConnectionState {
    return this._state;
  }

  async connect(): Promise<void> {
    if (this.closed) throw new Error("client is closed");
    this.setState("connecting");
    this.busy = true;
    try {
      await this.establish();
    } catch (err) {
      // never strand "connecting": auth exhaustion is already "failed", else "disconnected"
      if (!(err instanceof AuthFailedError)) this.setState("disconnected");
      throw err;
    } finally {
      this.busy = false;
    }
    if (this.closed) {
      // closed mid-connect: establish() opened a connection AFTER close() ran its transport.close()
      // (which was a no-op then) — close the late connection so it does not leak.
      void this.transport.close();
      return;
    }
    this.setState("connected");
    this.statusSub ??= this.transport.onStatus((s) => {
      if (s.type === "disconnected" && !this.closed) {
        if (s.final) this.setState("disconnected");
        else void this.reconnect();
      }
    });
  }

  interaction(id: string): InteractionHandle {
    const existing = this.handles.get(id);
    if (existing && !existing.isClosed) return existing;
    const handle = new InteractionHandle(
      () => this.transport,
      { tenantId: this.options.tenantId, selfUserId: this.options.selfUserId, interactionId: id },
      {
        requestTimeoutMs: this.options.requestTimeoutMs ?? 5000,
        sendRetries: this.options.sendRetries ?? 2,
        medium: this.options.medium ?? "chat",
      },
      (closedId) => {
        if (this.handles.get(closedId) === handle) this.handles.delete(closedId);
      },
    );
    this.handles.set(id, handle);
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
    if (this.busy || this.closed) return; // a connect/reconnect is already establishing
    this.busy = true;
    this.setState("reconnecting");
    this.emitter.emit("reconnecting");
    try {
      await this.establish();
      if (this.closed) {
        void this.transport.close(); // closed mid-reconnect — close the late connection
        return;
      }
      this.setState("connected");
      for (const h of this.handles.values()) h.resubscribe();
      this.emitter.emit("reconnected");
    } catch (err) {
      if (!(err instanceof AuthFailedError)) this.setState("disconnected");
    } finally {
      this.busy = false;
    }
  }

  // Retries a failing transport.connect() because nats.ws auto-reconnect is disabled (the SDK
  // refreshes the token per connection).
  private async establish(): Promise<void> {
    for (let attempt = 0; !this.closed; attempt++) {
      const token = await this.tokenWithRetry();
      try {
        await this.transport.connect(token);
        return;
      } catch (err) {
        if (attempt + 1 >= this.connectBackoff.length) throw err;
        await this.wait(this.connectBackoff[attempt] ?? 0);
      }
    }
  }

  private async tokenWithRetry(): Promise<string> {
    let lastErr: unknown;
    const attempts = Math.max(1, this.authBackoff.length); // try once even if backoff is empty
    for (let attempt = 0; attempt < attempts; attempt++) {
      try {
        return await this.options.getToken();
      } catch (err) {
        lastErr = err;
        if (attempt + 1 < attempts) await this.wait(this.authBackoff[attempt] ?? 0);
      }
    }
    this.setState("failed");
    const fail = new AuthFailedError(lastErr);
    this.emitter.emit("auth_failed", fail);
    throw fail;
  }
}
