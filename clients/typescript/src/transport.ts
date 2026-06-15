// The port the SDK core depends on; nats.ws is an adapter (loose-coupling HARD RULE, AGENTS.md).

export interface TransportMsg {
  readonly data: Uint8Array;
  readonly subject?: string;
  readonly headers?: Readonly<Record<string, string>>;
  respond?(data: Uint8Array): void;
}

export interface Subscription {
  unsubscribe(): void;
}

// `disconnected` with `final: false` drives a token-refresh reconnect (the client owns that
// loop, so token logic stays in the core rather than the transport).
export type TransportStatus =
  | { readonly type: "disconnected"; readonly final: boolean; readonly reason?: string }
  | { readonly type: "reconnecting" }
  | { readonly type: "connected" };

export interface RequestOptions {
  readonly timeoutMs: number;
}

export interface Transport {
  connect(token: string): Promise<void>;
  close(): Promise<void>;
  publish(subject: string, data: Uint8Array): void;
  request(subject: string, data: Uint8Array, opts: RequestOptions): Promise<TransportMsg>;
  subscribe(subject: string, cb: (msg: TransportMsg) => void): Subscription;
  // MUST throw (not return partial) if the store is unreachable, so delivery fails closed.
  replay(subject: string, fromSequence: number): AsyncIterable<TransportMsg>;
  onStatus(cb: (s: TransportStatus) => void): Subscription;
}
