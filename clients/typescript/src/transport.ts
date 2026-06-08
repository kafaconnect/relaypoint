// The Transport PORT — the SDK core's ONLY infrastructure dependency. The core
// (RelayPointClient, InteractionHandle, delivery) depends solely on this abstraction; the
// nats.ws implementation is a swappable adapter (src/adapters/nats.ts) and tests run against
// an in-memory fake (src/testing/fake-transport.ts). No core file imports nats.ws — that is
// the loose-coupling HARD RULE (see AGENTS.md). A different backbone needs only a new adapter.

export interface TransportMsg {
  readonly data: Uint8Array;
  readonly headers?: Readonly<Record<string, string>>;
  // present on a message received over a request subscription — used to reply on its inbox
  respond?(data: Uint8Array): void;
}

export interface Subscription {
  unsubscribe(): void;
}

// Connection-level lifecycle the client reacts to. A "disconnected" with `final: false` (a
// transient drop or server-enforced max-lifetime / token expiry) drives a token-refresh
// reconnect; the client owns that loop so token logic stays in the core, not the transport.
export type TransportStatus =
  | { readonly type: "disconnected"; readonly final: boolean; readonly reason?: string }
  | { readonly type: "reconnecting" }
  | { readonly type: "connected" };

export interface RequestOptions {
  readonly timeoutMs: number;
}

export interface Transport {
  // Open a connection authorized by `token`. May be called again (after a drop) to reconnect
  // with a freshly minted token.
  connect(token: string): Promise<void>;
  close(): Promise<void>;
  publish(subject: string, data: Uint8Array): void;
  // Request/reply over an ephemeral inbox. Rejects on no-responder / timeout so the caller can
  // retry with the SAME command_id (idempotent).
  request(subject: string, data: Uint8Array, opts: RequestOptions): Promise<TransportMsg>;
  subscribe(subject: string, cb: (msg: TransportMsg) => void): Subscription;
  // Ordered replay of a durable subject from `fromSequence` (inclusive, app-level sequence).
  // MUST throw if the durable store cannot be reached, so delivery fails closed (no partial
  // resume over a gap).
  replay(subject: string, fromSequence: number): AsyncIterable<TransportMsg>;
  onStatus(cb: (s: TransportStatus) => void): Subscription;
}
