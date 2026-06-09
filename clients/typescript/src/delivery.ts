// Orders `.log` facts by router `sequence`. The invariants that aren't obvious from the code:
// on an unfillable gap it fails closed (never drops facts past the gap, never resumes live over
// it, never loops forever — terminal after the backoff schedule), and `occurredAt` is never
// used for ordering/dedup.

import { DeliveryFailedError } from "./errors.js";
import { Mailbox } from "./mailbox.js";
import type { DeliveryState, LogEvent } from "./types.js";

export interface DeliveryDeps {
  replay(fromSequence: number): AsyncIterable<LogEvent>;
  onState(state: DeliveryState): void;
  // ordered tap for derived state (metadata), independent of the single consumer stream
  onApplied?: (ev: LogEvent) => void;
  backoffMs?: number[]; // length bounds replay retries before terminal fail
  wait?: (ms: number) => Promise<void>;
}

const DEFAULT_BACKOFF = [100, 250, 500, 1000, 2000];

export class Delivery {
  private readonly out = new Mailbox<LogEvent>();
  private readonly pending = new Map<number, LogEvent>();
  private readonly backoff: number[];
  private readonly wait: (ms: number) => Promise<void>;
  private applied = 0;
  private state: DeliveryState = "live";
  private recovering = false;
  private closed = false;

  constructor(private readonly deps: DeliveryDeps) {
    this.backoff = deps.backoffMs ?? DEFAULT_BACKOFF;
    this.wait = deps.wait ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  events(): AsyncIterable<LogEvent> {
    return this.out;
  }

  offer(ev: LogEvent): void {
    if (this.closed || this.state === "failed") return;
    if (ev.sequence <= this.applied) return; // duplicate
    this.pending.set(ev.sequence, ev);
    this.drainContiguous();
    if (this.pending.size > 0 && !this.recovering) void this.recover();
  }

  // Load existing/missed history once on open. Without it, a handle opened on an interaction
  // that already has facts but receives no NEW live event would deliver nothing.
  prime(): void {
    if (!this.recovering && !this.closed && this.state !== "failed") void this.recover(true);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.out.close();
  }

  private drainContiguous(): void {
    for (let next = this.pending.get(this.applied + 1); next; next = this.pending.get(this.applied + 1)) {
      this.pending.delete(next.sequence);
      this.applied = next.sequence;
      this.deps.onApplied?.(next);
      this.out.push(next);
    }
  }

  private setState(s: DeliveryState): void {
    if (s === this.state) return;
    this.state = s;
    this.deps.onState(s);
  }

  // `initial` forces one replay pass even with no pending gap (load history on open) and keeps
  // retrying that pass until it succeeds; a gap (pending > 0) drives the same loop thereafter.
  private async recover(initial = false): Promise<void> {
    this.recovering = true;
    this.setState("replaying");
    let needPass = initial;
    for (let attempt = 0; (needPass || this.pending.size > 0) && !this.closed; attempt++) {
      const from = this.applied + 1;
      try {
        for await (const ev of this.deps.replay(from)) {
          if (this.closed) break;
          if (ev.sequence <= this.applied) continue;
          this.pending.set(ev.sequence, ev);
          this.drainContiguous();
        }
        needPass = false; // a successful pass clears the initial obligation
        if (this.pending.size === 0) break;
        // replay returned but a gap is still open — a transient miss, so retry
      } catch (err) {
        this.setState("degraded");
        if (attempt + 1 >= this.backoff.length) {
          this.recovering = false;
          this.setState("failed");
          this.out.error(new DeliveryFailedError(from, err));
          return;
        }
        await this.wait(this.backoff[attempt] ?? 0);
        continue;
      }
      if (attempt + 1 >= this.backoff.length) {
        this.recovering = false;
        this.setState("failed");
        this.out.error(new DeliveryFailedError(this.applied + 1));
        return;
      }
      this.setState("degraded");
      await this.wait(this.backoff[attempt] ?? 0);
    }
    this.recovering = false;
    if (!this.closed && this.state !== "failed") {
      this.drainContiguous();
      if (this.pending.size > 0) void this.recover();
      else this.setState("live");
    }
  }
}
