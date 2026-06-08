// Ordered `.log` delivery: facts are delivered to the consumer strictly in ascending
// router-assigned `sequence`. A fact at or below the last applied sequence is a duplicate
// (dropped). A gap (sequence > applied + 1) pauses live apply and replays from the durable
// store until contiguous, then resumes live. If replay cannot reach the store, delivery
// surfaces a degraded state and retries with bounded backoff — facts are NEVER dropped past
// the gap, the stream never resumes live over an unfilled gap, and recovery never loops
// forever (after the backoff schedule is exhausted it fails terminally).
//
// `occurredAt` is display-only and plays no part here — ordering/dedup are by `sequence` only.

import { DeliveryFailedError } from "./errors.js";
import { Mailbox } from "./mailbox.js";
import type { DeliveryState, LogEvent } from "./types.js";

export interface DeliveryDeps {
  // Ordered replay of facts from `fromSequence` (inclusive). Throws if the durable store
  // cannot be reached, so the gap stays unfilled rather than silently resuming.
  replay(fromSequence: number): AsyncIterable<LogEvent>;
  onState(state: DeliveryState): void;
  // Fired for each fact in applied (sequence) order, independent of the consumer iterator —
  // lets the handle keep ordered derived state (e.g. metadata) without stealing the stream.
  onApplied?: (ev: LogEvent) => void;
  // Backoff schedule for replay retries; its length bounds the attempts before terminal fail.
  backoffMs?: number[];
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

  // Feed one live decoded fact.
  offer(ev: LogEvent): void {
    if (this.closed || this.state === "failed") return;
    if (ev.sequence <= this.applied) return; // duplicate / already applied
    this.pending.set(ev.sequence, ev);
    this.drainContiguous();
    if (this.pending.size > 0 && !this.recovering) void this.recover();
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

  private async recover(): Promise<void> {
    this.recovering = true;
    this.setState("replaying");
    for (let attempt = 0; this.pending.size > 0 && !this.closed; attempt++) {
      const from = this.applied + 1;
      try {
        for await (const ev of this.deps.replay(from)) {
          if (this.closed) break;
          if (ev.sequence <= this.applied) continue;
          this.pending.set(ev.sequence, ev);
          this.drainContiguous();
        }
        if (this.pending.size === 0) break; // gap filled
        // replay returned but the gap is still open — treat as a transient miss and retry
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
