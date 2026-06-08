import { describe, expect, it } from "vitest";
import { Delivery } from "../src/delivery.js";
import { DeliveryFailedError } from "../src/errors.js";
import type { DeliveryState, LogEvent } from "../src/types.js";
import { immediate, logEvent, take } from "./helpers.js";

// A replay backed by an in-memory durable log; can be made to fail the first N calls.
function makeDeps(durable: LogEvent[], opts: { failTimes?: number; states?: DeliveryState[] } = {}) {
  let fails = opts.failTimes ?? 0;
  const states = opts.states ?? [];
  return {
    replay: async function* (from: number) {
      if (fails > 0) {
        fails--;
        throw new Error("durable store unreachable");
      }
      for (const e of durable) if (e.sequence >= from) yield e;
    },
    onState: (s: DeliveryState) => states.push(s),
    backoffMs: [0, 0, 0],
    wait: immediate,
  };
}

describe("ordered log delivery", () => {
  // @spec:clientsdk.delivery.ordered-by-sequence
  it("delivers facts in ascending router-sequence order", async () => {
    const d = new Delivery(makeDeps([]));
    d.offer(logEvent(1));
    d.offer(logEvent(2));
    d.offer(logEvent(3));
    const got = await take(d.events(), 3);
    expect(got.map((e) => e.sequence)).toEqual([1, 2, 3]);
  });

  // @spec:clientsdk.delivery.dedup-event-id
  it("drops a re-delivered fact (same event_id / sequence) — applied once", async () => {
    const d = new Delivery(makeDeps([]));
    d.offer(logEvent(1, { eventId: "ev-A" }));
    d.offer(logEvent(1, { eventId: "ev-A" })); // duplicate redelivery
    d.offer(logEvent(2, { eventId: "ev-B" }));
    const got = await take(d.events(), 2);
    expect(got.map((e) => e.eventId)).toEqual(["ev-A", "ev-B"]);
  });

  // @spec:clientsdk.time.occurred-at-display-only
  it("orders by sequence even when occurredAt is reversed", async () => {
    const d = new Delivery(makeDeps([]));
    d.offer(logEvent(1, { occurredAt: "2026-06-08T03:00:00Z" }));
    d.offer(logEvent(2, { occurredAt: "2026-06-08T02:00:00Z" }));
    d.offer(logEvent(3, { occurredAt: "2026-06-08T01:00:00Z" }));
    const got = await take(d.events(), 3);
    expect(got.map((e) => e.sequence)).toEqual([1, 2, 3]);
  });

  // @spec:clientsdk.delivery.gap-replay
  it("pauses on a sequence gap, replays to fill it, then resumes", async () => {
    const durable = [logEvent(3), logEvent(4)];
    const states: DeliveryState[] = [];
    const d = new Delivery(makeDeps(durable, { states }));
    d.offer(logEvent(1));
    d.offer(logEvent(2));
    d.offer(logEvent(4)); // gap: 3 missing -> replay
    const got = await take(d.events(), 4);
    expect(got.map((e) => e.sequence)).toEqual([1, 2, 3, 4]);
    expect(states).toContain("replaying");
    expect(states.at(-1)).toBe("live");
  });

  // @spec:clientsdk.delivery.replay-failure
  it("surfaces a degraded then failed delivery state when replay cannot fill the gap", async () => {
    const states: DeliveryState[] = [];
    const d = new Delivery(makeDeps([], { failTimes: 99, states })); // replay always throws
    const iter = d.events()[Symbol.asyncIterator]();
    const first = iter.next(); // seq 1 will be delivered
    d.offer(logEvent(1));
    expect((await first).value).toMatchObject({ sequence: 1 });
    const next = iter.next(); // waits; the gap can never fill
    d.offer(logEvent(3)); // gap at 2 -> replay fails repeatedly
    await expect(next).rejects.toBeInstanceOf(DeliveryFailedError);
    expect(states).toContain("degraded");
    expect(states.at(-1)).toBe("failed");
  });
});
