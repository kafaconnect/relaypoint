// Live-NATS integration test for the nats.ws replay path (relaypoint#12). Skipped unless
// RELAYPOINT_NATS_WS is set (e.g. ws://127.0.0.1:18088). Bring NATS up with the deploy config:
//   docker run -d --name rp-nats -e ROUTER_PASSWORD=router-dev -e CLIENT_PASSWORD=client-dev \
//     -v "$PWD/deploy/nats/nats-server.conf:/etc/nats/nats-server.conf:ro" \
//     -p 14222:4222 -p 18088:8088 nats:2.10-alpine -c /etc/nats/nats-server.conf
//   RELAYPOINT_NATS_WS=ws://127.0.0.1:18088 pnpm test
//
// Validates what the fake-transport unit suite cannot: replay runs under the real CLIENT ACL
// (bindStream avoids the forbidden stream-discovery), an empty stream returns at once (no hang),
// and close() aborts without leaking the consumer.

import { connect, StringCodec, type NatsConnection } from "nats.ws";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { NatsWsTransport } from "../src/adapters/nats.js";
import { logSubject } from "../src/subjects.js";

declare const process: { env: Record<string, string | undefined> };
const WS = process.env.RELAYPOINT_NATS_WS;
const sc = StringCodec();
const dec = new TextDecoder();

async function seed(router: NatsConnection, interactionId: string, count: number): Promise<void> {
  const js = router.jetstream();
  const jsm = await router.jetstreamManager();
  try {
    await jsm.streams.add({ name: "INTERACTION_LOGS", subjects: ["tenant.*.interaction.*.log"] });
  } catch {
    /* already exists */
  }
  for (let n = 1; n <= count; n++) {
    await js.publish(logSubject("t1", interactionId), sc.encode(JSON.stringify({ sequence: n, event_id: `e${n}` })), {
      msgID: `t1.${interactionId}.c${n}`,
    });
  }
}

async function collect(it: AsyncIterable<{ data: Uint8Array }>): Promise<number[]> {
  const out: number[] = [];
  for await (const m of it) out.push((JSON.parse(dec.decode(m.data)) as { sequence: number }).sequence);
  return out;
}

describe.skipIf(!WS)("nats.ws replay against live NATS", () => {
  let router: NatsConnection;
  const id = `it${Date.now()}`;

  beforeAll(async () => {
    router = await connect({ servers: [WS!], user: "router", pass: "router-dev", reconnect: false });
    await seed(router, id, 5);
  });
  afterAll(async () => {
    await router?.drain();
  });

  function clientTransport(): NatsWsTransport {
    return new NatsWsTransport([WS!], "INTERACTION_LOGS", { user: "client", pass: "client-dev" });
  }

  it("replays the durable log in order under the client ACL", async () => {
    const t = clientTransport();
    await t.connect("");
    const got = await collect(t.replay(logSubject("t1", id), 1));
    await t.close();
    expect(got).toEqual([1, 2, 3, 4, 5]);
  });

  it("returns immediately on an empty interaction (no hang)", async () => {
    const t = clientTransport();
    await t.connect("");
    const started = Date.now();
    const got = await collect(t.replay(logSubject("t1", `${id}empty`), 1));
    const elapsed = Date.now() - started;
    await t.close();
    expect(got).toEqual([]);
    expect(elapsed).toBeLessThan(3000); // terminates by num_pending==0, not a long idle timer
  });

  it("close() aborts a replay without hanging", async () => {
    const t = clientTransport();
    await t.connect("");
    const iter = t.replay(logSubject("t1", `${id}empty2`), 1)[Symbol.asyncIterator]();
    await t.close();
    await expect(iter.next()).resolves.toMatchObject({ done: true });
  });
});
