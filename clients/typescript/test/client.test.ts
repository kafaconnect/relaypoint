import { describe, expect, it, vi } from "vitest";
import { RelayPointClient, type RelayPointClientOptions } from "../src/client.js";
import { AuthFailedError } from "../src/errors.js";
import { logSubject } from "../src/subjects.js";
import { FakeTransport } from "../src/testing/fake-transport.js";
import type { ConnectionState } from "../src/types.js";
import { immediate, take, wireEvent } from "./helpers.js";

function newClient(over: Partial<RelayPointClientOptions> = {}) {
  const transport = new FakeTransport();
  const options: RelayPointClientOptions = {
    servers: ["ws://x"],
    selfUserId: "alice",
    tenantId: "t1",
    getToken: () => Promise.resolve("tok"),
    authBackoffMs: [0, 0],
    ...over,
  };
  const client = new RelayPointClient(options, { transport, wait: immediate });
  return { client, transport };
}

describe("connection lifecycle", () => {
  // @spec:clientsdk.connection.connect-with-token
  it("connects with a token from getToken()", async () => {
    const getToken = vi.fn(() => Promise.resolve("tok-1"));
    const { client, transport } = newClient({ getToken });
    await client.connect();
    expect(getToken).toHaveBeenCalledOnce();
    expect(transport.connectTokens).toEqual(["tok-1"]);
    expect(client.state).toBe("connected");
  });

  // @spec:clientsdk.connection.token-refresh-reconnect
  it("refreshes the token and reconnects transparently on a transient drop", async () => {
    let n = 0;
    const getToken = vi.fn(() => Promise.resolve(`tok-${++n}`));
    const { client, transport } = newClient({ getToken });
    await client.connect();
    // open an interaction so reconnect must re-attach its live subscription
    const handle = client.interaction("im-1");
    void handle.events();

    transport.emitStatus({ type: "disconnected", final: false, reason: "max-lifetime" });
    await vi.waitFor(() => expect(client.state).toBe("connected"));

    expect(transport.connectTokens).toEqual(["tok-1", "tok-2"]);
    // the live subscription resumed: a fact delivered after reconnect reaches the consumer
    const got: number[] = [];
    void (async () => {
      for await (const e of handle.events()) got.push(e.sequence);
    })();
    transport.appendDurable(logSubject("t1", "im-1"), 1, wireEvent({ sequence: 1 }), true);
    await vi.waitFor(() => expect(got).toEqual([1]));
  });

  // @spec:clientsdk.connection.state-observable
  it("reports observable state transitions", async () => {
    const seen: ConnectionState[] = [];
    const { client, transport } = newClient();
    client.on("state", (s) => seen.push(s));
    await client.connect();
    transport.emitStatus({ type: "disconnected", final: false });
    await vi.waitFor(() => expect(client.state).toBe("connected"));
    await client.close();
    expect(seen).toEqual(["connecting", "connected", "reconnecting", "connected", "closed"]);
  });

  // R1/R4: a failing transport.connect() must not strand the client in "connecting".
  it("leaves connecting for disconnected when transport.connect keeps failing", async () => {
    const { client, transport } = newClient({ connectBackoffMs: [0, 0] });
    transport.failConnect(99);
    await expect(client.connect()).rejects.toThrow(/transport connect/);
    expect(client.state).toBe("disconnected");
  });

  // R1 (deep review): closing during an in-flight connect must NOT flip to "connected".
  it("does not flip to connected if closed mid-connect", async () => {
    const { client, transport } = newClient();
    const release = transport.gateConnect();
    const p = client.connect();
    await client.close();
    release();
    await p;
    expect(client.state).toBe("closed");
  });

  // Debate (X3/A4): close mid-connect must also CLOSE the late connection, not just guard state.
  it("closes the late connection when closed mid-connect", async () => {
    const { client, transport } = newClient();
    const release = transport.gateConnect();
    const p = client.connect();
    await client.close(); // transport.close() #1 (the nc isn't established yet — a no-op on real nats)
    release();
    await p; // establish() resolves; connect() sees closed → transport.close() #2 (the late conn)
    expect(transport.closeCount).toBe(2);
  });

  // Debate (A5): a closed interaction handle is dropped and re-created, not reused dead.
  it("recreates a handle after it is closed", () => {
    const { client } = newClient();
    const h1 = client.interaction("im-1");
    h1.close();
    const h2 = client.interaction("im-1");
    expect(h2).not.toBe(h1);
    expect(h2.isClosed).toBe(false);
  });

  it("recreates the agent feed after it is closed", () => {
    const { client } = newClient();
    const f1 = client.agentFeed();
    f1.close();
    const f2 = client.inbox();
    expect(f2).not.toBe(f1);
    expect(f2.isClosed).toBe(false);
  });

  // R3 (deep review): rapid drops must not run concurrent establish() — reconnects serialise.
  it("serialises reconnects (no concurrent establish)", async () => {
    const { client, transport } = newClient();
    await client.connect();
    const release = transport.gateConnect();
    transport.emitStatus({ type: "disconnected", final: false }); // reconnect #1 → establishing
    transport.emitStatus({ type: "disconnected", final: false }); // reconnect #2 → guarded by busy
    release();
    await vi.waitFor(() => expect(client.state).toBe("connected"));
    expect(transport.connectTokens).toHaveLength(2); // initial + exactly one reconnect
  });

  // R4: a still-flapping network on reconnect retries with backoff rather than halting forever.
  it("retries transport.connect on reconnect until it succeeds", async () => {
    const { client, transport } = newClient({ connectBackoffMs: [0, 0, 0, 0] });
    await client.connect();
    transport.failConnect(2); // first two reconnect attempts fail, third succeeds
    transport.emitStatus({ type: "disconnected", final: false });
    await vi.waitFor(() => expect(client.state).toBe("connected"));
  });

  it("resubscribes the agent feed after reconnect", async () => {
    const { client, transport } = newClient();
    await client.connect();
    const got = take(client.agentFeed().events(), 1);
    transport.emitStatus({ type: "disconnected", final: false });
    await vi.waitFor(() => expect(client.state).toBe("connected"));
    transport.deliverLive("tenant.t1.agent.alice.feed.im-1", wireEvent({ sequence: 1 }));
    await expect(got).resolves.toMatchObject([{ kind: "event", interactionId: "im-1" }]);
  });

  // @spec:clientsdk.connection.gettoken-failure
  it("becomes a fatal auth_failed after getToken retries exhaust", async () => {
    const getToken = vi.fn(() => Promise.reject(new Error("iam down")));
    const onAuthFailed = vi.fn();
    const { client } = newClient({ getToken });
    client.on("auth_failed", onAuthFailed);
    await expect(client.connect()).rejects.toBeInstanceOf(AuthFailedError);
    expect(getToken).toHaveBeenCalledTimes(2); // authBackoffMs length
    expect(client.state).toBe("failed");
    expect(onAuthFailed).toHaveBeenCalledOnce();
  });
});
