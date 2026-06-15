import { describe, expect, it, vi } from "vitest";
import { RelayPointClient } from "../src/client.js";
import { CommandRejectedError } from "../src/errors.js";
import { cmdSubject, logSubject, signalSubject } from "../src/subjects.js";
import { FakeTransport } from "../src/testing/fake-transport.js";
import { immediate, readCommand, take, wireEvent, wireFeedControl, wireFeedRevoked, wireResult } from "./helpers.js";

function setup() {
  const transport = new FakeTransport();
  const client = new RelayPointClient(
    { servers: ["ws://x"], selfUserId: "alice", tenantId: "t1", getToken: () => Promise.resolve("tok") },
    { transport, wait: immediate },
  );
  return { client, transport };
}

describe("command plane", () => {
  // @spec:clientsdk.cmd.send-to-cmd
  // @spec:clientsdk.cmd.result-correlation
  it("publishes on .cmd and resolves with the accepted CommandResult", async () => {
    const { client, transport } = setup();
    transport.setResponder((_s, _d) => wireResult({ commandId: "K", status: "accepted", causedBy: "K" }));
    const handle = client.interaction("im-1");
    const result = await handle.send({ type: "message.created", commandId: "K", data: { text: "hi" } });
    expect(result).toEqual({ commandId: "K", status: "accepted", causedBy: "K" });
    expect(transport.requests[0]?.subject).toBe(cmdSubject("t1", "im-1", "alice"));
  });

  // @spec:clientsdk.cmd.no-log-write
  it("never targets .log and exposes no log-write method", async () => {
    const { client, transport } = setup();
    transport.setResponder(() => wireResult({ commandId: "K", status: "accepted", causedBy: "K" }));
    const handle = client.interaction("im-1");
    await handle.send({ type: "message.created", commandId: "K" });
    await handle.signal({ type: "typing.start" });
    const subjects = [...transport.requests, ...transport.publishes].map((m) => m.subject);
    expect(subjects.some((s) => s.endsWith(".log"))).toBe(false);
    for (const m of ["append", "writeLog", "publishLog", "log"]) {
      expect((handle as unknown as Record<string, unknown>)[m]).toBeUndefined();
    }
  });

  // @spec:clientsdk.cmd.idempotent-retry
  it("reuses the same command_id on retry after a transport timeout", async () => {
    const { client, transport } = setup();
    let calls = 0;
    transport.setResponder(() => {
      if (calls++ === 0) throw new Error("timeout");
      return wireResult({ commandId: "K", status: "accepted", causedBy: "K" });
    });
    const handle = client.interaction("im-1");
    const result = await handle.send({ type: "message.created", commandId: "K", data: { text: "hi" } });
    expect(result.status).toBe("accepted");
    expect(transport.requests).toHaveLength(2);
    expect(readCommand(transport.requests[0]!.data).command_id).toBe("K");
    expect(readCommand(transport.requests[1]!.data).command_id).toBe("K");
  });

  // @spec:clientsdk.handle.concurrent-command-guard
  it("rejects with a typed error when the router rejects the command", async () => {
    const { client, transport } = setup();
    transport.setResponder(() => wireResult({ commandId: "K", status: "rejected", reason: "conflict" }));
    const handle = client.interaction("im-1");
    await expect(handle.send({ type: "interaction.transfer.requested", commandId: "K" })).rejects.toMatchObject({
      name: "CommandRejectedError",
      commandId: "K",
      reason: "conflict",
    });
  });
});

describe("agent feed", () => {
  // @spec:clientsdk.feed.own-subscription
  // @spec:clientsdk.feed.event-copy
  it("reads own per-agent feed and surfaces interaction ids from feed subjects", async () => {
    const { client, transport } = setup();
    const got = take(client.agentFeed().events(), 1);
    transport.deliverLive("tenant.t1.agent.bob.feed.im-0", wireEvent({ sequence: 99 }));
    transport.deliverLive("tenant.t1.agent.alice.feed.im-1", wireEvent({ sequence: 1 }));
    const [item] = await got;
    expect(item).toMatchObject({
      kind: "event",
      interactionId: "im-1",
      event: { sequence: 1, eventType: "message.created" },
    });
  });

  // @spec:clientsdk.feed.revoked-tombstone
  it("decodes feed.revoked tombstones as control items", async () => {
    const { client, transport } = setup();
    const got = take(client.inbox().events(), 1);
    transport.deliverLive("tenant.t1.agent.alice.feed.im-1", wireFeedRevoked({ interactionId: "im-1", atSequence: 7 }));
    await expect(got).resolves.toEqual([{ kind: "revoked", interactionId: "im-1", atSequence: 7 }]);
  });

  // @spec:clientsdk.feed.unknown-control
  it("surfaces unknown feed controls without forging Events", async () => {
    const { client, transport } = setup();
    const got = take(client.agentFeed().events(), 1);
    transport.deliverLive(
      "tenant.t1.agent.alice.feed.im-1",
      wireFeedControl({ control: "feed.paused", interactionId: "im-1", atSequence: 8 }),
    );
    await expect(got).resolves.toEqual([{ kind: "control", control: "feed.paused", interactionId: "im-1", atSequence: 8 }]);
  });

  // @spec:clientsdk.feed.decode-error-continues
  it("keeps the feed alive after an undecodable payload", async () => {
    const { client, transport } = setup();
    const got = take(client.agentFeed().events(), 2);
    transport.deliverLive("tenant.t1.agent.alice.feed.im-1", new Uint8Array([10, 2, 65]));
    transport.deliverLive("tenant.t1.agent.alice.feed.im-1", wireEvent({ sequence: 2 }));
    const [bad, good] = await got;
    expect(bad).toMatchObject({ kind: "decode_error", subject: "tenant.t1.agent.alice.feed.im-1" });
    expect(good).toMatchObject({ kind: "event", interactionId: "im-1", event: { sequence: 2 } });
  });
});

describe("interaction handle", () => {
  // @spec:clientsdk.handle.stream-and-send
  it("streams ordered .log facts and sends commands", async () => {
    const { client, transport } = setup();
    transport.setResponder(() => wireResult({ commandId: "K", status: "accepted", causedBy: "K" }));
    const handle = client.interaction("im-1");
    const got: number[] = [];
    void (async () => {
      for await (const e of handle.events()) got.push(e.sequence);
    })();
    transport.appendDurable(logSubject("t1", "im-1"), 1, wireEvent({ sequence: 1 }), true);
    await vi.waitFor(() => expect(got).toEqual([1]));
    await expect(handle.send({ type: "message.created", commandId: "K" })).resolves.toMatchObject({
      status: "accepted",
    });
  });

  // Regression (deep review): a handle opened on an interaction with existing facts but no NEW
  // live event must still deliver the history (initial replay on open).
  it("loads existing history on open even with no new live fact", async () => {
    const { client, transport } = setup();
    const subject = logSubject("t1", "im-1");
    transport.appendDurable(subject, 1, wireEvent({ sequence: 1 })); // durable only, not live
    transport.appendDurable(subject, 2, wireEvent({ sequence: 2 }));
    const handle = client.interaction("im-1");
    const got = await take(handle.events(), 2);
    expect(got.map((e) => e.sequence)).toEqual([1, 2]);
  });

  // @spec:clientsdk.handle.signal-own-author
  it("publishes signals only on the own-author subject", async () => {
    const { client, transport } = setup();
    const handle = client.interaction("im-1");
    await handle.signal({ type: "typing.start" });
    expect(transport.publishes[0]?.subject).toBe(signalSubject("t1", "im-1", "alice"));
    expect(transport.publishes.every((p) => p.subject.endsWith(".signal.alice"))).toBe(true);
  });

  // @spec:clientsdk.handle.metadata-observable
  it("surfaces the latest opaque context from context.updated facts", async () => {
    const { client, transport } = setup();
    const handle = client.interaction("im-1");
    const seen: unknown[] = [];
    handle.on("metadata", (c) => seen.push(c));
    void handle.events(); // open the live subscription
    const subject = logSubject("t1", "im-1");
    // interaction.context.updated has no registry payload yet → `data` is opaque bytes.
    const ctx1 = new Uint8Array([1]);
    const ctx2 = new Uint8Array([2]);
    transport.appendDurable(subject, 1, wireEvent({ sequence: 1, eventType: "interaction.context.updated", data: ctx1 }), true);
    transport.appendDurable(subject, 2, wireEvent({ sequence: 2, eventType: "interaction.context.updated", data: ctx2 }), true);
    await vi.waitFor(() => expect(handle.metadata).toEqual(ctx2));
    expect(seen).toEqual([ctx1, ctx2]);
  });
});
