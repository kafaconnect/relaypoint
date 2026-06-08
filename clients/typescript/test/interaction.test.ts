import { describe, expect, it, vi } from "vitest";
import { RelayPointClient } from "../src/client.js";
import { CommandRejectedError } from "../src/errors.js";
import { cmdSubject, logSubject, signalSubject } from "../src/subjects.js";
import { FakeTransport } from "../src/testing/fake-transport.js";
import { immediate, readJson, wireEvent, wireResult } from "./helpers.js";

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
    expect(transport.requests[0]?.subject).toBe(cmdSubject("t1", "im-1"));
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
    expect(readJson(transport.requests[0]!.data).command_id).toBe("K");
    expect(readJson(transport.requests[1]!.data).command_id).toBe("K");
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
    transport.appendDurable(subject, 1, wireEvent({ sequence: 1, eventType: "interaction.context.updated", data: { name: "Acme" } }), true);
    transport.appendDurable(subject, 2, wireEvent({ sequence: 2, eventType: "interaction.context.updated", data: { name: "Acme Corp" } }), true);
    await vi.waitFor(() => expect(handle.metadata).toEqual({ name: "Acme Corp" }));
    expect(seen).toEqual([{ name: "Acme" }, { name: "Acme Corp" }]);
  });
});
