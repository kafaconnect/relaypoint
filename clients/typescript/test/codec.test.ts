import { describe, expect, it } from "vitest";
import { decodeCommandResult, decodeLogEvent, encodeCommand } from "../src/codec.js";
import { readJson, wireEvent, wireResult } from "./helpers.js";

describe("wire-field naming projection", () => {
  // @spec:clientsdk.cmd.wire-field-mapping
  it("maps camelCase 1:1 to snake_case and keeps the projection precise", () => {
    const wire = readJson(
      encodeCommand(
        { type: "message.updated", commandId: "K1", refId: "m-7", data: { text: "hi" } },
        { tenantId: "t1", actorId: "alice", medium: "chat" },
      ),
    );
    // command serializes to snake_case wire fields only
    expect(wire).toMatchObject({
      command_id: "K1",
      tenant_id: "t1",
      actor_id: "alice",
      type: "message.updated",
      medium: "chat",
      ref_id: "m-7",
      data: { text: "hi" },
    });
    expect(Object.keys(wire)).not.toContain("commandId");
    expect(Object.keys(wire)).not.toContain("refId");

    // a fact decodes to camelCase, carries causedBy (NOT commandId), and keeps event-specific
    // fields INSIDE data (negotiation_id is not promoted to the envelope)
    const ev = decodeLogEvent(
      wireEvent({
        sequence: 5,
        eventType: "call.renegotiated",
        causedBy: "K1",
        mediaProfile: "webrtc-p2p",
        data: { negotiation_id: "n-1" },
      }),
    );
    expect(ev.causedBy).toBe("K1");
    expect(ev.eventType).toBe("call.renegotiated");
    expect(ev.mediaProfile).toBe("webrtc-p2p"); // media_profile is an envelope field, projected
    expect(ev.sequence).toBe(5);
    expect("commandId" in ev).toBe(false);
    expect(ev.data).toEqual({ negotiation_id: "n-1" });
    expect("negotiationId" in (ev as unknown as Record<string, unknown>)).toBe(false);

    const result = decodeCommandResult(wireResult({ commandId: "K1", status: "accepted", causedBy: "K1" }));
    expect(result).toEqual({ commandId: "K1", status: "accepted", causedBy: "K1" });
  });
});
