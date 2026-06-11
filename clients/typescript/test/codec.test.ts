import { describe, expect, it } from "vitest";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ChatMessageSchema,
  CommandResultSchema,
  CommandResult_Status,
  CommandSchema,
  EventSchema,
  SignalEventSchema,
} from "../src/gen/relaypoint/interaction/v1/interaction_pb.js";
import { decodeCommandResult, decodeLogEvent, encodeCommand, encodeSignal } from "../src/codec.js";
import { chatData, readCommand, wireEvent, wireResult } from "./helpers.js";

describe("protobuf wire codec", () => {
  // @spec:clientsdk.cmd.wire-field-mapping
  it("projects the camelCase public API onto the protobuf wire and back", () => {
    const cmdBytes = encodeCommand(
      { type: "message.updated", commandId: "K1", refId: "m-7", data: { text: "hi", attachmentRefs: [] } },
      { tenantId: "t1", actorId: "alice", medium: "chat" },
    );
    const wire = readCommand(cmdBytes);
    expect(wire).toMatchObject({
      command_id: "K1",
      tenant_id: "t1",
      actor_id: "alice",
      type: "message.updated",
      medium: "chat",
      ref_id: "m-7",
    });
    // chat `data` is a ChatMessage marshaled into bytes (the payload registry)
    const chat = fromBinary(ChatMessageSchema, wire.data as Uint8Array);
    expect(chat.text).toBe("hi");

    // a chat fact decodes to camelCase, carries causedBy (NOT commandId) and a decoded ChatPayload
    const ev = decodeLogEvent(
      wireEvent({ sequence: 5, eventType: "message.created", causedBy: "K1", data: chatData({ text: "hello", attachmentRefs: ["a"] }) }),
    );
    expect(ev.causedBy).toBe("K1");
    expect(ev.eventType).toBe("message.created");
    expect(ev.sequence).toBe(5);
    expect("commandId" in ev).toBe(false);
    expect(ev.data).toEqual({ text: "hello", attachmentRefs: ["a"] });

    // a non-chat fact keeps `data` opaque (raw bytes) — media_profile stays an envelope field
    const sdp = new Uint8Array([1, 2, 3]);
    const callEv = decodeLogEvent(wireEvent({ sequence: 6, eventType: "call.renegotiated", mediaProfile: "webrtc-p2p", data: sdp }));
    expect(callEv.mediaProfile).toBe("webrtc-p2p");
    expect(callEv.data).toEqual(sdp);

    const result = decodeCommandResult(wireResult({ commandId: "K1", status: "accepted", causedBy: "K1" }));
    expect(result).toEqual({ commandId: "K1", status: "accepted", causedBy: "K1" });
  });

  // @spec:wire.protobuf.round-trip
  // Round-trip each wire message through protobuf encode/decode unchanged.
  describe("encode/decode round-trip", () => {
    it("Command", () => {
      const bytes = encodeCommand(
        { type: "message.created", commandId: "c1", refId: "m1", data: { text: "hi", attachmentRefs: ["x"] } },
        { tenantId: "t1", actorId: "u1", medium: "chat" },
      );
      const c = fromBinary(CommandSchema, bytes);
      expect(c).toMatchObject({ commandId: "c1", tenantId: "t1", actorId: "u1", type: "message.created", medium: "chat", refId: "m1" });
      expect(fromBinary(ChatMessageSchema, c.data)).toMatchObject({ text: "hi", attachmentRefs: ["x"] });
    });

    it("CommandResult", () => {
      const bytes = toBinary(
        CommandResultSchema,
        fromBinary(CommandResultSchema, wireResult({ commandId: "c1", status: "rejected", reason: "conflict" })),
      );
      expect(decodeCommandResult(bytes)).toEqual({ commandId: "c1", status: "rejected", reason: "conflict" });
    });

    it("Event", () => {
      const ev = decodeLogEvent(wireEvent({ sequence: 9, eventType: "message.created", data: chatData({ text: "yo", attachmentRefs: [] }) }));
      expect(ev.sequence).toBe(9);
      expect(ev.data).toEqual({ text: "yo", attachmentRefs: [] });
    });

    it("SignalEvent", () => {
      const payload = new Uint8Array([9, 8, 7]);
      const s = fromBinary(SignalEventSchema, encodeSignal("typing", "alice", payload));
      expect(s).toMatchObject({ schema: "relaypoint.interaction.v1", type: "typing", actorId: "alice" });
      expect(s.data).toEqual(payload);
    });

    it("ChatMessage", () => {
      const bytes = chatData({ text: "hi", attachmentRefs: ["a", "b"] });
      expect(fromBinary(ChatMessageSchema, bytes)).toMatchObject({ text: "hi", attachmentRefs: ["a", "b"] });
    });

    it("CommandResult status enum maps to the public string", () => {
      expect(decodeCommandResult(wireResult({ commandId: "c", status: "accepted" })).status).toBe("accepted");
      expect(CommandResult_Status.REJECTED).toBe(2);
    });
  });
});
