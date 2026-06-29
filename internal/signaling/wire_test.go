package signaling

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// @spec:wire.protobuf.round-trip
func TestWireRoundTrip(t *testing.T) {
	chat, _ := proto.Marshal(&ChatMessage{Text: "hello", AttachmentRefs: []string{"obj-1", "obj-2"}})

	t.Run("Event", func(t *testing.T) {
		in := &Event{
			Schema: SchemaV1, EventType: "message.created", EventId: "e1", Sequence: 7,
			OccurredAt: timestamppb.Now(), TenantId: "t1", ActorId: "u1", Medium: "chat",
			MediaProfile: "webrtc-p2p", CommandId: "c1", PayloadHash: "ph", CausedBy: "c1",
			RefId: "m1", Data: chat,
		}
		assertRoundTrip(t, in)
	})

	t.Run("Command", func(t *testing.T) {
		assertRoundTrip(t, &Command{
			CommandId: "c1", TenantId: "t1", ActorId: "u1", Type: "message.created",
			Medium: "chat", RefId: "m1", Data: chat,
		})
	})

	t.Run("CommandResult", func(t *testing.T) {
		assertRoundTrip(t, &CommandResult{
			CommandId: "c1", Status: statusRejected, CausedBy: "c1", Reason: "conflict",
		})
	})

	t.Run("SignalEvent", func(t *testing.T) {
		assertRoundTrip(t, &SignalEvent{Schema: SchemaV1, Type: "typing", ActorId: "u1", Data: []byte{1, 2, 3}})
	})

	t.Run("ChatMessage", func(t *testing.T) {
		assertRoundTrip(t, &ChatMessage{Text: "hi", AttachmentRefs: []string{"a"}})
	})
}

func assertRoundTrip(t *testing.T, in proto.Message) {
	t.Helper()
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := in.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(b, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round-trip mismatch:\n in=%v\nout=%v", in, out)
	}
}
