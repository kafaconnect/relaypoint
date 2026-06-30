package signaling

import "testing"

func ev(seq int64, typ, agent string) *Event {
	return &Event{Sequence: seq, EventType: typ, ActorId: agent}
}

// @spec:signaling.feed.participation-from-facts
func TestParticipation_IntervalFold(t *testing.T) {
	facts := []*Event{
		ev(1, "interaction.started", "u1"),
		ev(2, "participant.joined", "alice"),
		ev(3, "message.created", "alice"),
		ev(4, "participant.joined", "bob"),
		ev(5, "participant.left", "alice"),
		ev(6, "message.created", "bob"),
	}
	v := FoldParticipation(facts)

	if v.IsParticipantNow("alice") {
		t.Error("alice left at seq 5 — must not be a current participant")
	}
	if !v.IsParticipantNow("bob") {
		t.Error("bob joined at seq 4 and never left — must be a current participant")
	}
	if v.IsParticipantNow("carol") {
		t.Error("carol never joined — must not be a participant")
	}

	al := v.Intervals("alice")
	if len(al) != 1 || al[0].JoinSeq != 2 || al[0].LeftSeq != 5 || al[0].LeftOpen {
		t.Fatalf("alice interval = %+v, want [2,5) closed", al)
	}
	bo := v.Intervals("bob")
	if len(bo) != 1 || bo[0].JoinSeq != 4 || !bo[0].LeftOpen {
		t.Fatalf("bob interval = %+v, want [4,∞) open", bo)
	}
	if v.Intervals("carol") != nil {
		t.Error("carol must have no intervals")
	}
}

func TestParticipation_AssignedAndRejoin(t *testing.T) {
	// An assign lands structurally as participant.joined (RH-11a: no distinct interaction.assigned fact).
	v := FoldParticipation([]*Event{
		ev(1, "interaction.started", "u1"),
		ev(2, "participant.joined", "alice"),
		ev(3, "participant.left", "alice"),
		ev(4, "participant.joined", "alice"),
	})
	in := v.Intervals("alice")
	if len(in) != 2 {
		t.Fatalf("alice intervals = %+v, want 2 (assigned→left, then re-join)", in)
	}
	if in[0].JoinSeq != 2 || in[0].LeftSeq != 3 || in[0].LeftOpen {
		t.Fatalf("first interval = %+v, want [2,3) closed", in[0])
	}
	if in[1].JoinSeq != 4 || !in[1].LeftOpen {
		t.Fatalf("second interval = %+v, want [4,∞) open", in[1])
	}
	if !v.IsParticipantNow("alice") {
		t.Error("alice re-joined at seq 4 — must be a current participant")
	}
}

// @spec:router.cluster.assigned-emit-or-drop
// Decision: DROP. interaction.assigned was recognized but never emitted; it is a domain verb, not a
// delivery structure, so it is removed from both the recognized set and the fold. An assign opens
// membership as participant.joined; a stray interaction.assigned fact must NOT open an interval.
func TestParticipation_AssignedFactDropped(t *testing.T) {
	if isParticipationFact("interaction.assigned") {
		t.Error("interaction.assigned must not be a recognized participation fact (dropped, never emitted)")
	}
	v := FoldParticipation([]*Event{
		ev(1, "interaction.started", "u1"),
		ev(2, "interaction.assigned", "alice"),
	})
	if v.IsParticipantNow("alice") {
		t.Error("a dropped interaction.assigned fact must not open membership in the fold")
	}
}
