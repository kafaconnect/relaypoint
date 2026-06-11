package signaling

import (
	"context"
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"
)

func cmdSubj(tenant, iid, identity string) string {
	return "tenant." + tenant + ".interaction." + iid + ".cmd." + identity
}

// agentCmd marshals an ordinary agent command (actor = the subject suffix).
func agentCmd(id, tenant, actor, typ string) []byte {
	b, _ := proto.Marshal(&Command{CommandId: id, TenantId: tenant, ActorId: actor, Type: typ, Medium: "chat"})
	return b
}

// privCmd marshals a privileged participation command with its audit/target payload.
func privCmd(id, tenant, actor, typ string, pd participationData) []byte {
	data, _ := json.Marshal(pd)
	b, _ := proto.Marshal(&Command{CommandId: id, TenantId: tenant, ActorId: actor, Type: typ, Medium: "chat", Data: data})
	return b
}

func agentCtx(tenant, user string) context.Context {
	return WithIdentity(context.Background(), Identity{TenantID: tenant, UserID: user, Role: RoleAgent})
}

func deskCtx(tenant, svc string) context.Context {
	return WithIdentity(context.Background(), Identity{TenantID: tenant, UserID: svc, Role: RoleTrustedBackend})
}

// startWithDesk starts interaction iid via the trusted backend (lifecycle is not participant-gated).
func startWithDesk(t *testing.T, r *Router, tenant, iid, svc string) {
	t.Helper()
	got := r.HandleCommand(deskCtx(tenant, svc), cmdSubj(tenant, iid, svc), agentCmd("start-"+iid, tenant, svc, "interaction.started"))
	if got.Status != statusAccepted {
		t.Fatalf("desk start: %+v", got)
	}
}

func factsOf(st *fakeStore, tenant, iid string) []*Event {
	f, _, _ := st.Replay(logSubjectFor(tenant, iid))
	return f
}

// @spec:signaling.feed.cmd-identity-pinned
// The publisher identity is the LAST subject token; a payload actor_id that disagrees with the
// suffix is rejected with reason actor_mismatch and writes no fact.
func TestRouter_ActorMismatchRejected(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iA", "desk")

	// suffix says carol, payload claims alice → actor_mismatch.
	body, _ := proto.Marshal(&Command{CommandId: "x1", TenantId: "t1", ActorId: "alice", Type: "message.created", Medium: "chat"})
	got := r.HandleCommand(agentCtx("t1", "carol"), cmdSubj("t1", "iA", "carol"), body)
	if got.Status != statusRejected || got.Reason != "actor_mismatch" {
		t.Fatalf("payload/suffix mismatch must be actor_mismatch, got %+v", got)
	}
	if n := len(factsOf(st, "t1", "iA")); n != 1 {
		t.Fatalf("a rejected mismatch must write no fact: %d facts", n)
	}
}

// @spec:signaling.feed.cmd-nonparticipant-denied
// An authenticated agent who is not a current participant is rejected server-side
// (not_a_participant) and writes no fact, even though the wildcard publish ACL let it publish.
func TestRouter_NonParticipantDenied(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iN", "desk")

	got := r.HandleCommand(agentCtx("t1", "carol"), cmdSubj("t1", "iN", "carol"), agentCmd("m1", "t1", "carol", "message.created"))
	if got.Status != statusRejected || got.Reason != "not_a_participant" {
		t.Fatalf("non-participant must be rejected not_a_participant, got %+v", got)
	}
	if n := len(factsOf(st, "t1", "iN")); n != 1 {
		t.Fatalf("non-participant command must write no fact: %d facts", n)
	}
}

// @spec:signaling.feed.cmd-wildcard-no-reconnect
// Once the trusted backend assigns an agent (participant.joined fact), that agent's command is
// accepted with no reconnect — the same wildcard publish grant, now authorized by participation.
func TestRouter_AssignedAgentCommandAccepted(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iW", "desk")

	assign := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iW", "desk"),
		privCmd("a1", "t1", "desk", "participant.assign", participationData{Agent: "bob", Reason: "routing", RequestID: "q1"}))
	if assign.Status != statusAccepted {
		t.Fatalf("assign must be accepted, got %+v", assign)
	}
	// bob now commands without any reconnect (same wildcard grant), authorized as a participant.
	got := r.HandleCommand(agentCtx("t1", "bob"), cmdSubj("t1", "iW", "bob"), agentCmd("m1", "t1", "bob", "message.created"))
	if got.Status != statusAccepted {
		t.Fatalf("assigned agent command must be accepted, got %+v", got)
	}
}

// @spec:signaling.feed.privileged-assign-to-fact
// A trusted-backend participant.assign lands a participant.joined fact with audit fields
// (commanded_by from the suffix, reason, request_id); actor_id is the affected agent.
func TestRouter_PrivilegedAssignWritesAuditedFact(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iP", "desk")

	got := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iP", "desk"),
		privCmd("a1", "t1", "desk", "participant.assign", participationData{Agent: "bob", Reason: "manual", RequestID: "req-7"}))
	if got.Status != statusAccepted {
		t.Fatalf("privileged assign must be accepted, got %+v", got)
	}
	facts := factsOf(st, "t1", "iP")
	last := facts[len(facts)-1]
	if last.EventType != "participant.joined" || last.ActorId != "bob" {
		t.Fatalf("assign fact = %s actor=%s, want participant.joined actor=bob", last.EventType, last.ActorId)
	}
	if last.CommandedBy != "desk" || last.Reason != "manual" || last.RequestId != "req-7" {
		t.Fatalf("audit fields wrong: commanded_by=%q reason=%q request_id=%q", last.CommandedBy, last.Reason, last.RequestId)
	}
	if !FoldParticipation(facts).IsParticipantNow("bob") {
		t.Error("participation for bob must derive from the written fact")
	}
}

// @spec:signaling.feed.privileged-actor-guarded
// An agent-role connection cannot issue a participation command — it is rejected and writes no
// participation fact (the role comes from the authenticated identity, never the payload).
func TestRouter_PrivilegedActorGuarded(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iG", "desk")

	got := r.HandleCommand(agentCtx("t1", "alice"), cmdSubj("t1", "iG", "alice"),
		privCmd("a1", "t1", "alice", "participant.assign", participationData{Agent: "alice", Reason: "self", RequestID: "q"}))
	if got.Status != statusRejected {
		t.Fatalf("agent-role participation command must be rejected, got %+v", got)
	}
	for _, f := range factsOf(st, "t1", "iG") {
		if f.EventType == "participant.joined" {
			t.Fatalf("agent-issued privileged command must write no participation fact, found %+v", f)
		}
	}
}

// @spec:signaling.feed.privileged-transfer-ordering
// participant.transfer writes participant.joined(new) BEFORE participant.left(old): the new leg's
// join sequence is lower than the old leg's left, so the interaction is never absent from both.
func TestRouter_TransferJoinedBeforeLeft(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iT", "desk")
	// alice is the current assignee.
	if got := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iT", "desk"),
		privCmd("a1", "t1", "desk", "participant.assign", participationData{Agent: "alice", Reason: "init", RequestID: "q0"})); got.Status != statusAccepted {
		t.Fatalf("setup assign alice: %+v", got)
	}

	got := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iT", "desk"),
		privCmd("x1", "t1", "desk", "participant.transfer", participationData{From: "alice", Agent: "bob", Reason: "escalate", RequestID: "q1"}))
	if got.Status != statusAccepted {
		t.Fatalf("transfer must be accepted, got %+v", got)
	}

	var joinedBobSeq, leftAliceSeq int64
	for _, f := range factsOf(st, "t1", "iT") {
		if f.EventType == "participant.joined" && f.ActorId == "bob" {
			joinedBobSeq = f.Sequence
		}
		if f.EventType == "participant.left" && f.ActorId == "alice" {
			leftAliceSeq = f.Sequence
		}
	}
	if joinedBobSeq == 0 || leftAliceSeq == 0 {
		t.Fatalf("transfer must write both joined(bob) and left(alice); got joined=%d left=%d", joinedBobSeq, leftAliceSeq)
	}
	if joinedBobSeq >= leftAliceSeq {
		t.Fatalf("joined(bob) seq %d must be BEFORE left(alice) seq %d (no-gap)", joinedBobSeq, leftAliceSeq)
	}
	// post-transfer participation: bob in, alice out.
	v := FoldParticipation(factsOf(st, "t1", "iT"))
	if !v.IsParticipantNow("bob") || v.IsParticipantNow("alice") {
		t.Errorf("after transfer want bob in / alice out; bob=%v alice=%v", v.IsParticipantNow("bob"), v.IsParticipantNow("alice"))
	}
}

// @spec:signaling.feed.cmd-identity-pinned (dev fallback)
// Without an authenticated identity (shared-`client` dev posture) the suffix is advisory: the
// participant gate is NOT enforced, but the suffix is still the dev identity source and a payload
// actor_id that disagrees with it is still actor_mismatch.
func TestRouter_DevFallbackSuffixIsAdvisory(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	// no Identity in ctx → dev fallback. A non-participant may still start+message (not enforced).
	if got := r.HandleCommand(context.Background(), cmdSubj("t1", "iD", "u1"), agentCmd("s1", "t1", "u1", "interaction.started")); got.Status != statusAccepted {
		t.Fatalf("dev-fallback start: %+v", got)
	}
	if got := r.HandleCommand(context.Background(), cmdSubj("t1", "iD", "u1"), agentCmd("m1", "t1", "u1", "message.created")); got.Status != statusAccepted {
		t.Fatalf("dev-fallback message (participant gate off) must be accepted: %+v", got)
	}
	// but suffix/payload disagreement is still rejected.
	body, _ := proto.Marshal(&Command{CommandId: "m2", TenantId: "t1", ActorId: "someone-else", Type: "message.created", Medium: "chat"})
	if got := r.HandleCommand(context.Background(), cmdSubj("t1", "iD", "u1"), body); got.Status != statusRejected || got.Reason != "actor_mismatch" {
		t.Fatalf("dev-fallback suffix/payload mismatch must still be actor_mismatch, got %+v", got)
	}
}
