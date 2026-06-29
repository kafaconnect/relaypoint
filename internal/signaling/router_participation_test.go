package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	r := NewRouter(st, WithDevMode())
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

// A trusted backend acts on behalf of agents: publishing under its service suffix (desk-svc) it may
// carry an arbitrary actor_id (≠ suffix) and an empty actor_id on interaction.started — both
// accepted, and the message.created folds with the real author. This is the open-bus signal-test
// posture (desk-svc operator-listed, identity minted as a trusted backend even in dev mode).
func TestRouter_TrustedBackendActsForAgent(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st, WithDevMode())
	ctx := deskCtx("t1", "desk-svc")
	// interaction.started with no actor_id, published under the desk-svc suffix.
	start, _ := proto.Marshal(&Command{CommandId: "s1", TenantId: "t1", Type: "interaction.started", Medium: "chat"})
	if got := r.HandleCommand(ctx, cmdSubj("t1", "iT", "desk-svc"), start); got.Status != statusAccepted {
		t.Fatalf("trusted-backend start (no actor_id): %+v", got)
	}
	// message.created authored by agent1 (actor_id != suffix) — accepted, folded with the real actor.
	msg, _ := proto.Marshal(&Command{CommandId: "m1", TenantId: "t1", ActorId: "agent1", Type: "message.created", Medium: "chat"})
	if got := r.HandleCommand(ctx, cmdSubj("t1", "iT", "desk-svc"), msg); got.Status != statusAccepted {
		t.Fatalf("trusted-backend message on behalf of agent1: %+v", got)
	}
	facts, _, _ := st.Replay(logSubjectFor("t1", "iT"))
	var found bool
	for _, e := range facts {
		if e.EventType == "message.created" && e.ActorId == "agent1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("message.created must fold with the real author agent1; facts=%+v", facts)
	}
}

// @spec:signaling.feed.cmd-nonparticipant-denied (prod fail-closed)
// In production (no dev mode) an unauthenticated command is rejected outright, an authenticated
// non-participant agent is rejected not_a_participant, and a trusted-backend identity is accepted —
// the role/participation gates run on the real identity, never the dev fallback (A1).
func TestRouter_ProdFailClosed(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st) // prod: no dev mode
	startWithDesk(t, r, "t1", "iX", "desk")

	if got := r.HandleCommand(context.Background(), cmdSubj("t1", "iX", "u1"), agentCmd("c0", "t1", "u1", "message.created")); got.Status != statusRejected || got.Reason != "unauthenticated" {
		t.Fatalf("unauthenticated command must fail closed, got %+v", got)
	}
	if got := r.HandleCommand(agentCtx("t1", "carol"), cmdSubj("t1", "iX", "carol"), agentCmd("c1", "t1", "carol", "message.created")); got.Status != statusRejected || got.Reason != "not_a_participant" {
		t.Fatalf("prod non-participant must be not_a_participant, got %+v", got)
	}
	if got := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iX", "desk"),
		privCmd("a1", "t1", "desk", "participant.assign", participationData{Agent: "carol", Reason: "r", RequestID: "q"})); got.Status != statusAccepted {
		t.Fatalf("trusted backend assign must be accepted, got %+v", got)
	}
	if got := r.HandleCommand(agentCtx("t1", "carol"), cmdSubj("t1", "iX", "carol"), agentCmd("c2", "t1", "carol", "message.created")); got.Status != statusAccepted {
		t.Fatalf("now-participant carol must be accepted, got %+v", got)
	}
}

// @spec:signaling.feed.cmd-nonparticipant-denied (lifecycle gated)
// interaction.ended is participant-gated: a non-participant agent cannot end an interaction, but an
// assigned participant can (A2a).
func TestRouter_EndedIsParticipantGated(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iE", "desk")

	if got := r.HandleCommand(agentCtx("t1", "mallory"), cmdSubj("t1", "iE", "mallory"), agentCmd("e0", "t1", "mallory", "interaction.ended")); got.Status != statusRejected || got.Reason != "not_a_participant" {
		t.Fatalf("non-participant ending must be not_a_participant, got %+v", got)
	}
	if got := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iE", "desk"),
		privCmd("a1", "t1", "desk", "participant.assign", participationData{Agent: "bob", Reason: "r", RequestID: "q"})); got.Status != statusAccepted {
		t.Fatalf("assign bob: %+v", got)
	}
	if got := r.HandleCommand(agentCtx("t1", "bob"), cmdSubj("t1", "iE", "bob"), agentCmd("e1", "t1", "bob", "interaction.ended")); got.Status != statusAccepted {
		t.Fatalf("participant bob ending must be accepted, got %+v", got)
	}
}

// @spec:signaling.feed.privileged-actor-guarded (direct fact rejected)
// A participation FACT (participant.joined/left, interaction.assigned) sent as a DIRECT command —
// not via the privileged assign/unassign/transfer path — is rejected and writes no fact (A2b).
func TestRouter_DirectParticipationFactRejected(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iJ", "desk")
	// even the trusted backend may not forge a raw fact directly.
	for _, typ := range []string{"participant.joined", "participant.left", "interaction.assigned"} {
		got := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iJ", "desk"), agentCmd("d-"+typ, "t1", "desk", typ))
		if got.Status != statusRejected {
			t.Fatalf("direct %s must be rejected, got %+v", typ, got)
		}
		for _, f := range factsOf(st, "t1", "iJ") {
			if f.EventType == typ {
				t.Fatalf("direct %s must write no fact", typ)
			}
		}
	}
}

// @spec:signaling.feed.cmd-nonparticipant-denied (post-revocation race)
// Participation is re-evaluated against fresh state after an OCC rebuild: a participant.left that
// commits between the first fold and the append makes the racing command fail not_a_participant
// rather than slip through (A3). leftMidAppendStore injects the revoking fact on the first append.
func TestRouter_ParticipationRecheckedAfterOCCRebuild(t *testing.T) {
	st := newFakeStore()
	seed := NewRouter(st)
	startWithDesk(t, seed, "t1", "iR", "desk")
	if got := seed.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iR", "desk"),
		privCmd("a1", "t1", "desk", "participant.assign", participationData{Agent: "bob", Reason: "r", RequestID: "q"})); got.Status != statusAccepted {
		t.Fatalf("assign bob: %+v", got)
	}

	// bob is a participant when the command's first fold runs, but a participant.left(bob) lands
	// (forcing an OCC conflict + rebuild) before bob's append commits.
	leftBob := &Event{Schema: SchemaV1, EventType: "participant.left", EventId: "x", TenantId: "t1", ActorId: "bob"}
	racing := &occInjectStore{fakeStore: st, inject: leftBob, subject: logSubjectFor("t1", "iR")}
	r := NewRouter(racing)

	got := r.HandleCommand(agentCtx("t1", "bob"), cmdSubj("t1", "iR", "bob"), agentCmd("m1", "t1", "bob", "message.created"))
	if got.Status != statusRejected || got.Reason != "not_a_participant" {
		t.Fatalf("post-revocation racing command must be not_a_participant, got %+v", got)
	}
}

// @spec:signaling.feed.privileged-assign-to-fact (parent idempotency)
// A retry of a privileged command's parent command_id carrying a DIVERGENT payload is rejected
// before any sub-fact is re-emitted; a same-payload retry replays idempotently with no extra fact (A5).
func TestRouter_PrivilegedParentIdempotency(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iI", "desk")

	first := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iI", "desk"),
		privCmd("p1", "t1", "desk", "participant.assign", participationData{Agent: "bob", Reason: "r", RequestID: "q"}))
	if first.Status != statusAccepted {
		t.Fatalf("first assign: %+v", first)
	}
	joinedAfterFirst := 0
	for _, f := range factsOf(st, "t1", "iI") {
		if f.EventType == "participant.joined" {
			joinedAfterFirst++
		}
	}

	// same parent id, divergent payload (different agent) → conflict, no new fact.
	div := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iI", "desk"),
		privCmd("p1", "t1", "desk", "participant.assign", participationData{Agent: "carol", Reason: "r", RequestID: "q"}))
	if div.Status != statusRejected {
		t.Fatalf("divergent parent retry must conflict, got %+v", div)
	}
	// same parent id, same payload → idempotent replay.
	same := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iI", "desk"),
		privCmd("p1", "t1", "desk", "participant.assign", participationData{Agent: "bob", Reason: "r", RequestID: "q"}))
	if same.Status != statusAccepted {
		t.Fatalf("same-payload retry must replay accepted, got %+v", same)
	}
	joinedAfter := 0
	for _, f := range factsOf(st, "t1", "iI") {
		if f.EventType == "participant.joined" {
			joinedAfter++
		}
	}
	if joinedAfter != joinedAfterFirst {
		t.Fatalf("retries appended extra participation facts: %d -> %d", joinedAfterFirst, joinedAfter)
	}
	if !FoldParticipation(factsOf(st, "t1", "iI")).IsParticipantNow("bob") || FoldParticipation(factsOf(st, "t1", "iI")).IsParticipantNow("carol") {
		t.Fatal("divergent retry must not have joined carol")
	}
}

type failLeftOnceStore struct {
	*fakeStore
	failed bool
}

func (s *failLeftOnceStore) Append(ctx context.Context, subject string, data []byte, dedupID string, exp uint64) (bool, uint64, error) {
	if !s.failed && strings.Contains(dedupID, "participant.left") {
		s.failed = true
		return false, 0, errors.New("append boom")
	}
	return s.fakeStore.Append(ctx, subject, data, dedupID, exp)
}

// @spec:router.transfer.partial-apply-idempotent
func TestRouter_TransferPartialApplyReDrives(t *testing.T) {
	st := &failLeftOnceStore{fakeStore: newFakeStore()}
	r := NewRouter(st)
	startWithDesk(t, r, "t1", "iPA", "desk")
	if got := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iPA", "desk"),
		privCmd("a0", "t1", "desk", "participant.assign", participationData{Agent: "alice", Reason: "init", RequestID: "q0"})); got.Status != statusAccepted {
		t.Fatalf("setup assign alice: %+v", got)
	}

	partial := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iPA", "desk"),
		privCmd("x1", "t1", "desk", "participant.transfer", participationData{From: "alice", Agent: "bob", Reason: "escalate", RequestID: "q1"}))
	if partial.Status != statusRejected {
		t.Fatalf("first transfer must surface the failed leave, got %+v", partial)
	}
	v := FoldParticipation(factsOf(st.fakeStore, "t1", "iPA"))
	if !v.IsParticipantNow("bob") || !v.IsParticipantNow("alice") {
		t.Fatalf("after partial apply want bob joined AND alice still a member (over-delivery); bob=%v alice=%v", v.IsParticipantNow("bob"), v.IsParticipantNow("alice"))
	}

	div := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iPA", "desk"),
		privCmd("x1", "t1", "desk", "participant.transfer", participationData{From: "alice", Agent: "dave", Reason: "escalate", RequestID: "q1"}))
	if div.Status != statusRejected || div.Reason != "conflict: command_id reused with a different payload" {
		t.Fatalf("divergent payload retry must still be a command_id-reuse conflict, got %+v", div)
	}

	retry := r.HandleCommand(deskCtx("t1", "desk"), cmdSubj("t1", "iPA", "desk"),
		privCmd("x1", "t1", "desk", "participant.transfer", participationData{From: "alice", Agent: "bob", Reason: "escalate", RequestID: "q1"}))
	if retry.Status != statusAccepted {
		t.Fatalf("partial-apply retry must re-drive and complete, got %+v", retry)
	}
	v = FoldParticipation(factsOf(st.fakeStore, "t1", "iPA"))
	if !v.IsParticipantNow("bob") || v.IsParticipantNow("alice") {
		t.Fatalf("after retry want bob in / alice out; bob=%v alice=%v", v.IsParticipantNow("bob"), v.IsParticipantNow("alice"))
	}
	var joinedBob, leftAlice int
	for _, f := range factsOf(st.fakeStore, "t1", "iPA") {
		if f.EventType == "participant.joined" && f.ActorId == "bob" {
			joinedBob++
		}
		if f.EventType == "participant.left" && f.ActorId == "alice" {
			leftAlice++
		}
	}
	if joinedBob != 1 || leftAlice != 1 {
		t.Fatalf("want exactly one joined(bob) and one left(alice); joined=%d left=%d", joinedBob, leftAlice)
	}
}

// occInjectStore appends `inject` to `subject` exactly once, on the FIRST Append, before delegating —
// so the next Append sees a moved stream sequence (OCC conflict) and the router re-folds fresh state.
type occInjectStore struct {
	*fakeStore
	inject   *Event
	subject  string
	injected bool
}

func (s *occInjectStore) Append(ctx context.Context, subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (bool, uint64, error) {
	if !s.injected && subject == s.subject {
		s.injected = true
		s.inject.Sequence = int64(expectedLastSubjSeq) + 1
		raw, _ := proto.Marshal(s.inject)
		if _, _, err := s.fakeStore.Append(ctx, subject, raw, "inject-"+dedupID, expectedLastSubjSeq); err != nil {
			return false, 0, err
		}
	}
	return s.fakeStore.Append(ctx, subject, data, dedupID, expectedLastSubjSeq)
}
