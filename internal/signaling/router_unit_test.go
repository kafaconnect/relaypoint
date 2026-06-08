package signaling

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

// fakeStore is an in-memory LogStore — proves the router core needs no NATS.
type fakeStore struct {
	mu    sync.Mutex
	facts map[string][]Event
	dedup map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{facts: map[string][]Event{}, dedup: map[string]bool{}}
}

func (s *fakeStore) Append(subject string, data []byte, dedupID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dedup[dedupID] {
		return true, nil
	}
	s.dedup[dedupID] = true
	var e Event
	_ = json.Unmarshal(data, &e)
	s.facts[subject] = append(s.facts[subject], e)
	return false, nil
}

func (s *fakeStore) Replay(subject string) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.facts[subject]...), nil
}

func cmd(id, tenant, typ string, data map[string]any) []byte {
	b, _ := json.Marshal(Command{CommandID: id, TenantID: tenant, ActorID: "u1", Type: typ, Medium: "chat", Data: data})
	return b
}

const subj = "tenant.t1.interaction.iX.cmd"

func TestCore_NoNATS(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st)

	if got := r.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", nil)); got.Status != "accepted" || got.CausedBy != "c1" {
		t.Fatalf("start: %+v (want accepted, caused_by=c1)", got)
	}
	if got := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", map[string]any{"t": "hi"})); got.Status != "accepted" {
		t.Fatalf("message: %+v", got)
	}
	facts, _ := st.Replay(logSubjectFor("t1", "iX"))
	if len(facts) != 2 || facts[0].Sequence != 1 || facts[1].Sequence != 2 {
		t.Fatalf("sequences %+v want 1,2", facts)
	}

	// idempotency: same command replayed → no second fact
	a := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", map[string]any{"t": "hi"}))
	facts, _ = st.Replay(logSubjectFor("t1", "iX"))
	if a.Status != "accepted" || len(facts) != 2 {
		t.Fatalf("retry double-appended: %+v / %d facts", a, len(facts))
	}
	// conflict: same id, different payload
	if got := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", map[string]any{"t": "DIFF"})); got.Status != "rejected" {
		t.Fatalf("conflict not rejected: %+v", got)
	}
	// payload tenant mismatch
	if got := r.HandleCommand(context.Background(), subj, cmd("c3", "OTHER", "message.created", nil)); got.Status != "rejected" {
		t.Fatalf("tenant mismatch not rejected: %+v", got)
	}
	// illegal: message before start on a fresh interaction
	if got := r.HandleCommand(context.Background(), "tenant.t1.interaction.iY.cmd", cmd("c4", "t1", "message.created", nil)); got.Status != "rejected" {
		t.Fatalf("illegal not rejected: %+v", got)
	}
}

// TestCore_RestartRebuild: a NEW router over the SAME store (a restart) continues the
// sequence and respects state — proving state is rebuilt from the durable log.
func TestCore_RestartRebuild(t *testing.T) {
	st := newFakeStore()
	r1 := NewRouter(st)
	r1.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", nil))
	r1.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", map[string]any{"t": "a"}))

	r2 := NewRouter(st) // restart: empty in-memory state
	got := r2.HandleCommand(context.Background(), subj, cmd("c3", "t1", "message.created", map[string]any{"t": "b"}))
	if got.Status != "accepted" {
		t.Fatalf("post-restart message rejected (state not rebuilt): %+v", got)
	}
	facts, _ := st.Replay(logSubjectFor("t1", "iX"))
	if n := len(facts); n != 3 || facts[2].Sequence != 3 {
		t.Fatalf("post-restart sequence wrong: %d facts, last seq %d (want 3,3)", n, facts[len(facts)-1].Sequence)
	}
	// a replayed command from before the restart is recognised (no double-append)
	got = r2.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", map[string]any{"t": "a"}))
	facts, _ = st.Replay(logSubjectFor("t1", "iX"))
	if got.Status != "accepted" || len(facts) != 3 {
		t.Fatalf("post-restart replay double-appended: %+v / %d", got, len(facts))
	}
}

// @spec:signaling.cmd.forged-author-rejected
// With an authenticated Identity in context, a command whose actor_id differs from
// the authenticated user is rejected (the subject/payload cannot forge authorship).
func TestCore_ForgedAuthor(t *testing.T) {
	r := NewRouter(newFakeStore())
	ctx := WithIdentity(context.Background(), Identity{TenantID: "t1", UserID: "u1"})
	body, _ := json.Marshal(Command{CommandID: "f1", TenantID: "t1", ActorID: "u2", Type: "interaction.started", Medium: "chat"})
	if got := r.HandleCommand(ctx, "tenant.t1.interaction.iF.cmd", body); got.Status != "rejected" {
		t.Fatalf("forged actor must be rejected, got %+v", got)
	}
	// the authenticated user's own command is accepted
	ok, _ := json.Marshal(Command{CommandID: "f2", TenantID: "t1", ActorID: "u1", Type: "interaction.started", Medium: "chat"})
	if got := r.HandleCommand(ctx, "tenant.t1.interaction.iF.cmd", ok); got.Status != "accepted" {
		t.Fatalf("authenticated actor must be accepted, got %+v", got)
	}
}

// edit/delete must name the message they target (ref_id).
func TestCore_RefIDRequired(t *testing.T) {
	r := NewRouter(newFakeStore())
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", nil))
	noRef, _ := json.Marshal(Command{CommandID: "u1", TenantID: "t1", ActorID: "u1", Type: "message.updated", Medium: "chat"})
	if got := r.HandleCommand(context.Background(), subj, noRef); got.Status != "rejected" {
		t.Fatalf("message.updated without ref_id must be rejected, got %+v", got)
	}
	withRef, _ := json.Marshal(Command{CommandID: "u2", TenantID: "t1", ActorID: "u1", Type: "message.updated", Medium: "chat", RefID: "m1"})
	if got := r.HandleCommand(context.Background(), subj, withRef); got.Status != "accepted" {
		t.Fatalf("message.updated with ref_id should be accepted, got %+v", got)
	}
}

// a rejected command_id reused with a DIFFERENT payload is a conflict (key bound to
// its first request); the SAME payload may be retried once it becomes legal.
func TestCore_RejectedReuseConflict(t *testing.T) {
	r := NewRouter(newFakeStore())
	// message before start → rejected (and memoised with its payload hash)
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", map[string]any{"t": "A"})); got.Status != "rejected" {
		t.Fatalf("setup: want rejected, got %+v", got)
	}
	// same id, DIFFERENT payload → conflict
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", map[string]any{"t": "B"})); got.Status != "rejected" || got.Reason == "" {
		t.Fatalf("reuse with different payload must conflict, got %+v", got)
	}
	// same id, SAME payload, now legal (after start) → accepted (transient rejection retried)
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", nil))
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", map[string]any{"t": "A"})); got.Status != "accepted" {
		t.Fatalf("same-payload retry once legal should be accepted, got %+v", got)
	}
}
