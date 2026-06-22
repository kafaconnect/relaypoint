package signaling

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
)

// fakeStore is an in-memory LogStore — proves the router core needs no NATS.
type fakeStore struct {
	mu    sync.Mutex
	facts map[string][]*Event
	dedup map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{facts: map[string][]*Event{}, dedup: map[string]bool{}}
}

// streamSeq is the fake's per-subject STREAM sequence — the in-memory analogue of JetStream's
// per-subject last-sequence the router echoes for OCC. Append enforces it like the real store.
func (s *fakeStore) Append(_ context.Context, subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dedup[dedupID] {
		return true, nil
	}
	if uint64(len(s.facts[subject])) != expectedLastSubjSeq {
		return false, ErrOCCConflict
	}
	s.dedup[dedupID] = true
	e := &Event{}
	_ = proto.Unmarshal(data, e)
	s.facts[subject] = append(s.facts[subject], e)
	return false, nil
}

func (s *fakeStore) Replay(subject string) ([]*Event, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]*Event(nil), s.facts[subject]...)
	return out, uint64(len(out)), nil
}

// chatData marshals chat message text into the `data` payload (the registry: medium=chat).
func chatData(text string) []byte {
	if text == "" {
		return nil
	}
	b, _ := proto.Marshal(&ChatMessage{Text: text})
	return b
}

func cmd(id, tenant, typ, text string) []byte {
	b, _ := proto.Marshal(&Command{CommandId: id, TenantId: tenant, ActorId: "u1", Type: typ, Medium: "chat", Data: chatData(text)})
	return b
}

func callCmd(id, tenant, typ string) []byte {
	b, _ := proto.Marshal(&Command{CommandId: id, TenantId: tenant, ActorId: "u1", Type: typ, Medium: "call"})
	return b
}

// the .cmd subject carries the publisher identity suffix; cmd()'s actor is u1.
const subj = "tenant.t1.interaction.iX.cmd.u1"

func TestCore_NoNATS(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st, WithDevMode())

	if got := r.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", "")); got.Status != statusAccepted || got.CausedBy != "c1" {
		t.Fatalf("start: %+v (want accepted, caused_by=c1)", got)
	}
	if got := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "hi")); got.Status != statusAccepted {
		t.Fatalf("message: %+v", got)
	}
	if got := r.HandleCommand(context.Background(), subj, callCmd("c-call-ring", "t1", "call.ringing")); got.Status != statusAccepted {
		t.Fatalf("call ringing: %+v", got)
	}
	facts, _, _ := st.Replay(logSubjectFor("t1", "iX"))
	if len(facts) != 3 || facts[0].Sequence != 1 || facts[1].Sequence != 2 || facts[2].Sequence != 3 || facts[2].Medium != "call" {
		t.Fatalf("facts %+v want chat start/message plus call ringing", facts)
	}

	// idempotency: same command replayed → no second fact
	a := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "hi"))
	facts, _, _ = st.Replay(logSubjectFor("t1", "iX"))
	if a.Status != statusAccepted || len(facts) != 3 {
		t.Fatalf("retry double-appended: %+v / %d facts", a, len(facts))
	}
	// conflict: same id, different payload
	if got := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "DIFF")); got.Status != statusRejected {
		t.Fatalf("conflict not rejected: %+v", got)
	}
	// payload tenant mismatch
	if got := r.HandleCommand(context.Background(), subj, cmd("c3", "OTHER", "message.created", "")); got.Status != statusRejected {
		t.Fatalf("tenant mismatch not rejected: %+v", got)
	}
	// illegal: message before start on a fresh interaction
	if got := r.HandleCommand(context.Background(), "tenant.t1.interaction.iY.cmd.u1", cmd("c4", "t1", "message.created", "")); got.Status != statusRejected {
		t.Fatalf("illegal not rejected: %+v", got)
	}
}

// TestCore_RestartRebuild: a NEW router over the SAME store (a restart) continues the
// sequence and respects state — proving state is rebuilt from the durable log.
func TestCore_RestartRebuild(t *testing.T) {
	st := newFakeStore()
	r1 := NewRouter(st, WithDevMode())
	r1.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", ""))
	r1.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "a"))

	r2 := NewRouter(st, WithDevMode()) // restart: empty in-memory state
	got := r2.HandleCommand(context.Background(), subj, cmd("c3", "t1", "message.created", "b"))
	if got.Status != statusAccepted {
		t.Fatalf("post-restart message rejected (state not rebuilt): %+v", got)
	}
	facts, _, _ := st.Replay(logSubjectFor("t1", "iX"))
	if n := len(facts); n != 3 || facts[2].Sequence != 3 {
		t.Fatalf("post-restart sequence wrong: %d facts, last seq %d (want 3,3)", n, facts[len(facts)-1].Sequence)
	}
	// a replayed command from before the restart is recognised (no double-append)
	got = r2.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "a"))
	facts, _, _ = st.Replay(logSubjectFor("t1", "iX"))
	if got.Status != statusAccepted || len(facts) != 3 {
		t.Fatalf("post-restart replay double-appended: %+v / %d", got, len(facts))
	}
}

// @spec:signaling.cmd.forged-author-rejected
// With an authenticated Identity in context, a command whose actor_id differs from
// the authenticated user is rejected (the subject/payload cannot forge authorship).
func TestCore_ForgedAuthor(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	ctx := WithIdentity(context.Background(), Identity{TenantID: "t1", UserID: "u1"})
	body, _ := proto.Marshal(&Command{CommandId: "f1", TenantId: "t1", ActorId: "u2", Type: "interaction.started", Medium: "chat"})
	if got := r.HandleCommand(ctx, "tenant.t1.interaction.iF.cmd.u1", body); got.Status != statusRejected {
		t.Fatalf("forged actor must be rejected, got %+v", got)
	}
	// the authenticated user's own command is accepted
	ok, _ := proto.Marshal(&Command{CommandId: "f2", TenantId: "t1", ActorId: "u1", Type: "interaction.started", Medium: "chat"})
	if got := r.HandleCommand(ctx, "tenant.t1.interaction.iF.cmd.u1", ok); got.Status != statusAccepted {
		t.Fatalf("authenticated actor must be accepted, got %+v", got)
	}
}

// concurrentWriterStore models a fact appended by another writer between our getState and our
// Append: Replay omits it the first time (getState) then reveals it (the dup-path reconcile), and
// Append always reports a duplicate.
type concurrentWriterStore struct {
	base    []*Event
	hidden  *Event
	replays int
}

func (s *concurrentWriterStore) Append(context.Context, string, []byte, string, uint64) (bool, error) {
	return true, nil
}
func (s *concurrentWriterStore) Replay(string) ([]*Event, uint64, error) {
	s.replays++
	if s.replays == 1 {
		return append([]*Event(nil), s.base...), uint64(len(s.base)), nil
	}
	out := append(append([]*Event(nil), s.base...), s.hidden)
	return out, uint64(len(out)), nil
}

// The dup-append path must compare the COMMITTED fact's payload_hash: a divergent reuse is a
// conflict, a matching one replays — never a blind accepted.
func TestCore_DupPathChecksPayloadHash(t *testing.T) {
	started := &Event{Schema: SchemaV1, EventType: "interaction.started", EventId: "e1", Sequence: 1, TenantId: "t1", ActorId: "u1", Medium: "chat", CommandId: "c1", CausedBy: "c1"}
	origCmd := &Command{CommandId: "m1", TenantId: "t1", ActorId: "u1", Type: "message.created", Medium: "chat", Data: chatData("A")}
	m1 := &Event{Schema: SchemaV1, EventType: "message.created", EventId: "e2", Sequence: 2, TenantId: "t1", ActorId: "u1", Medium: "chat", CommandId: "m1", PayloadHash: hashPayload(origCmd), CausedBy: "m1"}

	r1 := NewRouter(&concurrentWriterStore{base: []*Event{started}, hidden: m1}, WithDevMode())
	if got := r1.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "B")); got.Status != statusRejected || got.Reason == "" {
		t.Fatalf("dup-path divergent reuse must conflict, got %+v", got)
	}
	r2 := NewRouter(&concurrentWriterStore{base: []*Event{started}, hidden: m1}, WithDevMode())
	if got := r2.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "A")); got.Status != statusAccepted {
		t.Fatalf("dup-path matching reuse must replay accepted, got %+v", got)
	}
}

// After a restart, conflict detection still works because the fact carries payload_hash:
// a reused command_id with a DIFFERENT payload conflicts; the SAME payload replays accepted.
func TestCore_ConflictAcrossRestart(t *testing.T) {
	st := newFakeStore()
	r1 := NewRouter(st, WithDevMode())
	r1.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", ""))
	r1.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "A"))

	r2 := NewRouter(st, WithDevMode()) // restart: in-memory state gone, rebuilt from the log
	if got := r2.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "DIFF")); got.Status != statusRejected {
		t.Fatalf("cross-restart divergent command_id reuse must conflict, got %+v", got)
	}
	if got := r2.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "A")); got.Status != statusAccepted {
		t.Fatalf("cross-restart same-payload retry must replay accepted, got %+v", got)
	}
}

// edit/delete must name the message they target (ref_id).
func TestCore_RefIDRequired(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", ""))
	noRef, _ := proto.Marshal(&Command{CommandId: "u1", TenantId: "t1", ActorId: "u1", Type: "message.updated", Medium: "chat"})
	if got := r.HandleCommand(context.Background(), subj, noRef); got.Status != statusRejected {
		t.Fatalf("message.updated without ref_id must be rejected, got %+v", got)
	}
	withRef, _ := proto.Marshal(&Command{CommandId: "u2", TenantId: "t1", ActorId: "u1", Type: "message.updated", Medium: "chat", RefId: "m1"})
	if got := r.HandleCommand(context.Background(), subj, withRef); got.Status != statusAccepted {
		t.Fatalf("message.updated with ref_id should be accepted, got %+v", got)
	}
}

// dupRebuildFailStore: the first Replay (getState build) succeeds empty; the append reports a
// duplicate; the dup-path rebuild then fails — the router must NOT keep stale in-memory seq.
type dupRebuildFailStore struct{ calls int }

func (s *dupRebuildFailStore) Append(context.Context, string, []byte, string, uint64) (bool, error) {
	return true, nil
}
func (s *dupRebuildFailStore) Replay(string) ([]*Event, uint64, error) {
	s.calls++
	if s.calls == 1 {
		return nil, 0, nil
	}
	return nil, 0, errors.New("replay down")
}

// On a duplicate append whose reconciling rebuild fails, the interaction is evicted so the next
// command rebuilds from the log instead of appending behind an untrustworthy sequence.
func TestCore_DupRebuildFailEvicts(t *testing.T) {
	r := NewRouter(&dupRebuildFailStore{}, WithDevMode())
	got := r.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", ""))
	if got.Status != statusAccepted {
		t.Fatalf("dup append should still ack accepted, got %+v", got)
	}
	r.mu.Lock()
	n := len(r.inter)
	r.mu.Unlock()
	if n != 0 {
		t.Fatalf("interaction not evicted after dup+rebuild-fail: %d cached", n)
	}
}

// occBeforeDedupStore models a single-server (R1) JetStream where the expected-subject (OCC)
// check runs BEFORE Nats-Msg-Id dedup. The initial fold (getState) is one sequence behind the
// true tail — it omits the already-committed command_id — so the router believes it must append;
// the Append then comes back as ErrOCCConflict (not duplicate=true), and the re-fold reveals the
// committed fact. Append MUST be called at most once: the router satisfies the retry from its
// re-fold, never a second append.
type occBeforeDedupStore struct {
	base    []*Event // visible on the stale first fold
	hidden  *Event   // the already-committed command_id, revealed only after the OCC conflict
	replays int
	appends int
}

func (s *occBeforeDedupStore) Append(context.Context, string, []byte, string, uint64) (bool, error) {
	s.appends++
	return false, ErrOCCConflict
}
func (s *occBeforeDedupStore) Replay(string) ([]*Event, uint64, error) {
	s.replays++
	if s.replays == 1 {
		return append([]*Event(nil), s.base...), uint64(len(s.base)), nil
	}
	out := append(append([]*Event(nil), s.base...), s.hidden)
	return out, uint64(len(out)), nil
}

// On an R1 broker the OCC check precedes dedup, so a retry of an already-committed command_id
// surfaces as ErrOCCConflict. The router MUST re-fold, recognise the committed command_id, and
// replay the original cached accepted result (same caused_by) — not a spurious rejection — and
// MUST NOT append a second fact.
func TestCore_OCCBeforeDedupReplaysAccepted(t *testing.T) {
	started := &Event{Schema: SchemaV1, EventType: "interaction.started", EventId: "e1", Sequence: 1, TenantId: "t1", ActorId: "u1", Medium: "chat", CommandId: "c1", CausedBy: "c1"}
	origCmd := &Command{CommandId: "m1", TenantId: "t1", ActorId: "u1", Type: "message.created", Medium: "chat", Data: chatData("A")}
	committed := &Event{Schema: SchemaV1, EventType: "message.created", EventId: "e2", Sequence: 2, TenantId: "t1", ActorId: "u1", Medium: "chat", CommandId: "m1", PayloadHash: hashPayload(origCmd), CausedBy: "m1"}

	st := &occBeforeDedupStore{base: []*Event{started}, hidden: committed}
	r := NewRouter(st, WithDevMode())

	got := r.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "A"))
	if got.Status != statusAccepted {
		t.Fatalf("OCC-before-dedup retry must replay accepted, got %+v", got)
	}
	if got.CausedBy != "m1" {
		t.Fatalf("replayed result must keep original caused_by m1, got %q", got.CausedBy)
	}
	if st.appends != 1 {
		t.Fatalf("router must append no second fact (re-fold satisfies the retry): %d appends", st.appends)
	}
}

// a rejected command_id reused with a DIFFERENT payload is a conflict (key bound to
// its first request); the SAME payload may be retried once it becomes legal.
func TestCore_RejectedReuseConflict(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	// message before start → rejected (and memoised with its payload hash)
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", "A")); got.Status != statusRejected {
		t.Fatalf("setup: want rejected, got %+v", got)
	}
	// same id, DIFFERENT payload → conflict
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", "B")); got.Status != statusRejected || got.Reason == "" {
		t.Fatalf("reuse with different payload must conflict, got %+v", got)
	}
	// same id, SAME payload, now legal (after start) → accepted (transient rejection retried)
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", ""))
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", "A")); got.Status != statusAccepted {
		t.Fatalf("same-payload retry once legal should be accepted, got %+v", got)
	}
}
