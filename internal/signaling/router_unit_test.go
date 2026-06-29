package signaling

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
)

// fakeStore is an in-memory LogStore — proves the router core needs no NATS. It models the SHARED
// INTERACTION_LOGS stream: ONE global stream sequence advanced by a commit on ANY subject, plus each
// subject's last committed global seq as its OCC token (exactly JetStream's
// ExpectLastSequencePerSubject semantics). A per-subject COUNT would hide RH-01 — the spurious
// conflict only appears once an interleaving subject advances the shared seq by more than one.
type fakeStore struct {
	mu        sync.Mutex
	facts     map[string][]*Event
	dedup     map[string]bool
	streamSeq uint64            // the one shared stream's global last sequence
	lastSeq   map[string]uint64 // subject -> its last committed global seq (the OCC token)
}

func newFakeStore() *fakeStore {
	return &fakeStore{facts: map[string][]*Event{}, dedup: map[string]bool{}, lastSeq: map[string]uint64{}}
}

func (s *fakeStore) Append(_ context.Context, subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (bool, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dedup[dedupID] {
		return true, s.lastSeq[subject], nil
	}
	if s.lastSeq[subject] != expectedLastSubjSeq {
		return false, 0, ErrOCCConflict
	}
	s.dedup[dedupID] = true
	s.streamSeq++ // a commit on ANY subject advances the one shared stream sequence
	s.lastSeq[subject] = s.streamSeq
	e := &Event{}
	_ = proto.Unmarshal(data, e)
	s.facts[subject] = append(s.facts[subject], e)
	return false, s.streamSeq, nil
}

func (s *fakeStore) Replay(subject string) ([]*Event, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]*Event(nil), s.facts[subject]...)
	return out, s.lastSeq[subject], nil // the subject's last GLOBAL seq, like meta.Sequence.Stream
}

// countingStore wraps a LogStore and tallies appends the broker rejected with ErrOCCConflict — i.e.
// how often the router presented a stale OCC token. With the RH-01 fix, distinct interactions
// interleaving on the shared stream present exact (broker-committed) tokens, so this stays 0.
type countingStore struct {
	LogStore
	mu        sync.Mutex
	conflicts int
}

func (s *countingStore) Append(ctx context.Context, subject string, data []byte, dedupID string, exp uint64) (bool, uint64, error) {
	dup, seq, err := s.LogStore.Append(ctx, subject, data, dedupID, exp)
	if errors.Is(err, ErrOCCConflict) {
		s.mu.Lock()
		s.conflicts++
		s.mu.Unlock()
	}
	return dup, seq, err
}

func (s *countingStore) occConflicts() int { s.mu.Lock(); defer s.mu.Unlock(); return s.conflicts }

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

// @spec:web-call.lifecycle-ringing-active-ended
// @spec:web-call.audio-upgrades-to-video
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
	if got := r.HandleCommand(context.Background(), subj, callCmd("c-call-upgrade", "t1", "call.upgrade_video")); got.Status != statusAccepted {
		t.Fatalf("call upgrade: %+v", got)
	}
	facts, _, _ := st.Replay(logSubjectFor("t1", "iX"))
	if len(facts) != 4 || facts[0].Sequence != 1 || facts[1].Sequence != 2 || facts[2].Sequence != 3 || facts[3].Sequence != 4 || facts[2].Medium != "call" || facts[3].Medium != "call" {
		t.Fatalf("facts %+v want chat start/message plus call ringing and upgrade", facts)
	}

	// idempotency: same command replayed → no second fact
	a := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "hi"))
	facts, _, _ = st.Replay(logSubjectFor("t1", "iX"))
	if a.Status != statusAccepted || len(facts) != 4 {
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

func (s *concurrentWriterStore) Append(context.Context, string, []byte, string, uint64) (bool, uint64, error) {
	return true, 0, nil
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

// RP no longer enforces message edit/delete referential integrity (ref_id) — that is chat-domain
// knowledge owned by the producer (Desk). RP gates on delivery STRUCTURE only, so message.updated
// is an opaque annotation accepted on a started interaction with or without ref_id.
func TestCore_RefIDNotGatedByRP(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", ""))
	noRef, _ := proto.Marshal(&Command{CommandId: "u1", TenantId: "t1", ActorId: "u1", Type: "message.updated", Medium: "chat"})
	if got := r.HandleCommand(context.Background(), subj, noRef); got.Status != statusAccepted {
		t.Fatalf("RP must not gate on ref_id (a chat-domain rule); message.updated should be accepted, got %+v", got)
	}
}

// A novel domain verb (routing.*, emitted by desk-router) is accepted as an opaque annotation on a
// started interaction with ZERO RelayPoint change — the generic structural gate that replaced the
// closed cmdType enum. Regression for the live "illegal transition routing.offered" rejection.
func TestCore_NovelVerbIsOpaqueAnnotation(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	// The lifecycle gate still holds for new verbs: no annotation before the interaction is started.
	if got := r.HandleCommand(context.Background(), subj, cmd("r0", "t1", "routing.offered", "X")); got.Status != statusRejected {
		t.Fatalf("routing.offered before interaction.started must be rejected, got %+v", got)
	}
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", ""))
	if got := r.HandleCommand(context.Background(), subj, cmd("r1", "t1", "routing.offered", "X")); got.Status != statusAccepted {
		t.Fatalf("routing.offered on a started interaction must be accepted, got %+v", got)
	}
	if got := r.HandleCommand(context.Background(), subj, cmd("r2", "t1", "routing.no_candidates", "Y")); got.Status != statusAccepted {
		t.Fatalf("routing.no_candidates on a started interaction must be accepted, got %+v", got)
	}
	// Upper lifecycle bound: once the interaction is ended, the same opaque annotation is rejected.
	r.HandleCommand(context.Background(), subj, cmd("e1", "t1", "interaction.ended", ""))
	if got := r.HandleCommand(context.Background(), subj, cmd("r3", "t1", "routing.offered", "Z")); got.Status != statusRejected {
		t.Fatalf("routing.offered after interaction.ended must be rejected, got %+v", got)
	}
}

// dupRebuildFailStore: the first Replay (getState build) succeeds empty; the append reports a
// duplicate; the dup-path rebuild then fails — the router must NOT keep stale in-memory seq.
type dupRebuildFailStore struct{ calls int }

func (s *dupRebuildFailStore) Append(context.Context, string, []byte, string, uint64) (bool, uint64, error) {
	return true, 0, nil
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

func (s *occBeforeDedupStore) Append(context.Context, string, []byte, string, uint64) (bool, uint64, error) {
	s.appends++
	return false, 0, ErrOCCConflict
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

// @spec:router.occ.committed-stream-seq
// Two distinct interactions A and B append ALTERNATELY on the SHARED INTERACTION_LOGS stream, so the
// global stream sequence on each subject advances by more than one between that subject's own
// appends. Each clean append must record the broker-committed stream seq as its next OCC token, not
// prev+1 — otherwise the ++ guess is stale and ~every append after the first raises a SPURIOUS
// ErrOCCConflict. Asserts zero broker conflicts on correctly-folded appends and a dense, gapless
// per-interaction sequence.
func TestCore_InterleavedSharedStreamNoSpuriousOCC(t *testing.T) {
	cs := &countingStore{LogStore: newFakeStore()}
	r := NewRouter(cs, WithDevMode())
	subjA := "tenant.t1.interaction.A.cmd.u1"
	subjB := "tenant.t1.interaction.B.cmd.u1"

	mustAccept := func(s, id, typ, text string) {
		t.Helper()
		if got := r.HandleCommand(context.Background(), s, cmd(id, "t1", typ, text)); got.Status != statusAccepted {
			t.Fatalf("%s %s: %+v (want accepted)", s, id, got)
		}
	}
	mustAccept(subjA, "a0", "interaction.started", "")
	mustAccept(subjB, "b0", "interaction.started", "")
	for i := 1; i <= 5; i++ {
		mustAccept(subjA, fmt.Sprintf("a%d", i), "message.created", fmt.Sprintf("a-%d", i))
		mustAccept(subjB, fmt.Sprintf("b%d", i), "message.created", fmt.Sprintf("b-%d", i))
	}
	if n := cs.occConflicts(); n != 0 {
		t.Fatalf("stale ++-guessed OCC token caused %d spurious broker conflict(s) on interleaved interactions; want 0", n)
	}
	fake := cs.LogStore.(*fakeStore)
	for _, iid := range []string{"A", "B"} {
		facts, _, _ := fake.Replay(logSubjectFor("t1", iid))
		if len(facts) != 6 {
			t.Fatalf("interaction %s: want 6 facts, got %d", iid, len(facts))
		}
		for i, f := range facts {
			if f.Sequence != int64(i+1) {
				t.Fatalf("interaction %s: dense per-interaction sequence broke at index %d: seq=%d", iid, i, f.Sequence)
			}
		}
	}
}

// staleTokenRaceStore commits ONE genuine competing fact the first time a real append arrives on
// `subject` carrying `triggerSeq` (the token a correctly-folded router presents). Combined with an
// interleaving interaction that left a ++-guessing router's token stale, this is the exact RH-01
// failure: the stale token spuriously conflicts on attempt 0, so the genuine race lands on attempt 1
// with no retry budget left → wrongly rejected; under the fix attempt 0 IS the genuine race and the
// single retry arbitrates it → accepted.
type staleTokenRaceStore struct {
	*fakeStore
	subject    string
	triggerSeq uint64
	racer      *Event
	injected   bool
}

func (s *staleTokenRaceStore) Append(ctx context.Context, subject string, data []byte, dedupID string, exp uint64) (bool, uint64, error) {
	if !s.injected && subject == s.subject && exp == s.triggerSeq {
		s.injected = true
		raw, _ := proto.Marshal(s.racer)
		if _, _, err := s.fakeStore.Append(ctx, subject, raw, "racer-"+dedupID, exp); err != nil {
			return false, 0, err
		}
	}
	return s.fakeStore.Append(ctx, subject, data, dedupID, exp)
}

// @spec:router.occ.committed-stream-seq
// The single retry budget must remain available to arbitrate a GENUINE same-subject race. A's start
// advances the shared stream, so B's correct OCC token after its OWN start is 2, not 1. When a
// genuine concurrent writer commits to B at that token, a correctly-folded router spends its one
// retry on the real race and accepts; the ++-guessing router instead wastes the retry on a spurious
// staleness conflict first, then wrongly rejects the genuine race as "lost concurrent append".
func TestCore_StaleTokenDoesNotBurnRetryBudget(t *testing.T) {
	racer := &Event{Schema: SchemaV1, EventType: "message.created", EventId: "racer-ev", Sequence: 2, TenantId: "t1", ActorId: "u1", Medium: "chat", CommandId: "racer", CausedBy: "racer"}
	st := &staleTokenRaceStore{fakeStore: newFakeStore(), subject: logSubjectFor("t1", "B"), triggerSeq: 2, racer: racer}
	r := NewRouter(st, WithDevMode())
	subjA := "tenant.t1.interaction.A.cmd.u1"
	subjB := "tenant.t1.interaction.B.cmd.u1"

	if got := r.HandleCommand(context.Background(), subjA, cmd("a0", "t1", "interaction.started", "")); got.Status != statusAccepted {
		t.Fatalf("A start: %+v", got)
	}
	if got := r.HandleCommand(context.Background(), subjB, cmd("b0", "t1", "interaction.started", "")); got.Status != statusAccepted {
		t.Fatalf("B start: %+v", got)
	}
	if got := r.HandleCommand(context.Background(), subjB, cmd("b1", "t1", "message.created", "b-1")); got.Status != statusAccepted {
		t.Fatalf("genuine same-subject race wrongly rejected — the stale ++ token burned the retry budget: %+v", got)
	}
	facts, _, _ := st.Replay(logSubjectFor("t1", "B"))
	if len(facts) != 3 {
		t.Fatalf("B want 3 facts (start, racer, b1), got %d", len(facts))
	}
	for i, f := range facts {
		if f.Sequence != int64(i+1) {
			t.Fatalf("B dense per-interaction sequence broke at index %d: seq=%d", i, f.Sequence)
		}
	}
}
