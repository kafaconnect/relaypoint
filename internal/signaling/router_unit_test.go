package signaling

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

type fakeStore struct {
	mu        sync.Mutex
	facts     map[string][]*Event
	dedup     map[string]bool
	streamSeq uint64
	lastSeq   map[string]uint64
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
	s.streamSeq++
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
	return out, s.lastSeq[subject], nil
}

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

	a := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "hi"))
	facts, _, _ = st.Replay(logSubjectFor("t1", "iX"))
	if a.Status != statusAccepted || len(facts) != 4 {
		t.Fatalf("retry double-appended: %+v / %d facts", a, len(facts))
	}
	if got := r.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "DIFF")); got.Status != statusRejected {
		t.Fatalf("conflict not rejected: %+v", got)
	}
	if got := r.HandleCommand(context.Background(), subj, cmd("c3", "OTHER", "message.created", "")); got.Status != statusRejected {
		t.Fatalf("tenant mismatch not rejected: %+v", got)
	}
	if got := r.HandleCommand(context.Background(), "tenant.t1.interaction.iY.cmd.u1", cmd("c4", "t1", "message.created", "")); got.Status != statusRejected {
		t.Fatalf("illegal not rejected: %+v", got)
	}
}

func TestCore_RestartRebuild(t *testing.T) {
	st := newFakeStore()
	r1 := NewRouter(st, WithDevMode())
	r1.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", ""))
	r1.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "a"))

	r2 := NewRouter(st, WithDevMode())
	got := r2.HandleCommand(context.Background(), subj, cmd("c3", "t1", "message.created", "b"))
	if got.Status != statusAccepted {
		t.Fatalf("post-restart message rejected (state not rebuilt): %+v", got)
	}
	facts, _, _ := st.Replay(logSubjectFor("t1", "iX"))
	if n := len(facts); n != 3 || facts[2].Sequence != 3 {
		t.Fatalf("post-restart sequence wrong: %d facts, last seq %d (want 3,3)", n, facts[len(facts)-1].Sequence)
	}
	got = r2.HandleCommand(context.Background(), subj, cmd("c2", "t1", "message.created", "a"))
	facts, _, _ = st.Replay(logSubjectFor("t1", "iX"))
	if got.Status != statusAccepted || len(facts) != 3 {
		t.Fatalf("post-restart replay double-appended: %+v / %d", got, len(facts))
	}
}

// @spec:signaling.cmd.forged-author-rejected
func TestCore_ForgedAuthor(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	ctx := WithIdentity(context.Background(), Identity{TenantID: "t1", UserID: "u1"})
	body, _ := proto.Marshal(&Command{CommandId: "f1", TenantId: "t1", ActorId: "u2", Type: "interaction.started", Medium: "chat"})
	if got := r.HandleCommand(ctx, "tenant.t1.interaction.iF.cmd.u1", body); got.Status != statusRejected {
		t.Fatalf("forged actor must be rejected, got %+v", got)
	}
	ok, _ := proto.Marshal(&Command{CommandId: "f2", TenantId: "t1", ActorId: "u1", Type: "interaction.started", Medium: "chat"})
	if got := r.HandleCommand(ctx, "tenant.t1.interaction.iF.cmd.u1", ok); got.Status != statusAccepted {
		t.Fatalf("authenticated actor must be accepted, got %+v", got)
	}
}

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

func TestCore_ConflictAcrossRestart(t *testing.T) {
	st := newFakeStore()
	r1 := NewRouter(st, WithDevMode())
	r1.HandleCommand(context.Background(), subj, cmd("c1", "t1", "interaction.started", ""))
	r1.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "A"))

	r2 := NewRouter(st, WithDevMode())
	if got := r2.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "DIFF")); got.Status != statusRejected {
		t.Fatalf("cross-restart divergent command_id reuse must conflict, got %+v", got)
	}
	if got := r2.HandleCommand(context.Background(), subj, cmd("m1", "t1", "message.created", "A")); got.Status != statusAccepted {
		t.Fatalf("cross-restart same-payload retry must replay accepted, got %+v", got)
	}
}

func TestCore_RefIDNotGatedByRP(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", ""))
	noRef, _ := proto.Marshal(&Command{CommandId: "u1", TenantId: "t1", ActorId: "u1", Type: "message.updated", Medium: "chat"})
	if got := r.HandleCommand(context.Background(), subj, noRef); got.Status != statusAccepted {
		t.Fatalf("RP must not gate on ref_id (a chat-domain rule); message.updated should be accepted, got %+v", got)
	}
}

func TestCore_NovelVerbIsOpaqueAnnotation(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
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
	r.HandleCommand(context.Background(), subj, cmd("e1", "t1", "interaction.ended", ""))
	if got := r.HandleCommand(context.Background(), subj, cmd("r3", "t1", "routing.offered", "Z")); got.Status != statusRejected {
		t.Fatalf("routing.offered after interaction.ended must be rejected, got %+v", got)
	}
}

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

func TestCore_RejectedReuseConflict(t *testing.T) {
	r := NewRouter(newFakeStore(), WithDevMode())
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", "A")); got.Status != statusRejected {
		t.Fatalf("setup: want rejected, got %+v", got)
	}
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", "B")); got.Status != statusRejected || got.Reason == "" {
		t.Fatalf("reuse with different payload must conflict, got %+v", got)
	}
	r.HandleCommand(context.Background(), subj, cmd("s1", "t1", "interaction.started", ""))
	if got := r.HandleCommand(context.Background(), subj, cmd("k1", "t1", "message.created", "A")); got.Status != statusAccepted {
		t.Fatalf("same-payload retry once legal should be accepted, got %+v", got)
	}
}

// @spec:router.occ.committed-stream-seq
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

// @spec:router.state.idle-evict
func TestCore_IdleEvictionRebuildsOnNextAccess(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	clock := func() time.Time { return now }
	st := newFakeStore()
	r := NewRouter(st, WithDevMode(), WithClock(clock), WithStateLimits(1024, 8, time.Minute))
	subjA := "tenant.t1.interaction.A.cmd.u1"
	subjB := "tenant.t1.interaction.B.cmd.u1"

	if got := r.HandleCommand(context.Background(), subjA, cmd("a0", "t1", "interaction.started", "")); got.Status != statusAccepted {
		t.Fatalf("A start: %+v", got)
	}
	if got := r.HandleCommand(context.Background(), subjA, cmd("a1", "t1", "message.created", "hi")); got.Status != statusAccepted {
		t.Fatalf("A msg: %+v", got)
	}
	r.mu.Lock()
	_, cachedBefore := r.inter["t1/A"]
	r.mu.Unlock()
	if !cachedBefore {
		t.Fatal("A should be cached after access")
	}

	now = now.Add(2 * time.Minute) // A is now idle past the 1m TTL
	if got := r.HandleCommand(context.Background(), subjB, cmd("b0", "t1", "interaction.started", "")); got.Status != statusAccepted {
		t.Fatalf("B start: %+v", got) // any insert triggers the idle sweep
	}
	r.mu.Lock()
	_, cachedAfter := r.inter["t1/A"]
	r.mu.Unlock()
	if cachedAfter {
		t.Fatal("idle A must be evicted once it is idle past the TTL")
	}

	// transparent rebuild-on-next-access: a retry of a1 replays accepted (dedup rebuilt from the log)
	if got := r.HandleCommand(context.Background(), subjA, cmd("a1", "t1", "message.created", "hi")); got.Status != statusAccepted {
		t.Fatalf("rebuilt A retry must replay accepted, got %+v", got)
	}
	// a fresh command lands at the next dense sequence, proving seq was rebuilt from the log
	if got := r.HandleCommand(context.Background(), subjA, cmd("a2", "t1", "message.created", "yo")); got.Status != statusAccepted {
		t.Fatalf("rebuilt A new message rejected: %+v", got)
	}
	facts, _, _ := st.Replay(logSubjectFor("t1", "A"))
	if len(facts) != 3 || facts[2].Sequence != 3 {
		t.Fatalf("rebuilt-from-log state wrong: %d facts, last seq %d (want 3,3)", len(facts), facts[len(facts)-1].Sequence)
	}
}

// @spec:router.state.idle-evict
func TestCore_LRUCapAndResultsBounded(t *testing.T) {
	st := newFakeStore()
	r := NewRouter(st, WithDevMode(), WithStateLimits(1, 4, time.Hour))
	subjA := "tenant.t1.interaction.A.cmd.u1"
	subjB := "tenant.t1.interaction.B.cmd.u1"

	if got := r.HandleCommand(context.Background(), subjA, cmd("a0", "t1", "interaction.started", "")); got.Status != statusAccepted {
		t.Fatalf("A start: %+v", got)
	}
	for i := 1; i <= 8; i++ {
		if got := r.HandleCommand(context.Background(), subjA, cmd(fmt.Sprintf("a%d", i), "t1", "message.created", fmt.Sprintf("m%d", i))); got.Status != statusAccepted {
			t.Fatalf("A msg %d: %+v", i, got)
		}
	}
	r.mu.Lock()
	sa := r.inter["t1/A"]
	r.mu.Unlock()
	sa.mu.Lock()
	nres := len(sa.results)
	sa.mu.Unlock()
	if nres > 4 {
		t.Fatalf("per-interaction results cache unbounded: %d entries (cap 4)", nres)
	}

	// opening B evicts A under the maxInter=1 LRU cap
	if got := r.HandleCommand(context.Background(), subjB, cmd("b0", "t1", "interaction.started", "")); got.Status != statusAccepted {
		t.Fatalf("B start: %+v", got)
	}
	r.mu.Lock()
	_, aCached := r.inter["t1/A"]
	n := len(r.inter)
	r.mu.Unlock()
	if aCached || n != 1 {
		t.Fatalf("A not LRU-evicted under the cap: cached=%v size=%d (want false,1)", aCached, n)
	}

	// a retry of an OLD (results-evicted) command_id must still NOT double-append: the broker dedups
	// and the log rebuild yields accepted — eviction is safe.
	before, _, _ := st.Replay(logSubjectFor("t1", "A"))
	if got := r.HandleCommand(context.Background(), subjA, cmd("a1", "t1", "message.created", "m1")); got.Status != statusAccepted {
		t.Fatalf("evicted-id retry must replay accepted, got %+v", got)
	}
	after, _, _ := st.Replay(logSubjectFor("t1", "A"))
	if len(after) != len(before) {
		t.Fatalf("evicted-id retry double-appended: before=%d after=%d", len(before), len(after))
	}
}

// TestCopyMissingResultsPreservesFIFO asserts a re-fold copy lands missing dedup entries in fresh's
// chronological (resultOrder) order, not random map order — so st.resultOrder stays a true FIFO and the
// bounded cache evicts the OLDEST, never a younger entry early (cross-review FIX 4).
func TestCopyMissingResultsPreservesFIFO(t *testing.T) {
	mk := func(ids ...string) *interactionState {
		st := &interactionState{results: map[string]storedResult{}}
		for _, id := range ids {
			st.putResult(id, storedResult{result: &CommandResult{CommandId: id}}, 0)
		}
		return st
	}
	// fresh holds c1..c5 chronologically; a stray id sits in resultOrder but not results (defensive guard).
	fresh := mk("c1", "c2", "c3", "c4", "c5")
	fresh.resultOrder = append(fresh.resultOrder, "ghost")
	// st already has c2; it is missing c1,c3,c4,c5 and must receive them in fresh's chronological order.
	st := mk("c2")
	st.copyMissingResults(fresh, 0)

	want := []string{"c2", "c1", "c3", "c4", "c5"}
	if len(st.resultOrder) != len(want) {
		t.Fatalf("resultOrder = %v, want %v", st.resultOrder, want)
	}
	for i := range want {
		if st.resultOrder[i] != want[i] {
			t.Fatalf("resultOrder = %v, want %v (chronological FIFO copy)", st.resultOrder, want)
		}
	}
	if _, ok := st.results["ghost"]; ok {
		t.Fatal("ghost id (in resultOrder, absent from results) must be skipped, not copied")
	}
}
