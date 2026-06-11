package projector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// --- in-memory fakes (no live NATS — loose-coupling HARD RULE) ---

type fakeSource struct {
	facts   []Fact
	next    int
	floor   uint64
	deliver map[uint64]int // streamSeq -> delivery count
	acked   []uint64
	naked   []uint64
}

func newFakeSource(facts ...Fact) *fakeSource {
	return &fakeSource{facts: facts, deliver: map[uint64]int{}}
}

func (s *fakeSource) Deliver(ctx context.Context) (Fact, error) {
	if s.next >= len(s.facts) {
		return Fact{}, context.Canceled
	}
	f := s.facts[s.next]
	s.next++
	s.deliver[f.StreamSeq]++
	return f, nil
}

func (s *fakeSource) Ack(f Fact) error {
	s.acked = append(s.acked, f.StreamSeq)
	if f.StreamSeq > s.floor {
		s.floor = f.StreamSeq
	}
	return nil
}

func (s *fakeSource) Nak(f Fact) error {
	s.naked = append(s.naked, f.StreamSeq)
	// redeliver: rewind so the same fact is delivered again on the next Deliver.
	s.next--
	return nil
}

func (s *fakeSource) Delivered(f Fact) int                     { return s.deliver[f.StreamSeq] }
func (s *fakeSource) AckFloor(context.Context) (uint64, error) { return s.floor, nil }

func (s *fakeSource) FoldRange(_ context.Context, lo, hi uint64) ([]Fact, error) {
	var out []Fact
	for _, f := range s.facts {
		if f.StreamSeq > lo && f.StreamSeq <= hi {
			out = append(out, f)
		}
	}
	return out, nil
}

type pub struct {
	tenant, agent, iid, dedup string
	payload                   []byte
}

type fakeSink struct {
	mu      sync.Mutex
	pubs    []pub
	seen    map[string]bool // dedup id -> stored (at-most-once)
	dlq     []string
	failFor map[string]int // dedup id -> remaining forced failures
}

func newFakeSink() *fakeSink { return &fakeSink{seen: map[string]bool{}, failFor: map[string]int{}} }

func (s *fakeSink) Publish(_ context.Context, tenant, agent, iid, dedupID string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.failFor[dedupID]; n > 0 {
		s.failFor[dedupID] = n - 1
		return errors.New("forced publish failure")
	}
	if s.seen[dedupID] {
		return nil // dedup: the feed stores it at most once
	}
	s.seen[dedupID] = true
	s.pubs = append(s.pubs, pub{tenant, agent, iid, dedupID, payload})
	return nil
}

func (s *fakeSink) Dlq(_ context.Context, tenant, reason, eventID string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlq = append(s.dlq, fmt.Sprintf("%s/%s/%s/%d", tenant, reason, eventID, seq))
	return nil
}

func (s *fakeSink) feedsFor(agent, iid string) []pub {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []pub
	for _, p := range s.pubs {
		if p.agent == agent && p.iid == iid {
			out = append(out, p)
		}
	}
	return out
}

type fakeLease struct{}

func (fakeLease) Acquire(context.Context) error { return nil }
func (fakeLease) Renew(context.Context) error   { return nil }
func (fakeLease) Release(context.Context) error { return nil }

type fakeSnaps struct {
	mu    sync.Mutex
	saved map[uint64]*Snapshot
}

func newFakeSnaps() *fakeSnaps { return &fakeSnaps{saved: map[uint64]*Snapshot{}} }

func (s *fakeSnaps) Save(_ context.Context, seq uint64, snap *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved[seq] = snap
	return nil
}

func (s *fakeSnaps) Load(_ context.Context, maxSeq uint64) (*Snapshot, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best uint64
	var bestSnap *Snapshot
	for seq, snap := range s.saved {
		if seq <= maxSeq && seq >= best {
			best, bestSnap = seq, snap
		}
	}
	return bestSnap, best, nil
}

// --- helpers ---

const tn = "t1"

func fact(streamSeq uint64, iid string, seq int64, typ, actor string) Fact {
	return NewFact(&signaling.Event{
		Schema: signaling.SchemaV1, TenantId: tn, EventType: typ, ActorId: actor, Sequence: seq,
		EventId: fmt.Sprintf("ev-%d", streamSeq),
	}, iid, streamSeq)
}

func runAll(t *testing.T, src *fakeSource, sink *fakeSink, snaps *fakeSnaps, cfg Config) *Projector {
	t.Helper()
	if snaps == nil {
		snaps = newFakeSnaps()
	}
	p := New(src, sink, fakeLease{}, snaps, cfg)
	// Run drains the fake source then returns context.Canceled when facts are exhausted.
	err := p.Run(context.Background())
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	return p
}

// @spec:signaling.feed.fanout-to-participants
// A fact for an interaction with two participating agents lands verbatim in BOTH feeds; a
// non-participant's feed gets nothing.
func TestFanoutToParticipantsOnly(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "participant.joined", "bob"),
		fact(4, "I", 4, "message.created", "u1"),
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})

	if got := len(sink.feedsFor("alice", "I")); got != 3 { // joined(alice), joined(bob), message
		t.Fatalf("alice feed = %d facts, want 3", got)
	}
	if got := len(sink.feedsFor("bob", "I")); got != 2 { // joined(bob), message
		t.Fatalf("bob feed = %d facts, want 2", got)
	}
	if got := len(sink.feedsFor("carol", "I")); got != 0 {
		t.Fatalf("carol (non-participant) feed = %d, want 0", got)
	}

	// verbatim: the projected payload decodes to the same Event (sequence + event_id preserved).
	msg := sink.feedsFor("bob", "I")[1] // the message.created at sequence 4
	e := &signaling.Event{}
	if err := proto.Unmarshal(msg.payload, e); err != nil {
		t.Fatalf("decode projection: %v", err)
	}
	if e.Sequence != 4 || e.EventType != "message.created" || e.EventId != "ev-4" {
		t.Fatalf("projection not verbatim: %+v", e)
	}
}

// Tenant-wide dev/test shortcut: with TenantWideAgents set, every fact of the tenant reaches the
// configured agents' feeds regardless of participation (no participant.joined at all here), and a
// non-listed agent gets nothing.
func TestTenantWideFanoutShortcut(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "message.created", "u1"),
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{TenantWideAgents: map[string][]string{tn: {"agent1"}}})

	if got := len(sink.feedsFor("agent1", "I")); got != 2 { // started + message, no join needed
		t.Fatalf("agent1 feed = %d facts, want 2 (tenant-wide ignores participation)", got)
	}
	if got := len(sink.feedsFor("bob", "I")); got != 0 {
		t.Fatalf("bob (not in roster) feed = %d, want 0", got)
	}
}

// @spec:signaling.feed.fanout-dedup
// Concurrent/redelivered same-fact is deduped to one stored copy per (agent, interaction, sequence).
func TestFanoutDedup(t *testing.T) {
	f := fact(4, "I", 4, "message.created", "u1")
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		f, f, // the SAME fact delivered twice (redelivery)
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})
	if got := len(sink.feedsFor("alice", "I")); got != 2 { // joined + ONE message (dedup)
		t.Fatalf("alice feed = %d, want 2 (redelivery deduped)", got)
	}
}

// @spec:signaling.feed.revoke-future-facts
// @spec:signaling.feed.revoke-tombstone
// participant.left stops future projection AND emits the feed.revoked tombstone.
func TestRevokeStopsAndTombstones(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "message.created", "u1"),
		fact(4, "I", 4, "participant.left", "alice"),
		fact(5, "I", 5, "message.created", "u1"), // after revoke — must NOT reach alice
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})

	// Separate the projected Event copies from the feed-control tombstone.
	var eventSeqs []int64
	var tomb *signaling.FeedControl
	for _, p := range sink.feedsFor("alice", "I") {
		if fc := decodeControl(p.payload); fc != nil {
			tomb = fc
			continue
		}
		e := &signaling.Event{}
		_ = proto.Unmarshal(p.payload, e)
		eventSeqs = append(eventSeqs, e.Sequence)
	}
	// joined(2), message(3), left(4) — the left fact at L IS projected so the client drops I; the
	// post-revoke message at seq 5 must NOT reach alice.
	if len(eventSeqs) != 3 {
		t.Fatalf("alice Event feed = %v, want seqs [2 3 4]", eventSeqs)
	}
	for _, s := range eventSeqs {
		if s >= 5 {
			t.Fatalf("post-revocation fact at sequence %d leaked to alice", s)
		}
	}
	if tomb == nil {
		t.Fatal("no feed.revoked tombstone written to alice")
	}
	if tomb.InteractionId != "I" || tomb.AtSequence != 4 {
		t.Fatalf("tombstone = %+v, want {I, at_sequence 4}", tomb)
	}
}

// decodeControl returns the FeedControl iff the payload is a feed.revoked marker (not an Event).
func decodeControl(payload []byte) *signaling.FeedControl {
	fc := &signaling.FeedControl{}
	if err := proto.Unmarshal(payload, fc); err == nil && fc.Control == controlRevoked {
		return fc
	}
	return nil
}

// @spec:signaling.feed.transfer-no-gap
// Cold transfer: bob's join opens before alice's leave folds, so the interaction is never absent
// from both inboxes — a fact between the two facts reaches bob, and alice still gets her left fact.
func TestTransferNoGap(t *testing.T) {
	// router writes joined(new) BEFORE left(old): joined(bob)@4, left(alice)@5.
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "message.created", "u1"),
		fact(4, "I", 4, "participant.joined", "bob"),
		fact(5, "I", 5, "participant.left", "alice"),
		fact(6, "I", 6, "message.created", "u1"), // only bob now
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})

	// bob: joined(4), left-of-alice(5)? No — left is alice's fact (actor alice), but bob is an open
	// participant at seq 5 so he ALSO receives it; then message(6).
	bob := sink.feedsFor("bob", "I")
	if got := containsSeq(t, bob, 6); !got {
		t.Fatalf("bob missing the post-transfer message at seq 6; bob feeds=%v", seqsOf(t, bob))
	}
	if got := containsSeq(t, bob, 4); !got {
		t.Fatalf("bob missing his own join at seq 4")
	}
	// alice must NOT get the seq-6 message (left at 5 closed her interval).
	if containsSeq(t, sink.feedsFor("alice", "I"), 6) {
		t.Fatal("alice received a fact after her leave (no-gap violated the epoch guard)")
	}
}

// @spec:signaling.feed.exactly-once-crash
// Partial publish then crash: alice published, bob's publish fails → source Nak'd (un-acked) →
// redelivery re-projects both; alice dedups to one, bob now receives it; ack only after both.
func TestPartialPublishThenCrash(t *testing.T) {
	msg := fact(4, "I", 4, "message.created", "u1")
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "participant.joined", "bob"),
		msg,
	)
	sink := newFakeSink()
	// force bob's projection of seq 4 to fail every PublishRetry attempt on the FIRST delivery only:
	// fail just enough that the first delivery Naks, then succeeds on redelivery.
	bobDedup := fmt.Sprintf("%s.%s.%s.%d", tn, "bob", "I", 4)
	sink.failFor[bobDedup] = 4 // cfg default PublishRetry=4 → first delivery exhausts retries → Nak
	runAll(t, src, sink, nil, Config{})

	// alice received the seq-4 message EXACTLY once despite the redelivery (dedup).
	if n := countSeqIn(t, sink.feedsFor("alice", "I"), 4); n != 1 {
		t.Fatalf("alice got seq 4 %d times, want exactly 1 (dedup across redelivery)", n)
	}
	if !containsSeq(t, sink.feedsFor("bob", "I"), 4) {
		t.Fatal("bob never received seq 4 after redelivery")
	}
	// the source must be acked only after BOTH publishes succeeded → exactly one ack of streamSeq 4.
	if countSeq(src.acked, 4) != 1 {
		t.Fatalf("streamSeq 4 acked %d times, want 1 (ack only after all publishes)", countSeq(src.acked, 4))
	}
	if countSeq(src.naked, 4) < 1 {
		t.Fatal("streamSeq 4 was never Nak'd despite a partial-publish failure")
	}
}

// @spec:signaling.feed.poison-dlq
// A malformed envelope past max_deliver is DLQ'd and acked, not wedged.
func TestPoisonDLQ(t *testing.T) {
	bad := NewFact(nil, "I", 2) // nil Event = malformed; iid "I", streamSeq 2
	facts := []Fact{fact(1, "I", 1, "interaction.started", "u1")}
	// deliver the poison fact MaxDeliver times (the fake redelivers on Nak).
	facts = append(facts, bad)
	src := newFakeSource(facts...)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{MaxDeliver: 3})

	if len(sink.dlq) != 1 {
		t.Fatalf("DLQ entries = %d, want 1; entries=%v", len(sink.dlq), sink.dlq)
	}
	if countSeq(src.acked, 2) != 1 {
		t.Fatalf("poison streamSeq 2 acked %d times, want 1 (acked after DLQ so not wedged)", countSeq(src.acked, 2))
	}
}

// @spec:signaling.feed.serial-fold
// Facts are processed strictly in order, one fully (fold+fanout+ack) before the next — the fake
// source delivers serially and the ack order must equal the stream order.
func TestSerialFold(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "message.created", "u1"),
	)
	runAll(t, src, newFakeSink(), nil, Config{})
	want := []uint64{1, 2, 3}
	if len(src.acked) != 3 || src.acked[0] != want[0] || src.acked[1] != want[1] || src.acked[2] != want[2] {
		t.Fatalf("ack order = %v, want strictly %v (serial fold)", src.acked, want)
	}
}

// @spec:signaling.feed.shard-ownership
// @spec:signaling.feed.cursor-resume
// Restart mid-stream: a worker processes up to seq 3, snapshots; a fresh worker hydrates from the
// snapshot + (snapshot, ack_floor] tail-fold and resumes with no drop/dup.
func TestHydrateFromSnapshotNoDropNoDup(t *testing.T) {
	facts := []Fact{
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "message.created", "u1"),
		fact(4, "I", 4, "message.created", "u1"),
	}
	snaps := newFakeSnaps()

	// worker 1: process the first 3 facts, snapshot after each ack (SnapshotEvery=1).
	src1 := newFakeSource(facts[:3]...)
	sink1 := newFakeSink()
	runAll(t, src1, sink1, snaps, Config{SnapshotEvery: 1})
	if src1.floor != 3 {
		t.Fatalf("worker1 ack floor = %d, want 3", src1.floor)
	}

	// worker 2: a fresh source whose ack floor is 3 (the durable cursor survived) and the FULL log
	// available for the tail-fold; it must hydrate from the snapshot (no replay-from-zero) and
	// project ONLY the unacked fact (seq 4) — alice still a participant via the rehydrated view.
	src2 := newFakeSource(facts...)
	src2.floor = 3
	src2.next = 3 // the durable cursor: only the unacked fact (index 3, streamSeq 4) is delivered
	sink2 := newFakeSink()
	runAll(t, src2, sink2, snaps, Config{SnapshotEvery: 1})

	// seq 4 must reach alice exactly once; no re-projection of seqs 1..3.
	if got := len(sink2.feedsFor("alice", "I")); got != 1 {
		t.Fatalf("worker2 projected %d facts to alice, want 1 (only the unacked seq 4); no replay-from-zero", got)
	}
	e := &signaling.Event{}
	_ = proto.Unmarshal(sink2.feedsFor("alice", "I")[0].payload, e)
	if e.Sequence != 4 {
		t.Fatalf("worker2 projected seq %d, want 4", e.Sequence)
	}
}

// --- small assertions ---

func seqsOf(t *testing.T, ps []pub) []int64 {
	t.Helper()
	var out []int64
	for _, p := range ps {
		e := &signaling.Event{}
		if err := proto.Unmarshal(p.payload, e); err == nil {
			out = append(out, e.Sequence)
		}
	}
	return out
}

func containsSeq(t *testing.T, ps []pub, seq int64) bool {
	t.Helper()
	for _, s := range seqsOf(t, ps) {
		if s == seq {
			return true
		}
	}
	return false
}

func countSeqIn(t *testing.T, ps []pub, seq int64) int {
	t.Helper()
	n := 0
	for _, s := range seqsOf(t, ps) {
		if s == seq {
			n++
		}
	}
	return n
}

func countSeq(xs []uint64, v uint64) int {
	n := 0
	for _, x := range xs {
		if x == v {
			n++
		}
	}
	return n
}
