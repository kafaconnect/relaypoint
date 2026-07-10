package projector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

type fakeSource struct {
	facts        []Fact
	next         int
	floor        uint64
	deliver      map[uint64]int
	acked        []uint64
	naked        []uint64
	inProgress   int
	redeliverCap int
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
	if s.redeliverCap > 0 && s.deliver[f.StreamSeq] >= s.redeliverCap {
		return nil // broker gave up at MaxDeliver: do NOT rewind (the fact is terminated, no redelivery)
	}
	s.next--
	return nil
}

func (s *fakeSource) InProgress(Fact) error                    { s.inProgress++; return nil }
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
	traceparent               string
}

type fakeSink struct {
	mu      sync.Mutex
	pubs    []pub
	seen    map[string]bool
	dlq     []string
	failFor map[string]int
}

func newFakeSink() *fakeSink { return &fakeSink{seen: map[string]bool{}, failFor: map[string]int{}} }

func (s *fakeSink) Publish(ctx context.Context, tenant, agent, iid, dedupID string, payload []byte) error {
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
	var tp string
	obs.InjectTraceparent(ctx, func(_, v string) { tp = v })
	s.pubs = append(s.pubs, pub{tenant, agent, iid, dedupID, payload, tp})
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

type fakeLease struct {
	mu      sync.Mutex
	renew   func(context.Context) error
	renewed int
}

func (l *fakeLease) Acquire(context.Context) error { return nil }
func (l *fakeLease) Renew(ctx context.Context) error {
	l.mu.Lock()
	l.renewed++
	fn := l.renew
	l.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(ctx)
}
func (l *fakeLease) Release(context.Context) error { return nil }

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

const tn = "t1"

func fact(streamSeq uint64, iid string, seq int64, typ, actor string) Fact {
	return NewFact(&signaling.Event{
		Schema: signaling.SchemaV1, TenantId: tn, EventType: typ, ActorId: actor, Sequence: seq,
		EventId: fmt.Sprintf("ev-%d", streamSeq),
	}, iid, streamSeq)
}

// @spec:obs.rp-log-hop-preserves-trace
func TestProjector_PropagatesTraceFromLogToFeed(t *testing.T) {
	const tp = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"

	// alice is a folded participant of BOTH interactions (structural fan-out; no roster).
	i1join := fact(1, "i1", 1, "participant.joined", "alice")
	traced := fact(2, "i1", 2, "message.created", "u1")
	traced.traceparent = tp
	i2join := fact(3, "i2", 1, "participant.joined", "alice")
	untraced := fact(4, "i2", 2, "message.created", "u1")

	src := newFakeSource(i1join, traced, i2join, untraced)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})

	tracedMsg := feedAtSeq(t, sink.feedsFor("alice", "i1"), 2) // the traced message.created@2
	got, ok := obs.ParseTraceparent(tracedMsg.traceparent)
	if !ok {
		t.Fatalf("feed traceparent not well-formed: %q", tracedMsg.traceparent)
	}
	want, _ := obs.ParseTraceparent(tp)
	if got.TraceID != want.TraceID {
		t.Fatalf("feed trace id = %q, want the inbound .log trace id %q (continuity broken)", got.TraceID, want.TraceID)
	}
	if got.SpanID == want.SpanID {
		t.Fatalf("feed span id should be a fresh child span, got the parent's %q", got.SpanID)
	}

	untracedMsg := feedAtSeq(t, sink.feedsFor("alice", "i2"), 2) // the trace-less message.created@2
	if untracedMsg.traceparent != "" {
		t.Fatalf("a trace-less fact must not fabricate a trace, got %q", untracedMsg.traceparent)
	}
}

// feedAtSeq returns the single feed publish carrying the Event at the given sequence.
func feedAtSeq(t *testing.T, ps []pub, seq int64) pub {
	t.Helper()
	var out []pub
	for _, p := range ps {
		e := &signaling.Event{}
		if err := proto.Unmarshal(p.payload, e); err == nil && e.Sequence == seq {
			out = append(out, p)
		}
	}
	if len(out) != 1 {
		t.Fatalf("want exactly 1 feed publish at sequence %d, got %d", seq, len(out))
	}
	return out[0]
}

func runAll(t *testing.T, src *fakeSource, sink *fakeSink, snaps *fakeSnaps, cfg Config) *Projector {
	t.Helper()
	if snaps == nil {
		snaps = newFakeSnaps()
	}
	p := New(src, sink, &fakeLease{}, snaps, cfg)
	// Run drains the fake source then returns context.Canceled when facts are exhausted.
	err := p.Run(context.Background())
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	return p
}

// @spec:signaling.feed.fanout-to-participants
func TestFanoutToParticipantsOnly(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "participant.joined", "bob"),
		fact(4, "I", 4, "message.created", "u1"),
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})

	if got := len(sink.feedsFor("alice", "I")); got != 3 {
		t.Fatalf("alice feed = %d facts, want 3", got)
	}
	if got := len(sink.feedsFor("bob", "I")); got != 2 {
		t.Fatalf("bob feed = %d facts, want 2", got)
	}
	if got := len(sink.feedsFor("carol", "I")); got != 0 {
		t.Fatalf("carol (non-participant) feed = %d, want 0", got)
	}

	msg := sink.feedsFor("bob", "I")[1]
	e := &signaling.Event{}
	if err := proto.Unmarshal(msg.payload, e); err != nil {
		t.Fatalf("decode projection: %v", err)
	}
	if e.Sequence != 4 || e.EventType != "message.created" || e.EventId != "ev-4" {
		t.Fatalf("projection not verbatim: %+v", e)
	}
}

// @spec:substrate.no-roster-pull
// Fan-out is driven SOLELY by folded participation (coveredBy) — there is no tenant-wide roster
// override left in the substrate. The projector core depends only on LogSource/FeedSink/LeaseStore/
// SnapshotStore; a Roster port no longer exists, so a roster HTTP pull is structurally impossible on
// the projector path. This asserts the fold-only recipient set (SBI-03; desk ADR-0020).
func TestFanoutFromParticipationOnlyNoRosterPull(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "participant.joined", "alice"),
		fact(2, "I", 2, "participant.joined", "bob"),
		fact(3, "I", 3, "message.created", "u1"),
	)
	sink := newFakeSink()
	// No Roster / TenantWideAgents config exists to set — the only recipient source is participation.
	runAll(t, src, sink, nil, Config{})

	if got := len(sink.feedsFor("alice", "I")); got != 3 {
		t.Fatalf("alice feed = %d facts, want 3 (own join, bob join, message) from folded participation alone", got)
	}
	if got := len(sink.feedsFor("bob", "I")); got != 2 {
		t.Fatalf("bob feed = %d facts, want 2 (own join, message) from folded participation alone", got)
	}
	// A tenant agent who never joined this interaction gets nothing — the removed roster override would
	// otherwise have fanned every tenant agent every fact.
	if got := len(sink.feedsFor("stranger", "I")); got != 0 {
		t.Fatalf("stranger (never a participant) feed = %d, want 0 (no tenant-wide roster fan-out)", got)
	}
}

// @spec:projector.delivery.exhausted-to-dlq
func TestExhaustedDeliveryToDLQ(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "participant.joined", "agent1"),
		fact(2, "I", 2, "message.created", "u1"),
	)
	src.redeliverCap = 3
	sink := newFakeSink()
	sink.failFor[fmt.Sprintf("%s.%s.%s.%d", tn, "agent1", "I", 2)] = 99
	runAll(t, src, sink, nil, Config{MaxDeliver: 3, PublishRetry: 1})

	if len(sink.dlq) != 1 {
		t.Fatalf("DLQ entries = %d, want 1 (exhausted delivery dead-lettered, not silently dropped); entries=%v", len(sink.dlq), sink.dlq)
	}
	if !strings.Contains(sink.dlq[0], "ev-2") {
		t.Fatalf("DLQ record %q missing the source event_id", sink.dlq[0])
	}
	if countSeq(src.acked, 2) != 1 {
		t.Fatalf("exhausted streamSeq 2 acked %d times, want 1 (acked after DLQ so the MaxAckPending=1 consumer is not wedged)", countSeq(src.acked, 2))
	}
	if countSeq(src.naked, 2) < 1 {
		t.Fatal("streamSeq 2 was never Nak'd before exhaustion (transient failures must redeliver first)")
	}
}

// @spec:signaling.feed.fanout-dedup
func TestFanoutDedup(t *testing.T) {
	f := fact(4, "I", 4, "message.created", "u1")
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		f, f,
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})
	if got := len(sink.feedsFor("alice", "I")); got != 2 {
		t.Fatalf("alice feed = %d, want 2 (redelivery deduped)", got)
	}
}

// @spec:signaling.feed.revoke-future-facts
// @spec:signaling.feed.revoke-tombstone
func TestRevokeStopsAndTombstones(t *testing.T) {
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "message.created", "u1"),
		fact(4, "I", 4, "participant.left", "alice"),
		fact(5, "I", 5, "message.created", "u1"),
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})

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

func decodeControl(payload []byte) *signaling.FeedControl {
	fc := &signaling.FeedControl{}
	if err := proto.Unmarshal(payload, fc); err == nil && fc.Control == controlRevoked {
		return fc
	}
	return nil
}

// @spec:signaling.feed.transfer-no-gap
func TestTransferNoGap(t *testing.T) {
	// router writes joined(new) BEFORE left(old): joined(bob)@4, left(alice)@5.
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "message.created", "u1"),
		fact(4, "I", 4, "participant.joined", "bob"),
		fact(5, "I", 5, "participant.left", "alice"),
		fact(6, "I", 6, "message.created", "u1"),
	)
	sink := newFakeSink()
	runAll(t, src, sink, nil, Config{})

	bob := sink.feedsFor("bob", "I")
	if got := containsSeq(t, bob, 6); !got {
		t.Fatalf("bob missing the post-transfer message at seq 6; bob feeds=%v", seqsOf(t, bob))
	}
	if got := containsSeq(t, bob, 4); !got {
		t.Fatalf("bob missing his own join at seq 4")
	}
	if containsSeq(t, sink.feedsFor("alice", "I"), 6) {
		t.Fatal("alice received a fact after her leave (no-gap violated the epoch guard)")
	}
}

// @spec:signaling.feed.exactly-once-crash
func TestPartialPublishThenCrash(t *testing.T) {
	msg := fact(4, "I", 4, "message.created", "u1")
	src := newFakeSource(
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "participant.joined", "bob"),
		msg,
	)
	sink := newFakeSink()
	bobDedup := fmt.Sprintf("%s.%s.%s.%d", tn, "bob", "I", 4)
	sink.failFor[bobDedup] = 4 // cfg default PublishRetry=4 → first delivery exhausts retries → Nak
	runAll(t, src, sink, nil, Config{})

	if n := countSeqIn(t, sink.feedsFor("alice", "I"), 4); n != 1 {
		t.Fatalf("alice got seq 4 %d times, want exactly 1 (dedup across redelivery)", n)
	}
	if !containsSeq(t, sink.feedsFor("bob", "I"), 4) {
		t.Fatal("bob never received seq 4 after redelivery")
	}
	if countSeq(src.acked, 4) != 1 {
		t.Fatalf("streamSeq 4 acked %d times, want 1 (ack only after all publishes)", countSeq(src.acked, 4))
	}
	if countSeq(src.naked, 4) < 1 {
		t.Fatal("streamSeq 4 was never Nak'd despite a partial-publish failure")
	}
}

// @spec:projector.config.fanout-concurrency
func TestConfigFanoutConcurrencyDefault(t *testing.T) {
	if got := (Config{}).withDefaults().FanoutConcurrency; got != defaultFanoutConcurrency {
		t.Errorf("default FanoutConcurrency = %d, want %d", got, defaultFanoutConcurrency)
	}
	if got := (Config{FanoutConcurrency: 8}).withDefaults().FanoutConcurrency; got != 8 {
		t.Errorf("explicit FanoutConcurrency = %d, want 8 (caller value preserved, not overridden)", got)
	}
}

// @spec: RDL-01
// @spec: RDL-02
func TestConcurrentFanoutAllRecipients(t *testing.T) {
	agents := []string{"a1", "a2", "a3", "a4", "a5", "a6"}
	// Seed the recipient set via participation joins (structural fan-out); message.created lands at seq 7.
	facts := make([]Fact, 0, len(agents)+1)
	for i, a := range agents {
		facts = append(facts, fact(uint64(i+1), "I", int64(i+1), "participant.joined", a))
	}
	facts = append(facts, fact(7, "I", 7, "message.created", "u1"))
	src := newFakeSource(facts...)
	sink := newFakeSink()
	sink.failFor[fmt.Sprintf("%s.%s.%s.%d", tn, "a4", "I", 7)] = 4
	runAll(t, src, sink, nil, Config{})

	for _, a := range agents {
		if n := countSeqIn(t, sink.feedsFor(a, "I"), 7); n != 1 {
			t.Fatalf("%s got seq 7 %d times, want exactly 1 (concurrent fan-out + dedup across redelivery)", a, n)
		}
	}
	if countSeq(src.acked, 7) != 1 {
		t.Fatalf("streamSeq 7 acked %d times, want 1 (ack only after ALL recipients)", countSeq(src.acked, 7))
	}
	if countSeq(src.naked, 7) < 1 {
		t.Fatal("streamSeq 7 was never Nak'd despite one recipient failing its first delivery")
	}
}

// @spec:signaling.feed.poison-dlq
func TestPoisonDLQ(t *testing.T) {
	bad := NewFact(nil, "I", 2)
	facts := []Fact{fact(1, "I", 1, "interaction.started", "u1")}
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
func TestHydrateFromSnapshotNoDropNoDup(t *testing.T) {
	facts := []Fact{
		fact(1, "I", 1, "interaction.started", "u1"),
		fact(2, "I", 2, "participant.joined", "alice"),
		fact(3, "I", 3, "message.created", "u1"),
		fact(4, "I", 4, "message.created", "u1"),
	}
	snaps := newFakeSnaps()

	src1 := newFakeSource(facts[:3]...)
	sink1 := newFakeSink()
	runAll(t, src1, sink1, snaps, Config{SnapshotEvery: 1})
	if src1.floor != 3 {
		t.Fatalf("worker1 ack floor = %d, want 3", src1.floor)
	}

	src2 := newFakeSource(facts...)
	src2.floor = 3
	src2.next = 3
	sink2 := newFakeSink()
	runAll(t, src2, sink2, snaps, Config{SnapshotEvery: 1})

	if got := len(sink2.feedsFor("alice", "I")); got != 1 {
		t.Fatalf("worker2 projected %d facts to alice, want 1 (only the unacked seq 4); no replay-from-zero", got)
	}
	e := &signaling.Event{}
	_ = proto.Unmarshal(sink2.feedsFor("alice", "I")[0].payload, e)
	if e.Sequence != 4 {
		t.Fatalf("worker2 projected seq %d, want 4", e.Sequence)
	}
}

// @spec:RDL-03
func TestRenewBudgetDerivedUnderSlack(t *testing.T) {
	for _, tc := range []struct{ ttl, interval time.Duration }{
		{5 * time.Second, 2 * time.Second},
		{10 * time.Second, 3 * time.Second},
		{3 * time.Second, 1 * time.Second},
		{500 * time.Millisecond, 200 * time.Millisecond},
	} {
		perAttempt, attempts, backoff := renewBudget(tc.ttl, tc.interval)
		if perAttempt <= 0 || attempts <= 0 {
			t.Fatalf("ttl=%v interval=%v: non-positive budget perAttempt=%v attempts=%d", tc.ttl, tc.interval, perAttempt, attempts)
		}
		total := time.Duration(attempts)*perAttempt + time.Duration(attempts-1)*backoff
		slack := tc.ttl - tc.interval
		if total >= slack {
			t.Fatalf("ttl=%v interval=%v: renew budget total %v must be < fencing slack %v", tc.ttl, tc.interval, total, slack)
		}
	}
}

// @spec:RDL-03
func TestRenewWithRetryBoundedWhenLeaseStalls(t *testing.T) {
	const ttl, interval = 5 * time.Second, 2 * time.Second
	slack := ttl - interval
	lease := &fakeLease{renew: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1200 * time.Millisecond):
			return errors.New("nats: timeout")
		}
	}}
	p := New(newFakeSource(), newFakeSink(), lease, newFakeSnaps(), Config{LeaseTTL: ttl, LeaseRenew: interval})

	start := time.Now()
	err := p.renewWithRetry(context.Background(), newFence(context.Background()))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a permanently-stalled renew must fail, not succeed")
	}
	if elapsed >= slack {
		t.Fatalf("renewWithRetry took %v, must stay under the fencing slack %v — per-attempt ctx did not bound the stalled renew", elapsed, slack)
	}
}

// @spec:RDL-03
func TestFencePauseStopsResumeFail(t *testing.T) {
	f := newFence(context.Background())

	c1 := f.begin()
	if c1 == nil || c1.Err() != nil {
		t.Fatal("a healthy fence must yield a live data context")
	}
	f.pause()
	if c1.Err() == nil {
		t.Fatal("pause must cancel the in-flight data context (stop-the-world)")
	}

	blocked := make(chan context.Context, 1)
	go func() { blocked <- f.begin() }()
	select {
	case <-blocked:
		t.Fatal("begin must block while the lease is paused (data path fenced)")
	case <-time.After(30 * time.Millisecond):
	}

	f.resume()
	select {
	case c2 := <-blocked:
		if c2 == nil || c2.Err() != nil {
			t.Fatal("resume must yield a fresh live data context")
		}
	case <-time.After(time.Second):
		t.Fatal("resume must unblock a paused begin")
	}

	f.fail(errors.New("renew exhausted"))
	if f.begin() != nil {
		t.Fatal("a failed (lost-lease) fence must not yield a data context")
	}
	if err := f.exitErr(); err == nil || !strings.Contains(err.Error(), "lease lost") {
		t.Fatalf("exitErr after fail = %v, want a lease-lost error", err)
	}
}

// @spec:RDL-03
func TestRunStopsOnLeaseLoss(t *testing.T) {
	lease := &fakeLease{renew: func(context.Context) error { return errors.New("nats: timeout") }}
	p := New(blockingSource{}, newFakeSink(), lease, newFakeSnaps(),
		Config{LeaseTTL: 200 * time.Millisecond, LeaseRenew: 60 * time.Millisecond})

	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "lease lost") {
		t.Fatalf("Run must return a lease-lost error on a confirmed renew failure, got %v", err)
	}
}

type fenceSink struct {
	*fakeSink
	entered chan struct{}
	once    sync.Once
}

func (s *fenceSink) Publish(ctx context.Context, tenant, agent, iid, dedup string, payload []byte) error {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return s.fakeSink.Publish(context.Background(), tenant, agent, iid, dedup, payload)
}

// @spec:RDL-03
func TestFencedInFlightPublishNaksNotAcks(t *testing.T) {
	// A join fact folds agent1 in and is itself projected to agent1's feed (coveredBy at its own seq),
	// so the in-flight publish exists without any roster override.
	f := fact(1, "I", 1, "participant.joined", "agent1")
	src := newFakeSource(f)
	base := newFakeSink()
	sink := &fenceSink{fakeSink: base, entered: make(chan struct{})}
	snaps := newFakeSnaps()
	p := New(src, sink, &fakeLease{}, snaps, Config{SnapshotEvery: 1})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.process(ctx, f) }()

	<-sink.entered
	cancel()
	<-done

	if countSeq(src.acked, 1) != 0 {
		t.Fatalf("a fenced in-flight fan-out must NOT ack (a stale holder writing latest); acked=%v", src.acked)
	}
	if countSeq(src.naked, 1) < 1 {
		t.Fatal("a fenced in-flight fan-out must Nak the fact for redelivery")
	}
	if len(snaps.saved) != 0 {
		t.Fatalf("a fenced holder must not write a snapshot; saved=%v", snaps.saved)
	}
	if len(base.dlq) != 0 {
		t.Fatal("a fence cancellation must never DLQ")
	}
}

type blockingSource struct{}

func (blockingSource) Deliver(ctx context.Context) (Fact, error) {
	<-ctx.Done()
	return Fact{}, ctx.Err()
}
func (blockingSource) Ack(Fact) error                           { return nil }
func (blockingSource) Nak(Fact) error                           { return nil }
func (blockingSource) InProgress(Fact) error                    { return nil }
func (blockingSource) Delivered(Fact) int                       { return 0 }
func (blockingSource) AckFloor(context.Context) (uint64, error) { return 0, nil }
func (blockingSource) FoldRange(context.Context, uint64, uint64) ([]Fact, error) {
	return nil, nil
}

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
