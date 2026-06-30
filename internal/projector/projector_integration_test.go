//go:build integration

// Integration tests against a live JetStream NATS at NATS_URL_PROJECTOR (default nats://localhost:14222).
package projector

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func urlOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func connectJS(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(urlOr("NATS_URL_PROJECTOR", "nats://localhost:14222"))
	if err != nil {
		t.Skipf("no NATS: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return nc, js
}

func freshStreams(t *testing.T, js nats.JetStreamContext) {
	t.Helper()
	_ = js.DeleteConsumer("INTERACTION_LOGS", durableName)
	for _, s := range []string{"INTERACTION_LOGS", feedStream} {
		_ = js.DeleteStream(s)
	}
	for _, b := range []string{kvLeaseName, kvSnapName} {
		_ = js.DeleteKeyValue(b)
	}
	if err := signaling.EnsureLogStream(js); err != nil {
		t.Fatalf("log stream: %v", err)
	}
	if err := EnsureFeedStream(js, time.Hour, 10*time.Minute); err != nil {
		t.Fatalf("feed stream: %v", err)
	}
}

func appendFact(t *testing.T, js nats.JetStreamContext, iid string, seq int64, typ, actor string) {
	t.Helper()
	e := &signaling.Event{
		Schema: signaling.SchemaV1, TenantId: tn, EventType: typ, ActorId: actor, Sequence: seq,
		EventId: fmt.Sprintf("ev-%s-%d", iid, seq),
	}
	b, _ := proto.Marshal(e)
	subj := fmt.Sprintf("tenant.%s.interaction.%s.log", tn, iid)
	if _, err := js.Publish(subj, b, nats.MsgId(fmt.Sprintf("%s.%d", iid, seq))); err != nil {
		t.Fatalf("append %s seq %d: %v", typ, seq, err)
	}
}

func drainFeed(t *testing.T, js nats.JetStreamContext, agent, iid string) [][]byte {
	t.Helper()
	subj := fmt.Sprintf("tenant.%s.agent.%s.feed.%s", tn, agent, iid)
	sub, err := js.PullSubscribe(subj, "", nats.DeliverAll(), nats.AckNone())
	if err != nil {
		t.Fatalf("feed sub: %v", err)
	}
	defer sub.Unsubscribe()
	var out [][]byte
	for {
		msgs, ferr := sub.Fetch(64, nats.MaxWait(400*time.Millisecond))
		if ferr != nil || len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			out = append(out, m.Data)
		}
	}
	return out
}

func eventSeqs(t *testing.T, msgs [][]byte) []int64 {
	t.Helper()
	var out []int64
	for _, b := range msgs {
		fc := &signaling.FeedControl{}
		if err := proto.Unmarshal(b, fc); err == nil && fc.Control == controlRevoked {
			continue
		}
		e := &signaling.Event{}
		_ = proto.Unmarshal(b, e)
		out = append(out, e.Sequence)
	}
	return out
}

func has(xs []int64, v int64) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func count(xs []int64, v int64) int {
	n := 0
	for _, x := range xs {
		if x == v {
			n++
		}
	}
	return n
}

func runProjector(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, cfg Config) (stop func(), p *Projector) {
	t.Helper()
	jsKV, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream kv: %v", err)
	}
	src, err := NewLogSource(js, 3, 3*time.Second)
	if err != nil {
		t.Fatalf("log source: %v", err)
	}
	sink := NewFeedSink(js)
	lease, err := NewLeaseStore(jsKV, fmt.Sprintf("w-%d", time.Now().UnixNano()), 5*time.Second)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	snaps, err := NewSnapshotStore(jsKV)
	if err != nil {
		t.Fatalf("snaps: %v", err)
	}
	p = New(src, sink, lease, snaps, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()
	return func() { cancel(); <-done }, p
}

func waitUntil(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(msg)
}

// @spec:signaling.feed.fanout-to-participants
// @spec:signaling.feed.fanout-dedup
// @spec:signaling.feed.write-server-only
func TestIntegration_FanoutTwoParticipantsVerbatimOnce(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Drain()
	freshStreams(t, js)

	appendFact(t, js, "I", 1, "interaction.started", "u1")
	appendFact(t, js, "I", 2, "participant.joined", "alice")
	appendFact(t, js, "I", 3, "participant.joined", "bob")
	appendFact(t, js, "I", 4, "message.created", "u1")

	stop, _ := runProjector(t, nc, js, Config{SnapshotEvery: 1})
	defer stop()

	waitUntil(t, func() bool {
		return has(eventSeqs(t, drainFeed(t, js, "alice", "I")), 4) &&
			has(eventSeqs(t, drainFeed(t, js, "bob", "I")), 4)
	}, "seq 4 did not reach both alice and bob feeds")

	if n := count(eventSeqs(t, drainFeed(t, js, "alice", "I")), 4); n != 1 {
		t.Fatalf("alice got seq 4 %d times, want 1", n)
	}
	if n := count(eventSeqs(t, drainFeed(t, js, "bob", "I")), 4); n != 1 {
		t.Fatalf("bob got seq 4 %d times, want 1", n)
	}
	if got := drainFeed(t, js, "carol", "I"); len(got) != 0 {
		t.Fatalf("carol (non-participant) feed = %d, want 0", len(got))
	}

	msgs := drainFeed(t, js, "bob", "I")
	var seen bool
	for _, b := range msgs {
		e := &signaling.Event{}
		_ = proto.Unmarshal(b, e)
		if e.Sequence == 4 {
			if e.EventType != "message.created" || e.EventId != "ev-I-4" {
				t.Fatalf("projection not verbatim: %+v", e)
			}
			seen = true
		}
	}
	if !seen {
		t.Fatal("bob feed missing the verbatim seq-4 projection")
	}
}

// @spec:signaling.feed.revoke-future-facts
// @spec:signaling.feed.revoke-tombstone
func TestIntegration_RevokeStopsAndTombstones(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Drain()
	freshStreams(t, js)

	appendFact(t, js, "I", 1, "interaction.started", "u1")
	appendFact(t, js, "I", 2, "participant.joined", "alice")
	appendFact(t, js, "I", 3, "message.created", "u1")
	appendFact(t, js, "I", 4, "participant.left", "alice")
	appendFact(t, js, "I", 5, "message.created", "u1")

	stop, _ := runProjector(t, nc, js, Config{SnapshotEvery: 1})
	defer stop()

	waitUntil(t, func() bool {
		for _, b := range drainFeed(t, js, "alice", "I") {
			fc := &signaling.FeedControl{}
			if err := proto.Unmarshal(b, fc); err == nil && fc.Control == controlRevoked {
				return true
			}
		}
		return false
	}, "no feed.revoked tombstone for alice")

	msgs := drainFeed(t, js, "alice", "I")
	if has(eventSeqs(t, msgs), 5) {
		t.Fatal("post-revocation fact at seq 5 leaked to alice")
	}
	if !has(eventSeqs(t, msgs), 4) {
		t.Fatal("the participant.left fact at seq 4 must be projected to alice")
	}
	var tomb *signaling.FeedControl
	for _, b := range msgs {
		fc := &signaling.FeedControl{}
		if err := proto.Unmarshal(b, fc); err == nil && fc.Control == controlRevoked {
			tomb = fc
		}
	}
	if tomb == nil || tomb.InteractionId != "I" || tomb.AtSequence != 4 {
		t.Fatalf("tombstone = %+v, want {I, at_sequence 4}", tomb)
	}
}

// @spec:signaling.feed.shard-ownership
// @spec:signaling.feed.cursor-resume
// @spec:signaling.feed.serial-fold
func TestIntegration_RestartHydratesNoDropNoDup(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Drain()
	freshStreams(t, js)

	appendFact(t, js, "I", 1, "interaction.started", "u1")
	appendFact(t, js, "I", 2, "participant.joined", "alice")
	appendFact(t, js, "I", 3, "message.created", "u1")

	stop1, _ := runProjector(t, nc, js, Config{SnapshotEvery: 1})
	waitUntil(t, func() bool { return has(eventSeqs(t, drainFeed(t, js, "alice", "I")), 3) }, "worker1 did not project seq 3")
	stop1()

	appendFact(t, js, "I", 4, "message.created", "u1")
	appendFact(t, js, "I", 5, "message.created", "u1")

	stop2, _ := runProjector(t, nc, js, Config{SnapshotEvery: 1})
	defer stop2()
	waitUntil(t, func() bool {
		s := eventSeqs(t, drainFeed(t, js, "alice", "I"))
		return has(s, 4) && has(s, 5)
	}, "worker2 did not resume seq 4,5 after hydration")

	seqs := eventSeqs(t, drainFeed(t, js, "alice", "I"))
	for _, s := range []int64{2, 3, 4, 5} {
		if count(seqs, s) != 1 {
			t.Fatalf("seq %d appears %d times in alice feed, want exactly 1 (no drop/dup); all=%v", s, count(seqs, s), seqs)
		}
	}
}

// @spec:signaling.feed.exactly-once-crash
func TestIntegration_ConcurrentSameFactDeduped(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Drain()
	freshStreams(t, js)

	appendFact(t, js, "I", 1, "interaction.started", "u1")
	appendFact(t, js, "I", 2, "participant.joined", "alice")
	appendFact(t, js, "I", 3, "message.created", "u1")

	stop, _ := runProjector(t, nc, js, Config{SnapshotEvery: 1})
	waitUntil(t, func() bool { return has(eventSeqs(t, drainFeed(t, js, "alice", "I")), 3) }, "projector did not project seq 3")
	stop()

	dedup := fmt.Sprintf("%s.%s.%s.%d", tn, "alice", "I", 3)
	e := &signaling.Event{Schema: signaling.SchemaV1, TenantId: tn, EventType: "message.created", ActorId: "u1", Sequence: 3, EventId: "ev-I-3"}
	b, _ := proto.Marshal(e)
	subj := fmt.Sprintf("tenant.%s.agent.%s.feed.%s", tn, "alice", "I")
	for i := 0; i < 3; i++ {
		_, _ = js.Publish(subj, b, nats.MsgId(dedup))
	}
	if n := count(eventSeqs(t, drainFeed(t, js, "alice", "I")), 3); n != 1 {
		t.Fatalf("seq 3 stored %d times after concurrent same-fact publishes, want 1 (dedup)", n)
	}
}

func leaderCount(ps ...*Projector) int {
	n := 0
	for _, p := range ps {
		if p.Ready() == nil {
			n++
		}
	}
	return n
}

// @spec:projector.ha.warm-standby-replicas
//
// HA topology under test: TWO projector instances share ONE JetStream and ONE
// lease bucket (kvLeaseName) — the deployed `replicas: 2` warm standby. The KV
// lease (single-active leader election) is the ONLY thing holding the serial
// fold + one-command-per-feed delivery invariants under >=2 replicas.
//
// Proven here:
//  1. single-active — exactly ONE instance is the leader (Ready()==nil); the
//     other is fenced inside Acquire and never goes live (never fans out).
//  2. warm-standby failover — when the holder releases the lease, the standby
//     acquires it and resumes the fold from the durable ack floor.
//  3. exactly-once across the handover — every command lands on the feed once:
//     no command skipped, none double-delivered.
//
// Documented coverage / boundaries:
//   - A hard CRASH that lets the lease lapse by TTL takes the SAME Acquire/Create
//     path; it is driven here deterministically via an explicit lease Release
//     (stopping the leader) instead of a ~5s TTL sleep — no sleep-and-hope.
//   - The feed's Nats-Msg-Id dedup is the production at-most-once boundary, so a
//     rogue standby publish is invisible at the feed. Single-active is therefore
//     asserted at the lease/Ready() seam, and the end-to-end exactly-once
//     invariant is asserted by counting feed commands across the real handover.
func TestIntegration_TwoReplicasSingleActiveWarmStandbyFailover(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Drain()
	freshStreams(t, js)

	appendFact(t, js, "I", 1, "interaction.started", "u1")
	appendFact(t, js, "I", 2, "participant.joined", "alice")
	appendFact(t, js, "I", 3, "message.created", "u1")

	// Two replicas contend for the SAME lease bucket against the SAME JetStream.
	stopA, pa := runProjector(t, nc, js, Config{SnapshotEvery: 1})
	defer stopA()
	stopB, pb := runProjector(t, nc, js, Config{SnapshotEvery: 1})
	defer stopB()
	stopOf := map[*Projector]func(){pa: stopA, pb: stopB}

	// (1) single-active: exactly one leader, the other fenced in Acquire.
	waitUntil(t, func() bool { return leaderCount(pa, pb) == 1 }, "exactly one replica must hold the lease")
	leader, standby := pa, pb
	if pb.Ready() == nil {
		leader, standby = pb, pa
	}
	if standby.Ready() == nil {
		t.Fatal("two leaders at once — single-active lease election failed")
	}

	// The single active leader delivers the whole queued batch.
	waitUntil(t, func() bool { return has(eventSeqs(t, drainFeed(t, js, "alice", "I")), 3) },
		"the active leader did not project the initial batch")
	if standby.Ready() == nil {
		t.Fatal("standby went live while the leader still held the lease (a standby must not fan out)")
	}

	// (2) warm-standby failover: the holder releases the lease (clean stop ->
	// deferred Release deletes the leader key); the standby must take over.
	stopOf[leader]()
	waitUntil(t, func() bool { return standby.Ready() == nil },
		"standby did not acquire the lease after the holder released it")

	appendFact(t, js, "I", 4, "message.created", "u1")
	appendFact(t, js, "I", 5, "message.created", "u1")
	waitUntil(t, func() bool {
		s := eventSeqs(t, drainFeed(t, js, "alice", "I"))
		return has(s, 4) && has(s, 5)
	}, "standby did not resume the fold from the ack floor after takeover")

	// (3) exactly-once across the handover: no skip, no double-delivery.
	seqs := eventSeqs(t, drainFeed(t, js, "alice", "I"))
	for _, s := range []int64{2, 3, 4, 5} {
		if count(seqs, s) != 1 {
			t.Fatalf("seq %d appears %d times across the leader handover, want exactly 1 (no skip / no double-delivery); all=%v", s, count(seqs, s), seqs)
		}
	}
}
