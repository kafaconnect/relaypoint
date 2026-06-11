//go:build integration

package signaling

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// countingStore wraps a real LogStore (the JetStream adapter) and counts the ErrOCCConflicts the
// router provokes — the router core still depends only on the LogStore port, no NATS leaks in.
type countingStore struct {
	inner     LogStore
	conflicts int64
}

func (s *countingStore) Append(subject string, data []byte, dedupID string, expectedLastSubjSeq uint64) (uint64, bool, error) {
	seq, dup, err := s.inner.Append(subject, data, dedupID, expectedLastSubjSeq)
	if err == ErrOCCConflict {
		atomic.AddInt64(&s.conflicts, 1)
	}
	return seq, dup, err
}

func (s *countingStore) Replay(subject string) ([]*Event, uint64, error) {
	return s.inner.Replay(subject)
}

// Regression for the global-stream-sequence OCC bug (router-occ cross-review). STREAM sequences are
// GLOBAL to INTERACTION_LOGS, not per-subject contiguous: an append to ANOTHER interaction advances
// the global sequence, so a naive st.streamSeq++ sets a stale Nats-Expected-Last-Subject-Sequence
// for THIS interaction's next append → spurious ErrOCCConflict → a full re-fold on nearly every
// interleaved append (rebuild storms). A single uncontended writer interleaving appends across two
// interactions on ONE stream must provoke ZERO OCC conflicts.
//
// Load-bearing: with st.streamSeq++ the store sees a stale expected-sequence after each cross-
// interaction append and returns ErrOCCConflict (conflicts > 0); with st.streamSeq = the store-
// assigned stream sequence, conflicts == 0.
func TestOCC_InterleavedInteractionsNoSpuriousConflict(t *testing.T) {
	rnc, err := nats.Connect(urlOr("NATS_URL_ROUTER", "nats://router:router-dev@localhost:14222"))
	if err != nil {
		t.Skipf("no NATS: %v", err)
	}
	t.Cleanup(func() { rnc.Drain() })
	rjs, err := rnc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := ResetLogStream(rjs); err != nil {
		t.Fatalf("stream: %v", err)
	}
	cs := &countingStore{inner: NewJetStreamStore(rjs)}
	r := NewRouter(cs)

	const tn = "t1"
	ns := time.Now().UnixNano()
	iidA := fmt.Sprintf("occA-%d", ns)
	iidB := fmt.Sprintf("occB-%d", ns)

	for _, iid := range []string{iidA, iidB} {
		if got := handle(r, tn, iid, &Command{CommandId: "start-" + iid, TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"}); got.Status != statusAccepted {
			t.Fatalf("start %s: %+v", iid, got)
		}
	}

	// Interleave message appends A,B,A,B,... Each append to B advances the GLOBAL stream sequence
	// between two appends to A (and vice versa), so A's cached OCC token is stale under the bug.
	const rounds = 8
	for i := 0; i < rounds; i++ {
		for _, iid := range []string{iidA, iidB} {
			got := handle(r, tn, iid, &Command{
				CommandId: fmt.Sprintf("m-%s-%d", iid, i), TenantId: tn, ActorId: "u1",
				Type: "message.created", Medium: "chat", RefId: fmt.Sprintf("r%d", i),
				Data: chatBytes(fmt.Sprintf("msg-%s-%d", iid, i)),
			})
			if got.Status != statusAccepted {
				t.Fatalf("interleaved append %s round %d not accepted: %+v", iid, i, got)
			}
		}
	}

	if c := atomic.LoadInt64(&cs.conflicts); c != 0 {
		t.Fatalf("spurious OCC conflicts on interleaved single-writer appends: got %d, want 0 "+
			"(naive st.streamSeq++ token does not track the GLOBAL stream sequence)", c)
	}
}
