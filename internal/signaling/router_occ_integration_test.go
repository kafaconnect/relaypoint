//go:build integration

// OCC concurrency tests against a live NATS (JetStream). Two routers over ONE stream model an
// HA pair (or a stale rebuilt state): per-subject optimistic concurrency must keep the log
// authoritative — exactly one fact per router-assigned sequence, the loser re-folds and retries.
// See openspec change router-occ.
package signaling

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

func occRouters(t *testing.T) (*Router, *Router, nats.JetStreamContext) {
	t.Helper()
	rnc, err := nats.Connect(urlOr("NATS_URL_ROUTER", "nats://router:router-dev@localhost:14222"))
	if err != nil {
		t.Skipf("no NATS: %v", err)
	}
	rjs, err := rnc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := ResetLogStream(rjs); err != nil {
		t.Fatalf("stream: %v", err)
	}
	t.Cleanup(func() { rnc.Drain() })
	store := NewJetStreamStore(rjs)
	// Two routers, one store: distinct in-memory state, the same durable log — an HA pair.
	return NewRouter(store, WithDevMode()), NewRouter(store, WithDevMode()), rjs
}

func handle(r *Router, tenant, iid string, c *Command) *CommandResult {
	b, _ := proto.Marshal(c)
	return r.HandleCommand(context.Background(), fmt.Sprintf("tenant.%s.interaction.%s.cmd.%s", tenant, iid, c.ActorId), b)
}

// @spec:router.occ.expected-subject-seq
// Two concurrent commands on ONE interaction (one per router) over a live stream produce exactly
// one fact per sequence: no duplicate sequence, the loser re-folds and retries, ordering holds.
func TestOCC_ConcurrentSingleInteraction(t *testing.T) {
	ra, rb, js := occRouters(t)
	const tn = "t1"
	iid := fmt.Sprintf("occ%d", time.Now().UnixNano())

	if r := handle(ra, tn, iid, &Command{CommandId: "start", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"}); r.Status != statusAccepted {
		t.Fatalf("seed start: %+v", r)
	}
	// Prime rb's in-memory fold so both routers believe the subject is at the SAME last sequence:
	// the next two appends will collide on one sequence unless OCC arbitrates.
	if r := handle(rb, tn, iid, &Command{CommandId: "start", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"}); r.Status != statusAccepted {
		t.Fatalf("prime rb: %+v", r)
	}

	const n = 40
	results := make([]*CommandResult, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		r := ra
		if i%2 == 1 {
			r = rb
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = handle(r, tn, iid, &Command{
				CommandId: fmt.Sprintf("m%d", i), TenantId: tn, ActorId: "u1",
				Type: "message.created", Medium: "chat", RefId: fmt.Sprintf("ref%d", i),
				Data: chatBytes(fmt.Sprintf("msg-%d", i)),
			})
		}()
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r == nil || r.Status != statusAccepted {
			t.Fatalf("cmd %d not accepted under contention: %+v", i, r)
		}
	}

	facts := readLog(t, js, tn, iid)
	if len(facts) != n+1 { // the start + n messages, each committed exactly once
		t.Fatalf("want %d facts (1 start + %d msgs), got %d", n+1, n, len(facts))
	}
	seen := map[int64]string{}
	for i, f := range facts {
		if prev, dup := seen[f.Sequence]; dup {
			t.Fatalf("DUPLICATE sequence %d: command_ids %q and %q both got it", f.Sequence, prev, f.CommandId)
		}
		seen[f.Sequence] = f.CommandId
		if int64(i+1) != f.Sequence { // dense, gapless, monotonic
			t.Fatalf("sequence not dense/ordered at index %d: seq=%d", i, f.Sequence)
		}
	}
}

// @spec:router.occ.expected-subject-seq
// The loser of a head-to-head race re-folds and lands the next sequence — never a duplicate.
func TestOCC_LoserRefoldsAndRetries(t *testing.T) {
	ra, rb, js := occRouters(t)
	const tn = "t1"
	iid := fmt.Sprintf("occ2%d", time.Now().UnixNano())

	handle(ra, tn, iid, &Command{CommandId: "s", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"})
	handle(rb, tn, iid, &Command{CommandId: "s", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"})

	var wg sync.WaitGroup
	start := make(chan struct{})
	out := make([]*CommandResult, 2)
	for i, r := range []*Router{ra, rb} {
		i, r := i, r
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out[i] = handle(r, tn, iid, &Command{
				CommandId: fmt.Sprintf("c%d", i), TenantId: tn, ActorId: "u1",
				Type: "message.created", Medium: "chat", RefId: fmt.Sprintf("r%d", i), Data: chatBytes(fmt.Sprintf("p%d", i)),
			})
		}()
	}
	close(start)
	wg.Wait()

	for i, r := range out {
		if r == nil || r.Status != statusAccepted {
			t.Fatalf("racer %d not accepted (loser must re-fold + retry, not reject): %+v", i, r)
		}
	}
	facts := readLog(t, js, tn, iid)
	if len(facts) != 3 {
		t.Fatalf("want 3 facts (start + 2 racers), got %d", len(facts))
	}
	if facts[0].Sequence != 1 || facts[1].Sequence != 2 || facts[2].Sequence != 3 {
		t.Fatalf("sequences not 1,2,3 (a duplicate or gap): %d,%d,%d", facts[0].Sequence, facts[1].Sequence, facts[2].Sequence)
	}
}

// @spec:router.occ.committed-stream-seq
// Two DISTINCT interactions interleave on the live shared INTERACTION_LOGS stream: each clean append
// must echo the broker-committed ack.Sequence as its next OCC token, not prev+1. A ++-guessed token
// goes stale the moment the OTHER interaction advances the shared global stream sequence, raising a
// spurious ErrOCCConflict on ~every append. Asserts zero broker conflicts (counted on the real
// store) and dense, gapless per-interaction sequences.
func TestOCC_InterleavedInteractionsNoSpuriousConflict(t *testing.T) {
	rnc, err := nats.Connect(urlOr("NATS_URL_ROUTER", "nats://router:router-dev@localhost:14222"))
	if err != nil {
		t.Skipf("no NATS: %v", err)
	}
	rjs, err := rnc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := ResetLogStream(rjs); err != nil {
		t.Fatalf("stream: %v", err)
	}
	t.Cleanup(func() { rnc.Drain() })

	cs := &countingStore{LogStore: NewJetStreamStore(rjs)}
	r := NewRouter(cs, WithDevMode())
	const tn = "t1"
	iidA := fmt.Sprintf("ilA%d", time.Now().UnixNano())
	iidB := fmt.Sprintf("ilB%d", time.Now().UnixNano())

	mustAccept := func(iid string, c *Command) {
		t.Helper()
		if got := handle(r, tn, iid, c); got.Status != statusAccepted {
			t.Fatalf("%s/%s: %+v (want accepted)", iid, c.CommandId, got)
		}
	}
	mustAccept(iidA, &Command{CommandId: "a-start", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"})
	mustAccept(iidB, &Command{CommandId: "b-start", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"})
	const rounds = 8
	for i := 0; i < rounds; i++ {
		mustAccept(iidA, &Command{CommandId: fmt.Sprintf("a%d", i), TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat", Data: chatBytes(fmt.Sprintf("a-%d", i))})
		mustAccept(iidB, &Command{CommandId: fmt.Sprintf("b%d", i), TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat", Data: chatBytes(fmt.Sprintf("b-%d", i))})
	}
	if n := cs.occConflicts(); n != 0 {
		t.Fatalf("stale ++-guessed OCC token caused %d spurious broker conflict(s) on interleaved interactions; want 0", n)
	}
	for _, iid := range []string{iidA, iidB} {
		facts := readLog(t, rjs, tn, iid)
		if len(facts) != rounds+1 {
			t.Fatalf("interaction %s: want %d facts, got %d", iid, rounds+1, len(facts))
		}
		for i, f := range facts {
			if f.Sequence != int64(i+1) {
				t.Fatalf("interaction %s: dense sequence broke at index %d: seq=%d", iid, i, f.Sequence)
			}
		}
	}
}
