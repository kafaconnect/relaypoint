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
	return NewRouter(store), NewRouter(store), rjs
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
