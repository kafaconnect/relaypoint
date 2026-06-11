//go:build integration

// Integration tests for the chat-subset router against a live NATS (JetStream).
//
//	NATS_URL_ROUTER (default nats://router:router-dev@localhost:14222)
//	NATS_URL_CLIENT (default nats://client:client-dev@localhost:14222)
package signaling

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

func urlOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// startRouter wires the in-process router to the test NATS and returns a client conn.
func startRouter(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	rnc, err := nats.Connect(urlOr("NATS_URL_ROUTER", "nats://router:router-dev@localhost:14222"))
	if err != nil {
		t.Skipf("no NATS: %v", err)
	}
	rjs, err := rnc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	// ADR-0002 cutover: start from a clean stream so no JSON-era fact survives into the protobuf run.
	if err := ResetLogStream(rjs); err != nil {
		t.Fatalf("stream: %v", err)
	}
	r := NewRouter(NewJetStreamStore(rjs))
	sub, err := rnc.QueueSubscribe("tenant.*.interaction.*.cmd.*", "router", func(m *nats.Msg) {
		b, _ := proto.Marshal(r.HandleCommand(context.Background(), m.Subject, m.Data))
		if m.Reply != "" {
			_ = m.Respond(b)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	cnc, err := nats.Connect(urlOr("NATS_URL_CLIENT", "nats://client:client-dev@localhost:14222"))
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { sub.Unsubscribe(); rnc.Drain(); cnc.Drain() })
	return cnc, rjs
}

func sendCmd(t *testing.T, cnc *nats.Conn, tenant, iid string, c *Command) *CommandResult {
	t.Helper()
	b, _ := proto.Marshal(c)
	msg, err := cnc.Request(fmt.Sprintf("tenant.%s.interaction.%s.cmd.%s", tenant, iid, c.ActorId), b, 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	res := &CommandResult{}
	if err := proto.Unmarshal(msg.Data, res); err != nil {
		t.Fatalf("bad result: %v", err)
	}
	return res
}

// chatBytes marshals chat text into the `data` payload (registry: medium=chat).
func chatBytes(text string) []byte {
	b, _ := proto.Marshal(&ChatMessage{Text: text})
	return b
}

// readLog replays the durable facts for an interaction in order.
func readLog(t *testing.T, js nats.JetStreamContext, tenant, iid string) []*Event {
	t.Helper()
	subj := fmt.Sprintf("tenant.%s.interaction.%s.log", tenant, iid)
	sub, err := js.PullSubscribe(subj, "", nats.DeliverAll())
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	defer sub.Unsubscribe()
	var out []*Event
	for {
		msgs, err := sub.Fetch(10, nats.MaxWait(500*time.Millisecond))
		if err != nil || len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			e := &Event{}
			_ = proto.Unmarshal(m.Data, e)
			out = append(out, e)
			m.Ack()
		}
	}
	return out
}

// @spec:wire.protobuf.router-end-to-end
func TestRouterChat(t *testing.T) {
	cnc, js := startRouter(t)
	iid := fmt.Sprintf("im%d", time.Now().UnixNano())
	const tn = "t1"

	// @spec:signaling.cmd.router-assigns-sequence
	// @spec:signaling.cmd.result-transport
	// @spec:signaling.unified-interaction
	t.Run("sequence+result+envelope", func(t *testing.T) {
		r1 := sendCmd(t, cnc, tn, iid, &Command{CommandId: "c1", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"})
		if r1.Status != statusAccepted || r1.CausedBy == "" {
			t.Fatalf("start: %+v (want accepted + caused_by)", r1)
		}
		r2 := sendCmd(t, cnc, tn, iid, &Command{CommandId: "c2", TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat", RefId: "m1", Data: chatBytes("hi")})
		if r2.Status != statusAccepted {
			t.Fatalf("message: %+v", r2)
		}
		facts := readLog(t, js, tn, iid)
		if len(facts) != 2 || facts[0].Sequence != 1 || facts[1].Sequence != 2 {
			t.Fatalf("sequences: %+v want 1,2", facts)
		}
		if facts[1].CausedBy != "c2" || facts[1].Medium != "chat" {
			t.Fatalf("envelope: %+v (want caused_by=c2, medium=chat)", facts[1])
		}
	})

	// @spec:signaling.cmd.idempotent-command-id
	t.Run("idempotent-command-id", func(t *testing.T) {
		cmd := &Command{CommandId: "idem", TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat", Data: chatBytes("once")}
		a := sendCmd(t, cnc, tn, iid, cmd)
		before := len(readLog(t, js, tn, iid))
		b := sendCmd(t, cnc, tn, iid, cmd) // retry, identical
		after := len(readLog(t, js, tn, iid))
		if a.CausedBy != b.CausedBy || after != before {
			t.Fatalf("retry produced a second fact: a=%v b=%v before=%d after=%d", a, b, before, after)
		}
	})

	// @spec:signaling.cmd.command-id-conflict
	t.Run("command-id-conflict", func(t *testing.T) {
		_ = sendCmd(t, cnc, tn, iid, &Command{CommandId: "conf", TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat", Data: chatBytes("A")})
		r := sendCmd(t, cnc, tn, iid, &Command{CommandId: "conf", TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat", Data: chatBytes("B")})
		if r.Status != statusRejected || r.Reason == "" {
			t.Fatalf("reused id with different payload should be 'conflict' rejected, got %+v", r)
		}
	})

	// @spec:signaling.security.payload-tenant-match
	t.Run("payload-tenant-match", func(t *testing.T) {
		r := sendCmd(t, cnc, tn, iid, &Command{CommandId: "x1", TenantId: "OTHER", ActorId: "u1", Type: "message.created", Medium: "chat"})
		if r.Status != statusRejected {
			t.Fatalf("payload tenant mismatch must be rejected, got %+v", r)
		}
	})

	// @spec:signaling.cmd.illegal-transition-rejected
	t.Run("illegal-transition-rejected", func(t *testing.T) {
		j2 := fmt.Sprintf("iz%d", time.Now().UnixNano())
		// message.created before interaction.started → illegal
		r := sendCmd(t, cnc, tn, j2, &Command{CommandId: "z1", TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat"})
		if r.Status != statusRejected {
			t.Fatalf("message before start must be rejected, got %+v", r)
		}
		// start, end, then message → illegal (ended is terminal)
		sendCmd(t, cnc, tn, j2, &Command{CommandId: "z2", TenantId: tn, ActorId: "u1", Type: "interaction.started", Medium: "chat"})
		sendCmd(t, cnc, tn, j2, &Command{CommandId: "z3", TenantId: tn, ActorId: "u1", Type: "interaction.ended", Medium: "chat"})
		r = sendCmd(t, cnc, tn, j2, &Command{CommandId: "z4", TenantId: tn, ActorId: "u1", Type: "message.created", Medium: "chat"})
		if r.Status != statusRejected {
			t.Fatalf("message after end must be rejected, got %+v", r)
		}
	})

	// @spec:signaling.log-durable
	t.Run("log-durable", func(t *testing.T) {
		// a fresh JetStream consumer re-reads the ordered facts (durability/replay)
		facts := readLog(t, js, tn, iid)
		if len(facts) < 2 {
			t.Fatalf("log not durable/replayable: %d facts", len(facts))
		}
		for i := 1; i < len(facts); i++ {
			if facts[i].Sequence <= facts[i-1].Sequence {
				t.Fatalf("log not ordered: %v", facts)
			}
		}
	})
}

// @spec:signaling.cmd.log-write-only-router
func TestClientCannotWriteLog(t *testing.T) {
	cnc, err := nats.Connect(urlOr("NATS_URL_CLIENT", "nats://client:client-dev@localhost:14222"))
	if err != nil {
		t.Skipf("no NATS: %v", err)
	}
	defer cnc.Drain()
	permErr := make(chan error, 1)
	cnc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { permErr <- e })
	// a client publishing a forged fact to `.log` must be denied by the NATS ACL.
	if err := cnc.Publish("tenant.t1.interaction.iX.log", []byte(`{"forged":true}`)); err != nil {
		return // synchronous deny
	}
	cnc.Flush()
	select {
	case e := <-permErr:
		if e == nil {
			t.Fatal("expected a permissions error")
		}
	case <-time.After(time.Second):
		t.Fatal("client was allowed to publish to .log (ACL not enforced)")
	}
}

// @spec:wire.protobuf.stream-reset
// ResetLogStream deletes + recreates INTERACTION_LOGS so a protobuf router replays a clean log:
// a pre-existing (JSON-era) fact does NOT survive the reset, and the recreated stream is empty.
func TestResetLogStreamPurgesFacts(t *testing.T) {
	rnc, err := nats.Connect(urlOr("NATS_URL_ROUTER", "nats://router:router-dev@localhost:14222"))
	if err != nil {
		t.Skipf("no NATS: %v", err)
	}
	defer rnc.Drain()
	js, err := rnc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := EnsureLogStream(js); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	iid := fmt.Sprintf("ir%d", time.Now().UnixNano())
	subj := logSubjectFor("t1", iid)
	if _, err := js.Publish(subj, []byte(`{"legacy":"json"}`)); err != nil { // a JSON-era fact
		t.Fatalf("seed: %v", err)
	}

	if err := ResetLogStream(js); err != nil {
		t.Fatalf("reset: %v", err)
	}

	store := NewJetStreamStore(js)
	facts, _, err := store.Replay(subj)
	if err != nil {
		t.Fatalf("replay after reset must succeed on a clean stream, got %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("reset must purge facts, found %d", len(facts))
	}
}
