package authcallout

import (
	"testing"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// @spec:authcallout.visitor.grant-scoped-one-conversation
// A visitor grant subscribes EXACTLY its one conversation's `.log` (the SDK chat slice) + the transitional
// events plane plus its minted inbox, publishes nothing, and is denied every OTHER conversation's log/events,
// the agent feed, and the raw tenant-wide log. cid == interaction id (ADR-0009).
func TestGrantsForVisitor(t *testing.T) {
	id := signaling.Identity{TenantID: "T", UserID: "sess9", Role: signaling.RoleVisitor, ConversationID: "C1"}
	g, err := GrantsFor(id, "v1")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		kind  string
		subj  string
		allow bool
	}{
		{"sub own conversation log", "sub", "tenant.T.interaction.C1.log", true},
		{"deny other conversation log", "sub", "tenant.T.interaction.C2.log", false},
		{"sub own conversation events", "sub", "tenant.T.conversation.C1.events", true},
		{"deny other conversation events", "sub", "tenant.T.conversation.C2.events", false},
		{"deny agent feed", "sub", "tenant.T.agent.alice.feed.i1", false},
		{"deny tenant-wide", "sub", "tenant.T.routing.offer.user.alice", false},
		{"sub own inbox", "sub", "_INBOX_v1.reply", true},
		{"deny broad inbox", "sub", "_INBOX.reply", false},
		{"deny other conn inbox", "sub", "_INBOX_v2.reply", false},
		{"deny publish own conversation", "pub", "tenant.T.conversation.C1.events", false},
		{"deny publish cmd", "pub", "tenant.T.interaction.i1.cmd.sess9", false},
		{"deny publish anything", "pub", "tenant.T.whatever", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got bool
			if c.kind == "pub" {
				got = allowsPublish(g, c.subj)
			} else {
				got = allowsSubscribe(g, c.subj)
			}
			if got != c.allow {
				t.Fatalf("%s %s: allow=%v want %v", c.kind, c.subj, got, c.allow)
			}
		})
	}
}

// @spec:authcallout.visitor.subscribe-only-no-responses
// A visitor grant carries NO response permission, so an inbound event's `reply` subject can never become a
// one-shot publish path past the static PubDeny. Agent + trusted-backend keep responses (request/reply).
func TestVisitorGrantHasNoResponsePermission(t *testing.T) {
	vis, _ := GrantsFor(signaling.Identity{TenantID: "T", UserID: "s", Role: signaling.RoleVisitor, ConversationID: "C1"}, "v1")
	if vis.AllowResponses {
		t.Fatal("visitor must NOT be granted a response permission (subscribe-only)")
	}
	agent, _ := GrantsFor(signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent}, "v1")
	be, _ := GrantsFor(signaling.Identity{TenantID: "T", UserID: "desk", Role: signaling.RoleTrustedBackend}, "v1")
	if !agent.AllowResponses || !be.AllowResponses {
		t.Fatal("agent + trusted-backend must keep their response permission (request/reply)")
	}
}

// @spec:authcallout.visitor.grant-binds-cid
// A visitor for conversation A cannot subscribe conversation B: the grant binds the cid, so swapping the
// Identity's ConversationID changes ONLY which conversation literal is permitted.
func TestVisitorGrantBindsConversation(t *testing.T) {
	a, err := GrantsFor(signaling.Identity{TenantID: "T", UserID: "s", Role: signaling.RoleVisitor, ConversationID: "A"}, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if !allowsSubscribe(a, "tenant.T.conversation.A.events") {
		t.Fatal("visitor A must read conversation A")
	}
	if allowsSubscribe(a, "tenant.T.conversation.B.events") {
		t.Fatal("visitor A must NOT read conversation B")
	}
}

// @spec:authcallout.visitor.grant-binds-cid
// An unsafe/empty conversation id is rejected at the grant boundary (it is interpolated into ACL subjects).
func TestVisitorGrantRejectsUnsafeConversation(t *testing.T) {
	for _, cid := range []string{"", "A.B", "A*", "A>"} {
		id := signaling.Identity{TenantID: "T", UserID: "s", Role: signaling.RoleVisitor, ConversationID: cid}
		if _, err := GrantsFor(id, "v1"); err == nil {
			t.Errorf("GrantsFor must reject unsafe/empty conversation %q", cid)
		}
	}
}

// @spec:authcallout.responder.chain-no-regression
// The verify ladder (agent/backend HMAC → visitor) leaves the agent and trusted-backend paths unchanged.
func TestChainVerifierUnregressedAndVisitor(t *testing.T) {
	secret := []byte("s")
	hmac := NewHMACVerifier(secret)

	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	vis := NewVisitorVerifier(src, 0) // ttl 0 ⇒ always refetch; irrelevant to identity here

	chain := NewChainVerifier(hmac, vis)

	agentTok, _ := MintDevToken(secret, signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent}, 0)
	if id, err := chain.Verify(agentTok); err != nil || id.Role != signaling.RoleAgent || id.UserID != "alice" {
		t.Fatalf("agent path regressed: id=%+v err=%v", id, err)
	}
	beTok, _ := MintDevToken(secret, signaling.Identity{TenantID: "T", UserID: "desk", Role: signaling.RoleTrustedBackend}, 0)
	if id, err := chain.Verify(beTok); err != nil || id.Role != signaling.RoleTrustedBackend {
		t.Fatalf("trusted-backend path regressed: id=%+v err=%v", id, err)
	}
	visTok := m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})
	if id, err := chain.Verify(visTok); err != nil || id.Role != signaling.RoleVisitor || id.ConversationID != cidA {
		t.Fatalf("visitor path via chain failed: id=%+v err=%v", id, err)
	}
	if _, err := chain.Verify("garbage"); err == nil {
		t.Fatal("a token no link accepts must be denied")
	}
}
