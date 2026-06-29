package authcallout

import (
	"strings"
	"testing"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func has(set []string, want string) bool {
	for _, s := range set {
		if s == want {
			return true
		}
	}
	return false
}

// allowsPublish/allowsSubscribe approximate NATS allow/deny resolution (deny wins) for pure-unit policy assertions; the embedded-NATS integration test is authoritative.
func allowsPublish(g Grant, subj string) bool {
	for _, d := range g.PubDeny {
		if subjectMatch(d, subj) {
			return false
		}
	}
	for _, a := range g.PubAllow {
		if subjectMatch(a, subj) {
			return true
		}
	}
	return false
}

func allowsSubscribe(g Grant, subj string) bool {
	for _, d := range g.SubDeny {
		if subjectMatch(d, subj) {
			return false
		}
	}
	for _, a := range g.SubAllow {
		if subjectMatch(a, subj) {
			return true
		}
	}
	return false
}

func subjectMatch(pattern, subj string) bool {
	pt := strings.Split(pattern, ".")
	st := strings.Split(subj, ".")
	for i, tok := range pt {
		if tok == ">" {
			return true
		}
		if i >= len(st) {
			return false
		}
		if tok != "*" && tok != st[i] {
			return false
		}
	}
	return len(pt) == len(st)
}

func TestGrantsForAgent(t *testing.T) {
	id := signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent}
	g, err := GrantsFor(id, "c1")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		kind  string
		subj  string
		allow bool
	}{
		// @spec:signaling.feed.inbox-reads-own-feed-only
		{"sub own feed", "sub", "tenant.T.agent.alice.feed.i1", true},
		{"no raw log read", "sub", "tenant.T.interaction.i1.log", false},
		{"no tenant-wide log read", "sub", "tenant.T.interaction.x.log", false},
		// @spec:signaling.feed.cross-agent-denied
		{"deny other agent feed", "sub", "tenant.T.agent.bob.feed.i1", false},
		// @spec:signaling.feed.cmd-identity-pinned
		{"pub own cmd suffix", "pub", "tenant.T.interaction.i1.cmd.alice", true},
		{"deny cmd other suffix", "pub", "tenant.T.interaction.i1.cmd.bob", false},
		// @spec:signaling.feed.write-server-only
		{"deny feed publish", "pub", "tenant.T.agent.alice.feed.i1", false},
		{"deny log publish", "pub", "tenant.T.interaction.i1.log", false},
		// @spec:signaling.feed.inbox-prefix-isolated
		{"sub own inbox", "sub", "_INBOX_c1.reply", true},
		{"deny broad inbox", "sub", "_INBOX.reply", false},
		{"deny other conn inbox", "sub", "_INBOX_c2.reply", false},
		{"sub own offer", "sub", "tenant.T.routing.offer.user.alice", true},
		{"sub own notify", "sub", "tenant.T.notify.alice", true},
		{"deny other notify", "sub", "tenant.T.notify.bob", false},
		{"pub own presence state", "pub", "tenant.T.presence.alice.state", true},
		{"pub own typing", "pub", "tenant.T.presence.alice.typing.i1", true},
		{"deny forging another's presence", "pub", "tenant.T.presence.bob.state", false},
		{"deny forging another's typing", "pub", "tenant.T.presence.bob.typing.i1", false},
		{"sub roster presence (any actor)", "sub", "tenant.T.presence.bob.state", true},
		{"sub another's typing in a conversation", "sub", "tenant.T.presence.bob.typing.i1", true},
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

func TestGrantsForTrustedBackend(t *testing.T) {
	id := signaling.Identity{TenantID: "T", UserID: "desk", Role: signaling.RoleTrustedBackend}
	g, err := GrantsFor(id, "c9")
	if err != nil {
		t.Fatal(err)
	}
	// @spec:signaling.feed.privileged-assign-to-fact — desk publishes privileged cmds as its own suffix.
	if !allowsPublish(g, "tenant.T.interaction.i1.cmd.desk") {
		t.Fatal("desk should publish its own cmd suffix")
	}
	if allowsPublish(g, "tenant.T.interaction.i1.cmd.alice") {
		t.Fatal("desk must not publish as another identity")
	}
	if !allowsSubscribe(g, "tenant.T.interaction.i1.log") {
		t.Fatal("desk (trusted backend) should read interaction logs")
	}
	if allowsPublish(g, "tenant.T.interaction.i1.log") {
		t.Fatal("even desk may not write .log directly (router-only)")
	}
	if allowsSubscribe(g, "_INBOX.reply") {
		t.Fatal("broad inbox must stay denied")
	}
	if !has(g.SubAllow, "_INBOX_c9.>") {
		t.Fatal("desk should hold its minted inbox prefix")
	}
}

// @spec:signaling.feed.cmd-identity-pinned (ACL-subject injection guarded)
func TestGrantsForRejectsUnsafeSubjectTokens(t *testing.T) {
	bad := []signaling.Identity{
		{TenantID: "a.b", UserID: "alice"},
		{TenantID: "T", UserID: "al*ce"},
		{TenantID: "T", UserID: "ali>e"},
		{TenantID: "T", UserID: "ali ce"},
		{TenantID: "T>", UserID: "alice"},
		{TenantID: "T", UserID: "a\tb"},
	}
	for _, id := range bad {
		if _, err := GrantsFor(id, "c1"); err == nil {
			t.Errorf("GrantsFor must reject unsafe identity %+v", id)
		}
	}
}

func TestGrantsForRejectsIncompleteIdentity(t *testing.T) {
	for _, id := range []signaling.Identity{
		{UserID: "alice"},
		{TenantID: "T"},
		{},
	} {
		if _, err := GrantsFor(id, "c1"); err == nil {
			t.Fatalf("expected error for incomplete identity %+v", id)
		}
	}
	if _, err := GrantsFor(signaling.Identity{TenantID: "T", UserID: "alice"}, ""); err == nil {
		t.Fatal("expected error for empty conn id")
	}
}
