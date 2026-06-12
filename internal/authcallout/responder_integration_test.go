//go:build integration

// Integration test: an EPHEMERAL nats-server with auth_callout enabled + this responder, asserting
// the minted per-connection ACLs are actually enforced by NATS (not just the pure policy). Brings
// up nats-server via Docker (image NATS_IMAGE, default nats:2.10-alpine); skips if Docker is absent.
package authcallout

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

const tokenSecret = "integration-secret"

// natsConf renders an auth_callout config. account = APP account public key (where minted users
// land + the responder lives); responderUserPass = the responder's static credential (an auth_user
// exempt from the callout so cutover never locks it out).
func natsConf(account, responderPass string) string {
	return fmt.Sprintf(`
port: 4222
http_port: -1
accounts {
  APP: {
    users: [
      { user: "authsvc", password: %q, permissions: {
        publish:   { allow: [ "$SYS.REQ.USER.AUTH", "$SYS._INBOX.>", "_INBOX.>" ] }
        subscribe: { allow: [ "$SYS.REQ.USER.AUTH", "$SYS._INBOX.>", "_INBOX.>" ] }
      } },
      { user: "deskpub", password: "deskpub-dev", permissions: {
        publish:   { allow: [ "tenant.>", "_INBOX.>" ] }
        subscribe: { allow: [ "_INBOX.>" ] }
      } }
    ]
  }
}
authorization {
  auth_callout {
    issuer: %q
    account: APP
    auth_users: [ authsvc, deskpub ]
  }
}
`, responderPass, account)
}

func writeConf(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// startNATS launches nats-server in Docker with the given config and returns its client URL +
// monitoring port. The account issuer keypair signs minted user JWTs.
func startNATS(t *testing.T) (url string, accountKP nkeys.KeyPair, responderPass string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("no docker: %v", err)
	}
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, _ := kp.PublicKey()
	responderPass = "authsvc-dev"
	conf := writeConf(t, natsConf(accountPub, responderPass))

	img := os.Getenv("NATS_IMAGE")
	if img == "" {
		img = "nats:2.10-alpine"
	}
	name := fmt.Sprintf("rp-authcallout-it-%d", time.Now().UnixNano())
	// -p 0 lets Docker pick a free host port; we read it back via `docker port`.
	run := exec.Command("docker", "run", "-d", "--rm", "--name", name,
		"-p", "4222",
		"-v", conf+":/etc/nats/nats.conf:ro",
		img, "-c", "/etc/nats/nats.conf")
	out, err := run.CombinedOutput()
	if err != nil {
		t.Skipf("docker run failed (%v): %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	var hostPort string
	for i := 0; i < 50; i++ {
		pout, perr := exec.Command("docker", "port", name, "4222/tcp").CombinedOutput()
		if perr == nil && len(pout) > 0 {
			// "0.0.0.0:49xxx\n" (may have an IPv6 line too); take the first.
			line := string(pout)
			if idx := indexByte(line, '\n'); idx >= 0 {
				line = line[:idx]
			}
			if c := lastColon(line); c >= 0 {
				hostPort = line[c+1:]
			}
			if hostPort != "" {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if hostPort == "" {
		t.Fatal("could not resolve nats host port")
	}
	url = "nats://127.0.0.1:" + hostPort
	return url, kp, responderPass
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}

func dialResponder(t *testing.T, url, pass string, kp nkeys.KeyPair) {
	t.Helper()
	seed, _ := kp.Seed()
	v := NewHMACVerifier([]byte(tokenSecret))
	r, err := NewResponder(v, seed, "APP")
	if err != nil {
		t.Fatal(err)
	}
	nc, err := connectWithRetry(t, url, nats.UserInfo("authsvc", pass))
	if err != nil {
		t.Fatalf("responder connect: %v", err)
	}
	if _, err := r.Subscribe(nc); err != nil {
		t.Fatalf("responder subscribe: %v", err)
	}
	t.Cleanup(func() { nc.Drain() })
}

// dialResponderChain wires the responder with the F1 verify ladder (HMAC → visitor over the supplied
// JWKSSource), so the embedded NATS enforces the minted VISITOR ACL end-to-end — the authoritative proof
// the one-conversation scoping holds, not just the pure policy.
func dialResponderChain(t *testing.T, url, pass string, kp nkeys.KeyPair, vis Verifier) {
	t.Helper()
	seed, _ := kp.Seed()
	chain := NewChainVerifier(NewHMACVerifier([]byte(tokenSecret)), vis)
	r, err := NewResponder(chain, seed, "APP")
	if err != nil {
		t.Fatal(err)
	}
	nc, err := connectWithRetry(t, url, nats.UserInfo("authsvc", pass))
	if err != nil {
		t.Fatalf("responder connect: %v", err)
	}
	if _, err := r.Subscribe(nc); err != nil {
		t.Fatalf("responder subscribe: %v", err)
	}
	t.Cleanup(func() { nc.Drain() })
}

// @spec:authcallout.visitor.grant-scoped-one-conversation (enforced by real NATS)
// A desk `vis_` for conversation C1 mints a credential that subscribes ONLY tenant.T.conversation.C1.events,
// is denied C2 / the agent feed / .log / publish — the embedded server, not the policy unit test, enforces it.
func TestAuthCalloutMintsVisitorACL(t *testing.T) {
	url, kp, pass := startNATS(t)

	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	vis := NewVisitorVerifier(src, time.Minute)
	dialResponderChain(t, url, pass, kp, vis)

	visTok := m.mint(t, mintOpts{sub: "sess1", tid: "T", cid: "C1"})

	if !canSub(t, url, visTok, "tenant.T.conversation.C1.events") {
		t.Error("visitor must subscribe its own conversation events")
	}
	if canSub(t, url, visTok, "tenant.T.conversation.C2.events") {
		t.Error("visitor must NOT subscribe another conversation (cid-bound)")
	}
	if canSub(t, url, visTok, "tenant.T.agent.alice.feed.i1") {
		t.Error("visitor must NOT subscribe an agent feed")
	}
	if canSub(t, url, visTok, "tenant.T.interaction.i1.log") {
		t.Error("visitor must NOT subscribe raw .log")
	}
	if canSub(t, url, visTok, "_INBOX.>") {
		t.Error("visitor must NOT subscribe broad _INBOX.>")
	}
	if canPub(t, url, visTok, "tenant.T.conversation.C1.events") {
		t.Error("visitor must NOT publish its conversation (read-only)")
	}
	if canPub(t, url, visTok, "tenant.T.interaction.i1.cmd.sess1") {
		t.Error("visitor must NOT publish any cmd")
	}
}

// @spec:authcallout.visitor.subscribe-only-no-responses (enforced by real NATS)
// A visitor receives an event carrying a `reply` subject and tries to answer it. Because the visitor JWT
// has NO response permission, the reply publish is denied — the subscribe-only invariant holds even against
// the dynamic response-permission path (cross-review HIGH).
func TestAuthCalloutVisitorCannotReply(t *testing.T) {
	url, kp, pass := startNATS(t)

	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	dialResponderChain(t, url, pass, kp, NewVisitorVerifier(src, time.Minute))

	visTok := m.mint(t, mintOpts{sub: "sess1", tid: "T", cid: "C1"})

	denied := make(chan struct{}, 1)
	vc, err := connectWithRetry(t, url, nats.Token(visTok),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {
			select {
			case denied <- struct{}{}:
			default:
			}
		}))
	if err != nil {
		t.Fatalf("visitor connect: %v", err)
	}
	defer vc.Drain()

	// The visitor subscribes its conversation and, on receipt, attempts to publish to the message's reply.
	_, err = vc.Subscribe("tenant.T.conversation.C1.events", func(msg *nats.Msg) {
		_ = vc.Publish(msg.Reply, []byte("answer")) // must be denied (no response permission)
	})
	if err != nil {
		t.Fatalf("visitor subscribe: %v", err)
	}
	vc.Flush()

	// The desk data-plane publisher (a static account user, auth_users-exempt) publishes an event WITH a
	// reply pointing at a privileged subject — representing desk's kept conversation.events publisher (§3d).
	dc, err := connectWithRetry(t, url, nats.UserInfo("deskpub", "deskpub-dev"))
	if err != nil {
		t.Fatalf("desk publisher connect: %v", err)
	}
	defer dc.Drain()
	if err := dc.PublishMsg(&nats.Msg{
		Subject: "tenant.T.conversation.C1.events",
		Reply:   "tenant.T.interaction.evil.log",
		Data:    []byte("event"),
	}); err != nil {
		t.Fatalf("desk publish: %v", err)
	}

	select {
	case <-denied:
		// expected: the visitor's reply publish was rejected
	case <-time.After(800 * time.Millisecond):
		t.Error("visitor reply publish should have been denied (no response permission)")
	}
}

// @spec:authcallout.responder.chain-no-regression (enforced by real NATS)
// With the visitor link wired, an AGENT token still mints exactly its existing feed grant — the ladder
// does not regress the agent path.
func TestAuthCalloutChainAgentUnregressed(t *testing.T) {
	url, kp, pass := startNATS(t)

	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	dialResponderChain(t, url, pass, kp, NewVisitorVerifier(src, time.Minute))

	aliceTok := token(t, signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent})
	if !canSub(t, url, aliceTok, "tenant.T.agent.alice.feed.>") {
		t.Error("agent feed read regressed under the chain")
	}
	if canSub(t, url, aliceTok, "tenant.T.conversation.C1.events") {
		t.Error("agent must NOT gain the visitor conversation plane")
	}
	if !canPub(t, url, aliceTok, "tenant.T.interaction.i1.cmd.alice") {
		t.Error("agent cmd publish regressed under the chain")
	}
}

func connectWithRetry(t *testing.T, url string, opts ...nats.Option) (*nats.Conn, error) {
	t.Helper()
	var last error
	for i := 0; i < 50; i++ {
		nc, err := nats.Connect(url, opts...)
		if err == nil {
			return nc, nil
		}
		last = err
		time.Sleep(150 * time.Millisecond)
	}
	return nil, last
}

func token(t *testing.T, id signaling.Identity) string {
	t.Helper()
	tok, err := MintDevToken([]byte(tokenSecret), id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// canSub reports whether subj may be subscribed under the minted ACLs. A denied SUB triggers an
// async permission-violation on the connection (the sync sub stays "valid" locally), so we detect
// it via a dedicated connection's error handler — the authoritative signal NATS gives.
func canSub(t *testing.T, url, tok, subj string) bool {
	t.Helper()
	denied := make(chan struct{}, 1)
	nc, err := connectWithRetry(t, url, nats.Token(tok),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
			select {
			case denied <- struct{}{}:
			default:
			}
		}))
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer nc.Drain()
	if _, err := nc.SubscribeSync(subj); err != nil {
		return false
	}
	nc.Flush()
	select {
	case <-denied:
		return false
	case <-time.After(300 * time.Millisecond):
		return true
	}
}

// canPub reports whether a publish to subj is permitted: a denied publish triggers an async
// permission-violation error on the connection. We capture it via an error handler.
func canPub(t *testing.T, url, tok, subj string) bool {
	t.Helper()
	denied := make(chan struct{}, 1)
	nc, err := connectWithRetry(t, url, nats.Token(tok),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
			select {
			case denied <- struct{}{}:
			default:
			}
		}))
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer nc.Drain()
	if err := nc.Publish(subj, []byte("x")); err != nil {
		return false
	}
	nc.Flush()
	select {
	case <-denied:
		return false
	case <-time.After(300 * time.Millisecond):
		return true
	}
}

func TestAuthCalloutMintsPinnedAgentACLs(t *testing.T) {
	url, kp, pass := startNATS(t)
	dialResponder(t, url, pass, kp)

	aliceTok := token(t, signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent})

	// @spec:signaling.feed.inbox-reads-own-feed-only
	if !canSub(t, url, aliceTok, "tenant.T.agent.alice.feed.>") {
		t.Error("alice must subscribe her own feed")
	}
	if canSub(t, url, aliceTok, "tenant.T.interaction.i1.log") {
		t.Error("alice must NOT subscribe raw interaction logs")
	}
	// @spec:signaling.feed.cross-agent-denied
	if canSub(t, url, aliceTok, "tenant.T.agent.bob.feed.>") {
		t.Error("alice must NOT subscribe bob's feed")
	}
	// @spec:signaling.feed.inbox-prefix-isolated — broad inbox denied.
	if canSub(t, url, aliceTok, "_INBOX.>") {
		t.Error("alice must NOT subscribe broad _INBOX.>")
	}

	// @spec:signaling.feed.cmd-identity-pinned — can pub her own suffix, not another's, not a feed.
	if !canPub(t, url, aliceTok, "tenant.T.interaction.i1.cmd.alice") {
		t.Error("alice must publish her own cmd suffix")
	}
	if canPub(t, url, aliceTok, "tenant.T.interaction.i1.cmd.bob") {
		t.Error("alice must NOT publish another agent's cmd suffix")
	}
	// @spec:signaling.feed.write-server-only
	if canPub(t, url, aliceTok, "tenant.T.agent.alice.feed.i1") {
		t.Error("alice must NOT publish a feed subject")
	}
	if canPub(t, url, aliceTok, "tenant.T.interaction.i1.log") {
		t.Error("alice must NOT publish (forge) a .log fact")
	}
}

// @spec:signaling.feed.inbox-reads-own-feed-only (no JetStream API)
// The agent holds NO $JS.API grant, so it cannot drive the JetStream consumer API to pull-read raw
// .log or another agent's feed — the consumer-API path that would otherwise bypass the subject-level
// denies. Its own feed is reachable only by a core subscribe (asserted above). (A4)
func TestAuthCalloutAgentDeniedJetStreamConsumerAPI(t *testing.T) {
	url, kp, pass := startNATS(t)
	dialResponder(t, url, pass, kp)

	aliceTok := token(t, signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent})

	// Creating/binding a pull consumer or fetching the next message goes over $JS.API.CONSUMER.*;
	// every such publish must be denied.
	for _, subj := range []string{
		"$JS.API.CONSUMER.CREATE.INTERACTION_LOGS",
		"$JS.API.CONSUMER.DURABLE.CREATE.INTERACTION_LOGS.snoop",
		"$JS.API.CONSUMER.MSG.NEXT.INTERACTION_LOGS.snoop",
		"$JS.API.CONSUMER.CREATE.AGENT_FEED",
		"$JS.API.CONSUMER.MSG.NEXT.AGENT_FEED.snoop",
		"$JS.API.STREAM.MSG.GET.INTERACTION_LOGS",
	} {
		if canPub(t, url, aliceTok, subj) {
			t.Errorf("agent must NOT reach the JetStream API at %s (could pull-read raw .log / other feeds)", subj)
		}
	}
}

func TestAuthCalloutMintsTrustedBackendACLs(t *testing.T) {
	url, kp, pass := startNATS(t)
	dialResponder(t, url, pass, kp)

	deskTok := token(t, signaling.Identity{TenantID: "T", UserID: "desk", Role: signaling.RoleTrustedBackend})

	// @spec:signaling.feed.privileged-assign-to-fact — desk lands participation cmds as its suffix.
	if !canPub(t, url, deskTok, "tenant.T.interaction.i1.cmd.desk") {
		t.Error("desk must publish its own privileged cmd suffix")
	}
	if canPub(t, url, deskTok, "tenant.T.interaction.i1.cmd.alice") {
		t.Error("desk must NOT publish as another identity")
	}
	// Trusted backend reads interaction logs for routing (broader than an agent).
	if !canSub(t, url, deskTok, "tenant.T.interaction.i1.log") {
		t.Error("desk must read interaction logs")
	}
	// But still may not forge a .log fact.
	if canPub(t, url, deskTok, "tenant.T.interaction.i1.log") {
		t.Error("desk must NOT write .log directly (router-only)")
	}
}

func TestAuthCalloutDeniesBadToken(t *testing.T) {
	url, kp, pass := startNATS(t)
	dialResponder(t, url, pass, kp)
	// A garbage token must be rejected at connect (the responder signs a DENY).
	if nc, err := nats.Connect(url, nats.Token("not-a-valid-token"),
		nats.MaxReconnects(0), nats.Timeout(2*time.Second)); err == nil {
		nc.Close()
		t.Error("connection with an invalid token must be denied")
	}
}
