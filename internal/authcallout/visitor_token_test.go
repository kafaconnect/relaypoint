package authcallout

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// deskMinter mirrors desk's Ed25519 issuer enough to mint a real `vis_` (EdDSA, iss/aud, kid, tid/cid) + expose a JWKS, so the verifier's JWKS path runs against a real signature.
type deskMinter struct {
	kid  string
	priv ed25519.PrivateKey
}

func newDeskMinter(t *testing.T, kid string) *deskMinter {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &deskMinter{kid: kid, priv: priv}
}

func (m *deskMinter) jwks(t *testing.T) []byte {
	t.Helper()
	k, err := jwk.FromRaw(m.priv.Public())
	if err != nil {
		t.Fatal(err)
	}
	_ = k.Set(jwk.KeyIDKey, m.kid)
	_ = k.Set(jwk.AlgorithmKey, jwa.EdDSA)
	_ = k.Set(jwk.KeyUsageKey, "sig")
	set := jwk.NewSet()
	if err := set.AddKey(k); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type mintOpts struct {
	iss, aud, sub, tid, cid string
	role                    string // when set, stamps the `role` claim (e.g. "agent"); empty == a visitor token
	exp                     time.Time
	noExp                   bool // omit the exp claim entirely (an anomalous token the verifier must reject)
	kid                     string
}

func (m *deskMinter) mint(t *testing.T, o mintOpts) string {
	t.Helper()
	if o.iss == "" {
		o.iss = visitorIssuer
	}
	if o.aud == "" {
		o.aud = visitorAudience
	}
	if o.kid == "" {
		o.kid = m.kid
	}
	if o.exp.IsZero() {
		o.exp = time.Now().Add(5 * time.Minute)
	}
	signKey, err := jwk.FromRaw(m.priv)
	if err != nil {
		t.Fatal(err)
	}
	_ = signKey.Set(jwk.KeyIDKey, o.kid)
	_ = signKey.Set(jwk.AlgorithmKey, jwa.EdDSA)

	b := jwt.NewBuilder().
		Issuer(o.iss).
		Audience([]string{o.aud}).
		Subject(o.sub).
		IssuedAt(time.Now()).
		Claim("tid", o.tid).
		Claim("cid", o.cid)
	if o.role != "" {
		b = b.Claim("role", o.role)
	}
	if !o.noExp {
		b = b.Expiration(o.exp)
	}
	tok, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.EdDSA, signKey))
	if err != nil {
		t.Fatal(err)
	}
	return string(signed)
}

// fakeJWKS is the in-memory JWKSSource: counts fetches (to assert caching/refetch) and can fail (to prove fail-closed); NO network is touched.
type fakeJWKS struct {
	mu      sync.Mutex
	body    []byte
	err     error
	fetches int
}

func (f *fakeJWKS) Fetch(context.Context) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	if f.err != nil {
		return nil, f.err
	}
	return f.body, nil
}

func (f *fakeJWKS) set(body []byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body, f.err = body, err
}

func (f *fakeJWKS) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetches
}

const (
	tidOK = "T1"
	cidA  = "C1"
	subOK = "visitorsess7"
)

// @spec:authcallout.visitor.valid-mints-scoped-grant
func TestVisitorValidVerifies(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	v := NewVisitorVerifier(src, time.Minute)

	exp := time.Now().Add(7 * time.Minute)
	id, err := v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, exp: exp}))
	if err != nil {
		t.Fatalf("valid vis_ must verify: %v", err)
	}
	if id.Role != signaling.RoleVisitor || id.TenantID != tidOK || id.ConversationID != cidA || id.UserID != subOK {
		t.Fatalf("unexpected identity: %+v", id)
	}
	if id.ExpiresAt.Unix() != exp.Unix() {
		t.Fatalf("ExpiresAt = %v, want the vis_ exp %v", id.ExpiresAt, exp)
	}
}

// @spec:authcallout.visitor.rejects-forged-or-stale
func TestVisitorRejects(t *testing.T) {
	m := newDeskMinter(t, "k1")
	other := newDeskMinter(t, "k1") // same kid, different key ⇒ signature won't match the served JWKS

	cases := []struct {
		name string
		tok  func() string
	}{
		{"expired", func() string {
			return m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, exp: time.Now().Add(-time.Second)})
		}},
		{"wrong aud", func() string {
			return m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, aud: "someone-else"})
		}},
		{"wrong iss", func() string {
			return m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, iss: "evil"})
		}},
		{"bad signature", func() string {
			return other.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})
		}},
		{"missing cid", func() string {
			return m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: ""})
		}},
		{"missing exp", func() string {
			return m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, noExp: true})
		}},
		{"unsafe cid", func() string {
			return m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: "C1.evil"})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &fakeJWKS{}
			src.set(m.jwks(t), nil) // served JWKS is ALWAYS m's key
			v := NewVisitorVerifier(src, time.Minute)
			if _, err := v.Verify(c.tok()); err == nil {
				t.Fatalf("%s must be rejected", c.name)
			} else if !errors.Is(err, ErrVisitorToken) {
				t.Fatalf("%s: error must wrap ErrVisitorToken, got %v", c.name, err)
			}
		})
	}
}

// @spec:authcallout.visitor.unknown-kid-no-jwks-match
func TestVisitorUnknownKidNoMatch(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	v := NewVisitorVerifier(src, time.Minute)

	tok := m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, kid: "k-unknown"})
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("a kid absent from the JWKS (even after refetch) must be rejected")
	}
}

// @spec:authcallout.visitor.jwks-unreachable-fail-closed
func TestVisitorJWKSUnreachableFailsClosed(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(nil, errors.New("dial tcp: connection refused"))
	v := NewVisitorVerifier(src, time.Minute)

	if _, err := v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err == nil {
		t.Fatal("JWKS unreachable must fail closed (reject), never mint")
	}
}

// @spec:authcallout.visitor.jwks-unreachable-fail-closed
func TestVisitorStaleCacheNotTrustedOnRefetchFailure(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	clk := &fakeClock{now: time.Now()}
	v := NewVisitorVerifier(src, time.Minute, WithVisitorClock(clk.Now))

	if _, err := v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("prime: %v", err)
	}
	src.set(nil, errors.New("unreachable"))
	tok := m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, kid: "k2"})
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("refetch failure must reject even with a stale cache")
	}
}

// @spec:authcallout.visitor.rotation-refetch
func TestVisitorRotationRefetch(t *testing.T) {
	old := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(old.jwks(t), nil)
	clk := &fakeClock{now: time.Now()}
	v := NewVisitorVerifier(src, time.Hour, WithVisitorClock(clk.Now)) // long ttl: only an unknown kid forces refetch

	if _, err := v.Verify(old.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("old key verify: %v", err)
	}
	fetchesAfterPrime := src.count()

	rotated := newDeskMinter(t, "k2")
	src.set(twoKeyJWKS(t, old, rotated), nil)

	id, err := v.Verify(rotated.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA}))
	if err != nil {
		t.Fatalf("rotated key must verify after refetch: %v", err)
	}
	if id.ConversationID != cidA {
		t.Fatalf("bad identity after rotation: %+v", id)
	}
	if got := src.count() - fetchesAfterPrime; got != 1 {
		t.Fatalf("rotation should trigger exactly one refetch, got %d", got)
	}
}

func twoKeyJWKS(t *testing.T, a, b *deskMinter) []byte {
	t.Helper()
	set := jwk.NewSet()
	for _, m := range []*deskMinter{a, b} {
		k, err := jwk.FromRaw(m.priv.Public())
		if err != nil {
			t.Fatal(err)
		}
		_ = k.Set(jwk.KeyIDKey, m.kid)
		_ = k.Set(jwk.AlgorithmKey, jwa.EdDSA)
		_ = k.Set(jwk.KeyUsageKey, "sig")
		if err := set.AddKey(k); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// @spec:authcallout.visitor.cache-ttl
func TestVisitorCacheTTL(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	clk := &fakeClock{now: time.Now()}
	v := NewVisitorVerifier(src, time.Minute, WithVisitorClock(clk.Now))

	tok := func() string { return m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA}) }

	if _, err := v.Verify(tok()); err != nil {
		t.Fatal(err)
	}
	if src.count() != 1 {
		t.Fatalf("first verify should fetch once, got %d", src.count())
	}
	clk.now = clk.now.Add(30 * time.Second) // within ttl
	if _, err := v.Verify(tok()); err != nil {
		t.Fatal(err)
	}
	if src.count() != 1 {
		t.Fatalf("within ttl should serve from cache, got %d fetches", src.count())
	}
	clk.now = clk.now.Add(2 * time.Minute) // past ttl
	if _, err := v.Verify(tok()); err != nil {
		t.Fatal(err)
	}
	if src.count() != 2 {
		t.Fatalf("past ttl should refetch, got %d fetches", src.count())
	}
}

// @spec:authcallout.visitor.unknown-kid-flood-throttled
func TestVisitorUnknownKidFloodThrottled(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	clk := &fakeClock{now: time.Now()}
	v := NewVisitorVerifier(src, time.Hour, WithVisitorClock(clk.Now), WithVisitorRefetchCooldown(5*time.Second))

	if _, err := v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("prime: %v", err)
	}
	base := src.count()

	for i := 0; i < 100; i++ {
		_, _ = v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, kid: "forged"}))
	}
	if extra := src.count() - base; extra > 1 {
		t.Fatalf("unknown-kid flood must trigger at most one refetch in the cooldown, got %d", extra)
	}

	clk.now = clk.now.Add(6 * time.Second)
	rotated := newDeskMinter(t, "k2")
	src.set(twoKeyJWKS(t, m, rotated), nil)
	if _, err := v.Verify(rotated.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("rotation after cooldown must verify: %v", err)
	}
}

// @spec:authcallout.agent.token-verifies-grants-feed
func TestAgentTokenVerifiesViaJWKSAndGrantsOwnFeed(t *testing.T) {
	m := newDeskMinter(t, "desk-ingress-1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	v := NewVisitorVerifier(src, time.Minute)

	const agentSub = "agent7"
	id, err := v.Verify(m.mint(t, mintOpts{sub: agentSub, tid: tidOK, role: "agent"}))
	if err != nil {
		t.Fatalf("desk agent token must verify via JWKS: %v", err)
	}
	if id.Role != signaling.RoleAgent || id.TenantID != tidOK || id.UserID != agentSub {
		t.Fatalf("unexpected agent identity: %+v", id)
	}
	if id.ConversationID != "" {
		t.Fatalf("agent identity must not carry a ConversationID, got %q", id.ConversationID)
	}

	g, err := GrantsFor(id, "conn-abc")
	if err != nil {
		t.Fatalf("GrantsFor(agent): %v", err)
	}
	if !allowsSubscribe(g, "tenant."+tidOK+".agent."+agentSub+".feed.i1") {
		t.Fatalf("agent must subscribe its own feed; SubAllow=%v", g.SubAllow)
	}
	if !allowsPublish(g, "tenant."+tidOK+".interaction.i1.cmd."+agentSub) {
		t.Fatalf("agent must publish its own cmd suffix; PubAllow=%v", g.PubAllow)
	}
	if allowsSubscribe(g, "tenant."+tidOK+".interaction.i1.log") {
		t.Fatal("agent must NOT read raw interaction logs")
	}
	if allowsSubscribe(g, "tenant."+tidOK+".agent.someone-else.feed.i1") {
		t.Fatal("agent must NOT read another agent's feed")
	}
}

// @spec:authcallout.agent.requires-tid-sub
func TestAgentTokenMissingTidRejected(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	v := NewVisitorVerifier(src, time.Minute)

	for _, c := range []struct {
		name     string
		sub, tid string
	}{
		{"missing tid", subOK, ""},
		{"missing sub", "", tidOK},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := v.Verify(m.mint(t, mintOpts{sub: c.sub, tid: c.tid, role: "agent"}))
			if err == nil {
				t.Fatalf("%s must be rejected", c.name)
			} else if !errors.Is(err, ErrVisitorToken) {
				t.Fatalf("%s: error must wrap ErrVisitorToken, got %v", c.name, err)
			}
		})
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
