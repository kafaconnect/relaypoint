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

// deskMinter mirrors desk's internal/ingress Ed25519Issuer ENOUGH to mint a real `vis_` for these tests:
// EdDSA, iss=desk-ingress, aud=relaypoint, kid header, claims tid/cid + subject. It also exposes its public
// key(s) as a JWKS so the verifier's JWKS path is exercised end-to-end against a real signature.
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

// fakeJWKS is the in-memory JWKSSource: it serves bytes, counts fetches (to assert caching/refetch), and can
// be made to fail (unreachable) to prove fail-closed. NO network is touched in these unit tests.
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

	id, err := v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA}))
	if err != nil {
		t.Fatalf("valid vis_ must verify: %v", err)
	}
	if id.Role != signaling.RoleVisitor || id.TenantID != tidOK || id.ConversationID != cidA || id.UserID != subOK {
		t.Fatalf("unexpected identity: %+v", id)
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
// Even with a previously-cached good key set, a refetch triggered by an unknown kid that then FAILS must
// reject — we never fall back to a stale set we could not re-confirm.
func TestVisitorStaleCacheNotTrustedOnRefetchFailure(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	clk := &fakeClock{now: time.Now()}
	v := NewVisitorVerifier(src, time.Minute, WithVisitorClock(clk.Now))

	// Prime the cache with a good verify.
	if _, err := v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("prime: %v", err)
	}
	// Rotate desk to a new kid the cache lacks, but make the JWKS endpoint fail: refetch fails ⇒ reject.
	src.set(nil, errors.New("unreachable"))
	tok := m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, kid: "k2"})
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("refetch failure must reject even with a stale cache")
	}
}

// @spec:authcallout.visitor.rotation-refetch
// A `vis_` signed by a freshly rotated desk key (new kid) triggers exactly one refetch and then verifies.
func TestVisitorRotationRefetch(t *testing.T) {
	old := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(old.jwks(t), nil)
	clk := &fakeClock{now: time.Now()}
	v := NewVisitorVerifier(src, time.Hour, WithVisitorClock(clk.Now)) // long ttl: only an unknown kid forces refetch

	// Verify against the old key, caching it.
	if _, err := v.Verify(old.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("old key verify: %v", err)
	}
	fetchesAfterPrime := src.count()

	// Desk rotates: a new kid signs. The endpoint now serves BOTH keys (publish-before-sign overlap).
	rotated := newDeskMinter(t, "k2") // a distinct key + kid
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

// twoKeyJWKS serves both minters' public keys (rotation overlap).
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
// Within the TTL a known kid is served from cache (no refetch); past the TTL the next verify refetches.
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
// A flood of forged tokens carrying ever-changing unknown kids must NOT fan out into one JWKS fetch each:
// the per-kid refetch cooldown caps unknown-kid-driven refetches to one per window, so an attacker cannot
// hammer desk's endpoint or stall verification (cross-review BLOCKER).
func TestVisitorUnknownKidFloodThrottled(t *testing.T) {
	m := newDeskMinter(t, "k1")
	src := &fakeJWKS{}
	src.set(m.jwks(t), nil)
	clk := &fakeClock{now: time.Now()}
	v := NewVisitorVerifier(src, time.Hour, WithVisitorClock(clk.Now), WithVisitorRefetchCooldown(5*time.Second))

	// Prime a fresh cache (1 fetch).
	if _, err := v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("prime: %v", err)
	}
	base := src.count()

	// Spam 100 forged unknown-kid tokens with the clock frozen inside the cooldown window: at most ONE
	// extra refetch may occur (the first unknown kid), the rest are throttled.
	for i := 0; i < 100; i++ {
		_, _ = v.Verify(m.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA, kid: "forged"}))
	}
	if extra := src.count() - base; extra > 1 {
		t.Fatalf("unknown-kid flood must trigger at most one refetch in the cooldown, got %d", extra)
	}

	// After the cooldown elapses, a genuine new kid (rotation) still verifies — the throttle is not a permanent block.
	clk.now = clk.now.Add(6 * time.Second)
	rotated := newDeskMinter(t, "k2")
	src.set(twoKeyJWKS(t, m, rotated), nil)
	if _, err := v.Verify(rotated.mint(t, mintOpts{sub: subOK, tid: tidOK, cid: cidA})); err != nil {
		t.Fatalf("rotation after cooldown must verify: %v", err)
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
