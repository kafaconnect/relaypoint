package authcallout

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/sync/singleflight"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// F1 (desk design §3): RP is the SOLE auth-callout responder. A widget end-user presents a desk-minted
// `vis_` token — an EdDSA JWT (iss=desk-ingress, aud=relaypoint) RP verifies against desk's PUBLISHED
// public keys (JWKS) only. RP can verify a visitor but can NEVER mint one (the owner-required asymmetry);
// the symmetric `wk_` HMAC stays inside desk. Verification fails CLOSED: JWKS unreachable, bad signature,
// wrong iss/aud, expired, or unknown kid that the (refetched) JWKS still lacks → reject, no grant.

const (
	// visitorIssuer / visitorAudience are the fixed iss/aud the desk ingress stamps on every `vis_`
	// (desk internal/ingress/visitor_exchange.go: VisitorIssuer / VisitorAudience). A token missing or
	// differing on either is not a visitor token and is rejected.
	visitorIssuer   = "desk-ingress"
	visitorAudience = "relaypoint"
)

// ErrVisitorToken is the sentinel a visitor verify failure wraps so the chain can distinguish a "not a
// visitor token / rejected" outcome. It never carries key/token material.
var ErrVisitorToken = errors.New("authcallout: visitor token rejected")

// JWKSSource is the owned port the visitor verifier resolves desk's public key set through (loose-coupling
// HARD RULE): unit tests inject a fake and NEVER hit the network. Fetch returns the current JWKS bytes; a
// non-nil error MUST fail the verify closed (no grant). The implementation is responsible only for I/O —
// caching/rotation policy lives in VisitorVerifier so it is identically tested against the fake.
type JWKSSource interface {
	Fetch(ctx context.Context) ([]byte, error)
}

// VisitorVerifier verifies a desk `vis_` token against desk's JWKS and derives a RoleVisitor Identity bound
// to the token's tid/cid. It caches the parsed key set with a TTL and refetches on an unknown kid (rotation):
// a `vis_` signed by a freshly rotated desk key triggers exactly one refetch, then verifies. It implements
// the Verifier port so it slots into the responder's verify ladder.
//
// Refetch is DoS-hardened (cross-review BLOCKER): the global lock is NEVER held across the HTTP fetch, a
// singleflight collapses concurrent refetches into one upstream call, and a per-kid cooldown throttles
// unknown-kid floods — an attacker spamming forged tokens with random kids cannot stall verification or
// hammer desk's JWKS endpoint.
type VisitorVerifier struct {
	src        JWKSSource
	ttl        time.Duration
	refetchMin time.Duration // min interval between refetches driven by an UNKNOWN kid (negative throttle)
	now        func() time.Time
	fetchCtx   func() (context.Context, context.CancelFunc)
	group      singleflight.Group

	mu              sync.Mutex
	cached          jwk.Set
	fetchedAt       time.Time
	lastUnknownPoll time.Time // last time an unknown kid triggered a refetch (cooldown anchor)
}

// VisitorOption configures the verifier (tests inject a clock / fetch timeout).
type VisitorOption func(*VisitorVerifier)

// WithVisitorClock overrides the verifier clock (tests assert expiry without sleeping).
func WithVisitorClock(now func() time.Time) VisitorOption {
	return func(v *VisitorVerifier) { v.now = now }
}

// WithVisitorFetchTimeout bounds each JWKS fetch so a hung desk endpoint cannot stall the responder; a
// timed-out fetch is a fetch error and fails the verify closed.
func WithVisitorFetchTimeout(d time.Duration) VisitorOption {
	return func(v *VisitorVerifier) {
		v.fetchCtx = func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), d)
		}
	}
}

// NewVisitorVerifier builds the verifier over a JWKS source. ttl bounds how long a cached key set is reused
// before a time-based refetch; an unknown kid forces a refetch regardless of ttl (rotation safety).
func NewVisitorVerifier(src JWKSSource, ttl time.Duration, opts ...VisitorOption) *VisitorVerifier {
	v := &VisitorVerifier{
		src:        src,
		ttl:        ttl,
		refetchMin: 5 * time.Second, // an unknown kid can drive at most one refetch per this window
		now:        time.Now,
		fetchCtx:   func() (context.Context, context.CancelFunc) { return context.Background(), func() {} },
	}
	for _, o := range opts {
		o(v)
	}
	return v
}

// WithVisitorRefetchCooldown sets the minimum interval between unknown-kid-driven refetches (the negative
// throttle that absorbs a forged-kid flood). A new known kid still verifies on the next allowed refetch.
func WithVisitorRefetchCooldown(d time.Duration) VisitorOption {
	return func(v *VisitorVerifier) { v.refetchMin = d }
}

// Verify validates the `vis_` token and returns a RoleVisitor Identity bound to tid/cid. Every failure path
// (parse, signature, iss/aud, expiry, unsafe claims, JWKS fetch) returns an error wrapping ErrVisitorToken so
// no partial/unverified identity ever escapes — fail closed.
func (v *VisitorVerifier) Verify(token string) (signaling.Identity, error) {
	if token == "" {
		return signaling.Identity{}, fmt.Errorf("%w: empty token", ErrVisitorToken)
	}

	// Resolve the signing kid from the JWS header WITHOUT trusting the body, so an unknown kid can drive a
	// rotation refetch before any signature work.
	kid, err := headerKID(token)
	if err != nil {
		return signaling.Identity{}, fmt.Errorf("%w: %v", ErrVisitorToken, err)
	}

	set, err := v.keySet(kid)
	if err != nil {
		return signaling.Identity{}, fmt.Errorf("%w: jwks: %v", ErrVisitorToken, err)
	}

	// Verify the signature (EdDSA only — pin the alg so an `alg:none`/HMAC-confusion token is rejected),
	// require iss/aud, and validate exp against the verifier clock (server time authority, never the client).
	parsed, err := jwt.Parse([]byte(token),
		jwt.WithKeySet(set, jws.WithRequireKid(true), jws.WithInferAlgorithmFromKey(false)),
		jwt.WithValidate(true),
		jwt.WithIssuer(visitorIssuer),
		jwt.WithAudience(visitorAudience),
		jwt.WithRequiredClaim(jwt.ExpirationKey), // a `vis_` is always time-bounded (desk caps exp ≤ wk_.exp); a missing exp is anomalous → reject
		jwt.WithClock(clockFunc(v.clock)),
	)
	if err != nil {
		return signaling.Identity{}, fmt.Errorf("%w: %v", ErrVisitorToken, err)
	}

	tid, _ := claimString(parsed, "tid")
	sub := parsed.Subject()
	role, _ := claimString(parsed, "role")
	if tid == "" || sub == "" {
		return signaling.Identity{}, fmt.Errorf("%w: missing tid/sub", ErrVisitorToken)
	}
	// tid/sub are interpolated into the minted ACL subjects — reject any value that is not a single
	// safe NATS subject token (the A6 injection guard the grant boundary enforces) before it reaches a grant.
	for _, c := range []struct{ k, s string }{{"tid", tid}, {"sub", sub}} {
		if err := validSubjectToken(c.s); err != nil {
			return signaling.Identity{}, fmt.Errorf("%w: unsafe %s: %v", ErrVisitorToken, c.k, err)
		}
	}

	// A desk-minted AGENT connect token (role=agent) carries no conversation binding — its grant is the
	// agent's own feed (GrantsFor RoleAgent). Same EdDSA/desk-JWKS trust as a `vis_` (desk resolved the
	// tenant + embedded it, so RP stays DB-free); only the role + the absent cid differ. The default
	// (no/visitor role) stays the single-conversation visitor binding.
	if role == string(signaling.RoleAgent) {
		return signaling.Identity{TenantID: tid, UserID: sub, Role: signaling.RoleAgent}, nil
	}

	cid, _ := claimString(parsed, "cid")
	if cid == "" {
		return signaling.Identity{}, fmt.Errorf("%w: missing cid", ErrVisitorToken)
	}
	if err := validSubjectToken(cid); err != nil {
		return signaling.Identity{}, fmt.Errorf("%w: unsafe cid: %v", ErrVisitorToken, err)
	}

	return signaling.Identity{
		TenantID:       tid,
		UserID:         sub,
		Role:           signaling.RoleVisitor,
		ConversationID: cid,
		// The responder caps the minted credential at min(this, its ceiling) so a visitor connection
		// is short-lived + revocable (ADR-0012 §4).
		ExpiresAt: parsed.Expiration(),
	}, nil
}

// keySet returns a cached key set that contains kid; it refetches when the cache is empty, stale (past ttl),
// or missing kid (rotation). A refetch error fails closed — the caller rejects the token. The lock is held
// only for the in-memory cache reads/writes, NEVER across the HTTP fetch (DoS-hardening, cross-review).
func (v *VisitorVerifier) keySet(kid string) (jwk.Set, error) {
	v.mu.Lock()
	cached, fresh := v.cached, v.cached != nil && v.clock().Sub(v.fetchedAt) < v.ttl
	if fresh && cached != nil {
		if _, ok := cached.LookupKeyID(kid); ok {
			v.mu.Unlock()
			return cached, nil
		}
		// kid unknown despite a fresh cache ⇒ either a rotation since the last fetch OR a forged kid. Refetch
		// at most once per cooldown so a forged-kid flood cannot hammer the JWKS endpoint (negative throttle).
		if v.clock().Sub(v.lastUnknownPoll) < v.refetchMin {
			v.mu.Unlock()
			return nil, fmt.Errorf("unknown kid (refetch throttled)")
		}
		v.lastUnknownPoll = v.clock()
	}
	v.mu.Unlock()

	set, err := v.refetch()
	if err != nil {
		// Refetch failed: fail closed even if a stale cache exists — never verify against a key set we could
		// not re-confirm. (A stale key the token still matches would otherwise outlive a desk revocation.)
		return nil, err
	}
	if _, ok := set.LookupKeyID(kid); !ok {
		return nil, fmt.Errorf("unknown kid")
	}
	return set, nil
}

// refetch fetches + parses the JWKS and updates the cache. Concurrent refetches collapse into ONE upstream
// call via singleflight, so a burst of misses (rotation or flood) cannot fan out into many HTTP requests.
// The HTTP fetch runs WITHOUT the verifier lock; only the cache write re-acquires it briefly.
func (v *VisitorVerifier) refetch() (jwk.Set, error) {
	res, err, _ := v.group.Do("jwks", func() (interface{}, error) {
		ctx, cancel := v.fetchCtx()
		defer cancel()
		raw, err := v.src.Fetch(ctx)
		if err != nil {
			return nil, err
		}
		set, err := jwk.Parse(raw)
		if err != nil {
			return nil, err
		}
		v.mu.Lock()
		v.cached = set
		v.fetchedAt = v.clock()
		v.mu.Unlock()
		return set, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(jwk.Set), nil
}

func (v *VisitorVerifier) clock() time.Time {
	if v.now != nil {
		return v.now()
	}
	return time.Now()
}

// clockFunc adapts a now-func to the jwx jwt.Clock interface so expiry is validated against the verifier
// clock (deterministic in tests), not the process wall-clock.
type clockFunc func() time.Time

func (f clockFunc) Now() time.Time { return f() }

// headerKID extracts the `kid` from the token's first (protected) JWS header without verifying the
// signature, so an unknown kid can drive a rotation refetch.
func headerKID(token string) (string, error) {
	msg, err := jws.Parse([]byte(token))
	if err != nil {
		return "", fmt.Errorf("parse jws: %v", err)
	}
	sigs := msg.Signatures()
	if len(sigs) == 0 {
		return "", fmt.Errorf("no signature")
	}
	kid := sigs[0].ProtectedHeaders().KeyID()
	if kid == "" {
		return "", fmt.Errorf("no kid header")
	}
	if alg := sigs[0].ProtectedHeaders().Algorithm(); alg != jwa.EdDSA {
		return "", fmt.Errorf("unexpected alg")
	}
	return kid, nil
}

func claimString(t jwt.Token, key string) (string, bool) {
	raw, ok := t.Get(key)
	if !ok {
		return "", false
	}
	s, ok := raw.(string)
	return s, ok
}
