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

// F1 (desk design §3): RP verifies a desk-minted `vis_` (EdDSA, desk JWKS) but can NEVER mint one (owner-required asymmetry); verification fails CLOSED.

const (
	// Fixed iss/aud the desk ingress stamps on every `vis_` (desk visitor_exchange.go); a token differing on either is rejected.
	visitorIssuer   = "desk-ingress"
	visitorAudience = "relaypoint"
)

// ErrVisitorToken is the sentinel a visitor verify failure wraps (chain-distinguishable); it never carries key/token material.
var ErrVisitorToken = errors.New("authcallout: visitor token rejected")

// JWKSSource is the owned port for desk's key set (loose-coupling HARD RULE): I/O only, a non-nil error MUST fail closed; caching/rotation lives in VisitorVerifier (tests inject a fake, no network).
type JWKSSource interface {
	Fetch(ctx context.Context) ([]byte, error)
}

// Refetch is DoS-hardened (cross-review BLOCKER): the lock is NEVER held across the HTTP fetch, a singleflight collapses concurrent refetches, and a GLOBAL unknown-kid cooldown — one shared timer (lastUnknownPoll/refetchMin), NOT per-kid — throttles forged-kid floods (RH-11e).
type VisitorVerifier struct {
	src        JWKSSource
	ttl        time.Duration
	refetchMin time.Duration
	now        func() time.Time
	fetchCtx   func() (context.Context, context.CancelFunc)
	group      singleflight.Group

	mu              sync.Mutex
	cached          jwk.Set
	fetchedAt       time.Time
	lastUnknownPoll time.Time
}

type VisitorOption func(*VisitorVerifier)

func WithVisitorClock(now func() time.Time) VisitorOption {
	return func(v *VisitorVerifier) { v.now = now }
}

// Bounds each JWKS fetch so a hung desk endpoint can't stall the responder; a timeout is a fetch error → fail closed.
func WithVisitorFetchTimeout(d time.Duration) VisitorOption {
	return func(v *VisitorVerifier) {
		v.fetchCtx = func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), d)
		}
	}
}

// ttl bounds cached-key-set reuse before a time-based refetch; an unknown kid forces a refetch regardless of ttl (rotation safety).
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

// Sets the min interval between unknown-kid-driven refetches (negative throttle absorbing a forged-kid flood); a genuinely new kid still verifies on the next allowed refetch.
func WithVisitorRefetchCooldown(d time.Duration) VisitorOption {
	return func(v *VisitorVerifier) { v.refetchMin = d }
}

// Every failure path wraps ErrVisitorToken so no partial/unverified identity ever escapes — fail closed.
func (v *VisitorVerifier) Verify(token string) (signaling.Identity, error) {
	if token == "" {
		return signaling.Identity{}, fmt.Errorf("%w: empty token", ErrVisitorToken)
	}

	// Resolve the kid from the JWS header WITHOUT trusting the body, so an unknown kid can drive a rotation refetch before any signature work.
	kid, err := headerKID(token)
	if err != nil {
		return signaling.Identity{}, fmt.Errorf("%w: %v", ErrVisitorToken, err)
	}

	set, err := v.keySet(kid)
	if err != nil {
		return signaling.Identity{}, fmt.Errorf("%w: jwks: %v", ErrVisitorToken, err)
	}

	// EdDSA only — pin the alg so an `alg:none`/HMAC-confusion token is rejected; validate exp against the verifier clock (server time authority, never the client).
	// NO clock-skew leeway on exp/nbf is applied (RH-11f, deliberate): desk and RP share infra clocks, and zero leeway keeps a just-expired/revoked token from lingering — secure as-is.
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
	// tid/sub are interpolated into the minted ACL subjects — reject any non-safe-subject-token value before it reaches a grant (A6 injection guard).
	for _, c := range []struct{ k, s string }{{"tid", tid}, {"sub", sub}} {
		if err := validSubjectToken(c.s); err != nil {
			return signaling.Identity{}, fmt.Errorf("%w: unsafe %s: %v", ErrVisitorToken, c.k, err)
		}
	}

	// A desk-minted AGENT token (role=agent) carries no cid — its grant is the agent's own feed, same EdDSA/desk-JWKS trust as a `vis_` (desk embedded the tenant, so RP stays DB-free).
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
		// The responder caps the minted credential at min(this, its ceiling) so a visitor connection is short-lived + revocable (ADR-0012 §4).
		ExpiresAt: parsed.Expiration(),
	}, nil
}

// Refetches when the cache is empty/stale/missing-kid; a refetch error fails closed. The lock is NEVER held across the HTTP fetch (DoS-hardening, cross-review).
func (v *VisitorVerifier) keySet(kid string) (jwk.Set, error) {
	v.mu.Lock()
	cached, fresh := v.cached, v.cached != nil && v.clock().Sub(v.fetchedAt) < v.ttl
	if fresh && cached != nil {
		if _, ok := cached.LookupKeyID(kid); ok {
			v.mu.Unlock()
			return cached, nil
		}
		// Unknown kid despite a fresh cache = rotation OR a forged kid; refetch at most once per cooldown so a flood can't hammer the JWKS endpoint.
		if v.clock().Sub(v.lastUnknownPoll) < v.refetchMin {
			v.mu.Unlock()
			return nil, fmt.Errorf("unknown kid (refetch throttled)")
		}
		v.lastUnknownPoll = v.clock()
	}
	v.mu.Unlock()

	set, err := v.refetch()
	if err != nil {
		// Fail closed even if a stale cache exists — a stale key the token still matches would outlive a desk revocation.
		// OPERATIONAL DEPENDENCY (RH-11d): if desk's JWKS endpoint is unreachable past `ttl`, ALL visitor verifies fail closed
		// (an availability cliff). Runbook: alert on a sustained refetch-error rate; a bounded stale-grace window for still-exp-valid
		// tokens is a possible future softening, deliberately NOT taken here (security-over-availability: a revoked key must not linger).
		return nil, err
	}
	if _, ok := set.LookupKeyID(kid); !ok {
		return nil, fmt.Errorf("unknown kid")
	}
	return set, nil
}

// Concurrent refetches collapse into ONE upstream call via singleflight (a miss burst can't fan out); the HTTP fetch runs WITHOUT the verifier lock.
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

// clockFunc adapts a now-func to jwx's jwt.Clock so expiry is validated against the verifier clock (deterministic in tests), not the process wall-clock.
type clockFunc func() time.Time

func (f clockFunc) Now() time.Time { return f() }

// headerKID extracts the kid from the protected JWS header WITHOUT verifying the signature, so an unknown kid can drive a rotation refetch.
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
