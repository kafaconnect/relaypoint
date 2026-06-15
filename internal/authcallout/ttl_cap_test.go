package authcallout

import (
	"testing"
	"time"

	"github.com/nats-io/nkeys"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func testAccountSeed(t *testing.T) []byte {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return seed
}

// testResponder builds a Responder for the cappedExpiry unit (which never touches the issuer/verifier).
func testResponder(t *testing.T, opts ...ResponderOption) *Responder {
	t.Helper()
	r, err := NewResponder(&HMACVerifier{}, testAccountSeed(t), "RP", opts...)
	if err != nil {
		t.Fatalf("NewResponder: %v", err)
	}
	return r
}

// @spec:f1.visitor-credential-ttl-capped
func TestCappedExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }

	t.Run("no ExpiresAt (agent/backend) → no cap", func(t *testing.T) {
		r := testResponder(t, WithClock(clock))
		if got := r.cappedExpiry(signaling.Identity{Role: signaling.RoleAgent}); got != 0 {
			t.Fatalf("cappedExpiry = %d, want 0 (uncapped)", got)
		}
	})

	t.Run("vis_ exp within the ceiling → use the vis_ exp", func(t *testing.T) {
		r := testResponder(t, WithClock(clock), WithVisitorTTLCap(time.Hour))
		visExp := now.Add(10 * time.Minute)
		id := signaling.Identity{Role: signaling.RoleVisitor, ExpiresAt: visExp}
		if got := r.cappedExpiry(id); got != visExp.Unix() {
			t.Fatalf("cappedExpiry = %d, want %d (the vis_ exp)", got, visExp.Unix())
		}
	})

	t.Run("vis_ exp beyond the ceiling → cap at now+ceiling", func(t *testing.T) {
		r := testResponder(t, WithClock(clock), WithVisitorTTLCap(time.Hour))
		id := signaling.Identity{Role: signaling.RoleVisitor, ExpiresAt: now.Add(24 * time.Hour)}
		want := now.Add(time.Hour).Unix()
		if got := r.cappedExpiry(id); got != want {
			t.Fatalf("cappedExpiry = %d, want %d (ceiling)", got, want)
		}
	})

	t.Run("visitor with zero ExpiresAt → STILL capped at the ceiling (never bypassed)", func(t *testing.T) {
		r := testResponder(t, WithClock(clock), WithVisitorTTLCap(time.Hour))
		id := signaling.Identity{Role: signaling.RoleVisitor} // no ExpiresAt
		want := now.Add(time.Hour).Unix()
		if got := r.cappedExpiry(id); got != want {
			t.Fatalf("cappedExpiry = %d, want %d (ceiling — a visitor is never uncapped)", got, want)
		}
	})
}
