package authcallout

import (
	"testing"
	"time"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// @spec:signaling.feed.cmd-identity-pinned (verify rejects unsafe claims)
// Verify rejects a signed token whose tenant/user is not a single safe NATS subject token, even
// though the HMAC is valid — the claim is interpolated into ACL subjects (A6).
func TestVerifyRejectsUnsafeClaims(t *testing.T) {
	secret := []byte("s")
	v := NewHMACVerifier(secret)
	for _, id := range []signaling.Identity{
		{TenantID: "a.b", UserID: "alice"},
		{TenantID: "T", UserID: "bob*"},
		{TenantID: "T", UserID: "c d"},
	} {
		tok, err := MintDevToken(secret, id, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := v.Verify(tok); err == nil {
			t.Errorf("Verify must reject unsafe claim %+v", id)
		}
	}
}

func TestVerifyAcceptsSafeClaims(t *testing.T) {
	secret := []byte("s")
	v := NewHMACVerifier(secret)
	tok, _ := MintDevToken(secret, signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent}, time.Hour)
	id, err := v.Verify(tok)
	if err != nil || id.UserID != "alice" {
		t.Fatalf("safe claim must verify, got id=%+v err=%v", id, err)
	}
}
