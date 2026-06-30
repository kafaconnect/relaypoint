package authcallout

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// mintRawRoleToken signs an HMAC dev token carrying an ARBITRARY role string. MintDevToken can't —
// it forces empty→agent via RoleOf — so this lets the test exercise Verify's role allowlist directly.
func mintRawRoleToken(secret []byte, tenant, user, role string) string {
	c := claims{Tenant: tenant, User: user, Role: role}
	body, _ := json.Marshal(c)
	b64 := base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(b64))
	return b64 + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// @spec:authcallout.role.fail-closed-unknown
func TestHMACVerifyFailsClosedOnUnknownRole(t *testing.T) {
	secret := []byte("s")
	secure := NewHMACVerifier(secret)
	// Unknown/empty/visitor roles must be REJECTED over HMAC, never silently mapped to agent (that
	// would bypass the RH-08 grant-layer default, which only ever sees RoleAgent on this path).
	for _, role := range []string{"superuser", "visitor", ""} {
		tok := mintRawRoleToken(secret, "T", "mallory", role)
		if _, err := secure.Verify(tok); err == nil {
			t.Errorf("HMAC role %q must be rejected, not granted agent", role)
		}
	}
	// An explicit agent still verifies (no over-tightening).
	if id, err := secure.Verify(mintRawRoleToken(secret, "T", "alice", "agent")); err != nil || id.Role != signaling.RoleAgent {
		t.Fatalf("explicit agent must verify: id=%+v err=%v", id, err)
	}
	// Trusted-backend gating is unchanged: rejected on the secure verifier, accepted on the dev one.
	beTok := mintRawRoleToken(secret, "T", "desk", string(signaling.RoleTrustedBackend))
	if _, err := secure.Verify(beTok); err == nil {
		t.Fatal("trusted-backend must stay rejected on the secure verifier")
	}
	dev := NewHMACVerifier(secret, AllowHMACTrustedBackend())
	if id, err := dev.Verify(beTok); err != nil || id.Role != signaling.RoleTrustedBackend {
		t.Fatalf("dev verifier must still accept trusted-backend: id=%+v err=%v", id, err)
	}
}

// @spec:signaling.feed.cmd-identity-pinned (verify rejects unsafe claims)
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

// @spec:authcallout.hmac.no-trusted-backend-prod
func TestHMACRejectsTrustedBackendByDefault(t *testing.T) {
	secret := []byte("s")
	beTok, err := MintDevToken(secret, signaling.Identity{TenantID: "T", UserID: "desk", Role: signaling.RoleTrustedBackend}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	agentTok, err := MintDevToken(secret, signaling.Identity{TenantID: "T", UserID: "alice", Role: signaling.RoleAgent}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// Prod posture: the default (secure) verifier must reject a self-asserted trusted-backend over the process-wide HMAC secret, but still verify an agent.
	secure := NewHMACVerifier(secret)
	if _, err := secure.Verify(beTok); err == nil {
		t.Fatal("default HMAC verifier must reject a self-asserted trusted-backend (prod posture)")
	}
	if id, err := secure.Verify(agentTok); err != nil || id.Role != signaling.RoleAgent {
		t.Fatalf("default HMAC verifier must still accept an agent: id=%+v err=%v", id, err)
	}

	// Dev posture: the opt-in verifier mints trusted-backend (local wiring through the callout).
	dev := NewHMACVerifier(secret, AllowHMACTrustedBackend())
	if id, err := dev.Verify(beTok); err != nil || id.Role != signaling.RoleTrustedBackend {
		t.Fatalf("dev HMAC verifier must accept trusted-backend: id=%+v err=%v", id, err)
	}
}
