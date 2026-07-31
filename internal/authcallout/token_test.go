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
	for _, role := range []string{"superuser", "visitor", "agent", ""} {
		tok := mintRawRoleToken(secret, "T", "mallory", role)
		if _, err := secure.Verify(tok); err == nil {
			t.Errorf("HMAC role %q must be rejected", role)
		}
	}
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
		{TenantID: "a.b", UserID: "alice", Capability: signaling.CapabilityAgentFeed},
		{TenantID: "T", UserID: "bob*", Capability: signaling.CapabilityAgentFeed},
		{TenantID: "T", UserID: "c d", Capability: signaling.CapabilityAgentFeed},
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
	tok, _ := MintDevToken(secret, signaling.Identity{
		TenantID: "T", UserID: "alice", Capability: signaling.CapabilityAgentFeed,
	}, time.Hour)
	id, err := v.Verify(tok)
	if err != nil || id.UserID != "alice" || id.Role != "" ||
		id.Capability != signaling.CapabilityAgentFeed {
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
	agentTok, err := MintDevToken(secret, signaling.Identity{
		TenantID: "T", UserID: "alice", Capability: signaling.CapabilityAgentFeed,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	secure := NewHMACVerifier(secret)
	if _, err := secure.Verify(beTok); err == nil {
		t.Fatal("default HMAC verifier must reject a self-asserted trusted-backend (prod posture)")
	}
	if id, err := secure.Verify(agentTok); err != nil ||
		id.Capability != signaling.CapabilityAgentFeed || id.Role != "" {
		t.Fatalf("default HMAC verifier must accept a subscriber capability: id=%+v err=%v", id, err)
	}

	dev := NewHMACVerifier(secret, AllowHMACTrustedBackend())
	if id, err := dev.Verify(beTok); err != nil || id.Role != signaling.RoleTrustedBackend {
		t.Fatalf("dev HMAC verifier must accept trusted-backend: id=%+v err=%v", id, err)
	}
}
