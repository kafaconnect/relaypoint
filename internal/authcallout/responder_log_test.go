package authcallout

import (
	"reflect"
	"testing"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func TestAuthAllowAttrsIncludesSubscriberCapability(t *testing.T) {
	id := signaling.Identity{
		TenantID:   "tenant-a",
		UserID:     "subscriber-a",
		Capability: signaling.CapabilityAgentFeed,
	}

	got := authAllowAttrs(id)
	want := []any{
		"tenant", "tenant-a",
		"user", "subscriber-a",
		"role", "",
		"capability", signaling.CapabilityAgentFeed,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("authAllowAttrs() = %#v, want %#v", got, want)
	}
}

func TestAuthAllowAttrsOmitsCapabilityForRoleIdentity(t *testing.T) {
	id := signaling.Identity{
		TenantID: "tenant-a",
		UserID:   "backend-a",
		Role:     signaling.RoleTrustedBackend,
	}

	got := authAllowAttrs(id)
	want := []any{
		"tenant", "tenant-a",
		"user", "backend-a",
		"role", string(signaling.RoleTrustedBackend),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("authAllowAttrs() = %#v, want %#v", got, want)
	}
}
