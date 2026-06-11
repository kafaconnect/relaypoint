package signaling

import "context"

// Role MUST come from the authenticated identity, never the payload; RoleTrustedBackend gates the
// privileged participation commands. See openspec change agent-feed-fanout (Decision 2).
type Role string

const (
	RoleAgent          Role = "agent"
	RoleTrustedBackend Role = "trusted-backend"
)

// Identity is the trusted source of tenant/actor — the authenticated caller, not the client-controlled
// subject or payload, which the router validates against it. Empty fields mean unauthenticated.
type Identity struct {
	TenantID string
	UserID   string
	Role     Role
}

type identityKey struct{}

func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

func IdentityFrom(ctx context.Context) Identity {
	if id, ok := ctx.Value(identityKey{}).(Identity); ok {
		return id
	}
	return Identity{}
}

// RoleOf returns the identity's role, defaulting an unset role to RoleAgent.
func RoleOf(id Identity) Role {
	if id.Role != "" {
		return id.Role
	}
	return RoleAgent
}
