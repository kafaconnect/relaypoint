package signaling

import "context"

// Role distinguishes an ordinary agent connection from a trusted backend (Desk). Privileged
// participation commands (participant.assign/unassign/transfer) require RoleTrustedBackend; the
// role MUST come from the authenticated identity, NEVER from the payload. See openspec change
// agent-feed-fanout (Decision 2).
type Role string

const (
	RoleAgent          Role = "agent"           // default — an ordinary agent connection
	RoleTrustedBackend Role = "trusted-backend" // Desk: may issue privileged participation commands
)

// Identity is the trusted source of tenant/actor — the authenticated caller, NOT the
// client-controlled subject or payload, which the router validates against it.
type Identity struct {
	TenantID string // "" if the transport authenticated no tenant
	UserID   string // "" if not yet bound (pre auth-callout)
	Role     Role   // "" defaults to RoleAgent; RoleTrustedBackend gates privileged commands
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
