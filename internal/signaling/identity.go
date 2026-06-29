package signaling

import (
	"context"
	"time"
)

// Role MUST come from the authenticated identity, never the payload; RoleTrustedBackend gates the privileged participation commands (agent-feed-fanout Decision 2).
type Role string

const (
	RoleAgent          Role = "agent"
	RoleTrustedBackend Role = "trusted-backend"
	// RoleVisitor (F1) is an end-user widget session, NOT an agent: a single-conversation subscribe-only grant (no feed/cmd/.log/other conversation), bound via ConversationID at mint time.
	RoleVisitor Role = "visitor"
)

// Identity is the trusted tenant/actor from the authenticated caller, not the client-controlled subject/payload (the router validates those against it); empty fields mean unauthenticated.
type Identity struct {
	TenantID string
	UserID   string
	Role     Role
	// ConversationID is set ONLY for RoleVisitor: the one conversation the `vis_` token is bound to, scoping its subscribe-only grant; agents and trusted backends leave it empty.
	ConversationID string
	// ExpiresAt caps a minted credential's lifetime, set ONLY for RoleVisitor (the `vis_` token exp); the responder caps the NATS credential at min(this, its ceiling) so visitor connections stay short-lived + revocable (ADR-0012 §4). Zero = no cap.
	ExpiresAt time.Time
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

func RoleOf(id Identity) Role {
	if id.Role != "" {
		return id.Role
	}
	return RoleAgent
}
