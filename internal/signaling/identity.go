package signaling

import (
	"context"
	"time"
)

// Role MUST come from the authenticated identity, never the payload; RoleTrustedBackend gates the
// privileged participation commands. See openspec change agent-feed-fanout (Decision 2).
type Role string

const (
	RoleAgent          Role = "agent"
	RoleTrustedBackend Role = "trusted-backend"
	// RoleVisitor is an end-user widget session (F1): NOT an agent. Its grant is a single
	// conversation, subscribe-only (no feed, no cmd, no .log, no other conversation). The
	// ConversationID field carries the one conversation it is bound to at mint time.
	RoleVisitor Role = "visitor"
)

// Identity is the trusted source of tenant/actor — the authenticated caller, not the client-controlled
// subject or payload, which the router validates against it. Empty fields mean unauthenticated.
type Identity struct {
	TenantID string
	UserID   string
	Role     Role
	// ConversationID is set ONLY for RoleVisitor: the one conversation a `vis_` token is bound to
	// (the token's server-resolved `cid`). It scopes the visitor's subscribe-only grant; agents and
	// trusted backends leave it empty.
	ConversationID string
	// ExpiresAt is the upper bound on a minted credential's lifetime, set ONLY for RoleVisitor (from
	// the `vis_` token's exp). The responder caps the minted NATS credential at min(this, its ceiling)
	// so a visitor connection is short-lived + revocable (ADR-0012 §4). Zero = no cap.
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

// RoleOf returns the identity's role, defaulting an unset role to RoleAgent.
func RoleOf(id Identity) Role {
	if id.Role != "" {
		return id.Role
	}
	return RoleAgent
}
