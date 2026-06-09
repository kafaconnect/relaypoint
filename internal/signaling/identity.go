package signaling

import "context"

// Identity is the trusted source of tenant/actor — the authenticated caller, NOT the
// client-controlled subject or payload, which the router validates against it.
type Identity struct {
	TenantID string // "" if the transport authenticated no tenant
	UserID   string // "" if not yet bound (pre auth-callout)
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
