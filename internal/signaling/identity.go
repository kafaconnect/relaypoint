package signaling

import "context"

// Identity is the AUTHENTICATED caller, derived from the connection (Phase-1: minimal;
// auth-callout-minted JWT later). It is the trusted source of tenant/actor — NOT the
// subject (client-controlled) and NOT the command payload (client-supplied). The
// router validates the subject and payload AGAINST this identity.
type Identity struct {
	TenantID string // authenticated tenant; "" if the transport could not authenticate one
	UserID   string // authenticated user; "" if not yet bound (pre auth-callout)
}

type identityKey struct{}

// WithIdentity attaches the authenticated identity to a context (set by the transport).
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFrom returns the authenticated identity from the context.
func IdentityFrom(ctx context.Context) Identity {
	if id, ok := ctx.Value(identityKey{}).(Identity); ok {
		return id
	}
	return Identity{}
}
