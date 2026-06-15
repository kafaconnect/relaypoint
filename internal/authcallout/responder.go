package authcallout

import (
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// AuthRequestSubject is the NATS auth-callout request subject the server publishes to.
const AuthRequestSubject = "$SYS.REQ.USER.AUTH"

// authQueue is the responder's queue group: with replicas>=2 all responders subscribe under it so
// exactly one answers each $SYS.REQ.USER.AUTH (HA without double-minting).
const authQueue = "authsvc"

// defaultVisitorTTLCap bounds a minted VISITOR credential's lifetime regardless of the vis_ token's
// own exp — a defense-in-depth ceiling (ADR-0012 §4: visitor creds are short-lived + revocable). NATS
// drops the connection at expiry; the client re-exchanges a fresh vis_.
const defaultVisitorTTLCap = time.Hour

// Responder is the NATS adapter for the auth-callout: it answers $SYS.REQ.USER.AUTH by verifying the
// token (Verifier port) and minting a per-connection user JWT whose ACLs are pinned to that identity
// (GrantsFor policy). It mints the reply prefix `<conn>` itself, so `_INBOX_<conn>` is bound to this
// connection and not client-chosen. The NATS signing/decoding lives ONLY here (loose-coupling HARD RULE).
type Responder struct {
	verify        Verifier
	issuer        nkeys.KeyPair // account signing-key seed — the auth_callout issuer
	account       string        // named account minted users land in (user JWT aud)
	connID        func() string
	visitorTTLCap time.Duration    // ceiling on a minted visitor credential's lifetime
	now           func() time.Time // clock (injectable for tests)
}

// NewResponder builds the responder. issuerSeed is the account signing-key SEED (the trust root; from
// a secret, never committed); account is the NAME of the account minted users are placed in.
func NewResponder(v Verifier, issuerSeed []byte, account string, opts ...ResponderOption) (*Responder, error) {
	kp, err := nkeys.FromSeed(issuerSeed)
	if err != nil {
		return nil, fmt.Errorf("authcallout: bad issuer seed: %w", err)
	}
	r := &Responder{
		verify:        v,
		issuer:        kp,
		account:       account,
		connID:        defaultConnID,
		visitorTTLCap: defaultVisitorTTLCap,
		now:           time.Now,
	}
	for _, o := range opts {
		o(r)
	}
	return r, nil
}

type ResponderOption func(*Responder)

// WithConnIDGen overrides the per-connection reply-prefix minter (tests inject a deterministic one).
func WithConnIDGen(gen func() string) ResponderOption {
	return func(r *Responder) { r.connID = gen }
}

// WithVisitorTTLCap overrides the ceiling on a minted visitor credential's lifetime.
func WithVisitorTTLCap(d time.Duration) ResponderOption {
	return func(r *Responder) {
		if d > 0 {
			r.visitorTTLCap = d
		}
	}
}

// WithClock overrides the responder clock (tests assert the TTL cap without sleeping).
func WithClock(now func() time.Time) ResponderOption {
	return func(r *Responder) {
		if now != nil {
			r.now = now
		}
	}
}

func defaultConnID() string { return nats.NewInbox()[len("_INBOX."):] }

// Subscribe wires the responder to the auth-callout request subject. The connection MUST be one of
// the config's `auth_users` (a trusted identity exempt from the callout) so the responder itself is
// not locked out at cutover.
func (r *Responder) Subscribe(nc *nats.Conn) (*nats.Subscription, error) {
	// QueueSubscribe (not Subscribe): replicas>=2 share the `authsvc` queue so exactly one answers
	// each request — HA without two responders minting for the same connect.
	return nc.QueueSubscribe(AuthRequestSubject, authQueue, func(m *nats.Msg) {
		token, err := r.handle(m.Data)
		if err != nil {
			token, _ = r.deny(m.Data, err.Error())
		}
		if token != "" {
			_ = m.Respond([]byte(token))
		}
	})
}

// handle returns the signed authorization-response JWT for the minted per-connection user; a
// verify/grant failure returns an error the caller turns into a signed DENY (not a timeout).
func (r *Responder) handle(reqJWT []byte) (string, error) {
	req, err := jwt.DecodeAuthorizationRequestClaims(string(reqJWT))
	if err != nil {
		return "", fmt.Errorf("decode auth request: %w", err)
	}
	id, err := r.verify.Verify(req.ConnectOptions.Token)
	if err != nil {
		return "", err
	}
	conn := r.connID()
	grant, err := GrantsFor(id, conn)
	if err != nil {
		return "", err
	}

	uc := jwt.NewUserClaims(req.UserNkey)
	uc.Name = id.TenantID + "/" + id.UserID
	uc.Audience = r.account
	uc.Pub.Allow = grant.PubAllow
	uc.Pub.Deny = grant.PubDeny
	uc.Sub.Allow = grant.SubAllow
	uc.Sub.Deny = grant.SubDeny
	// answer requests on the minted reply-prefix without widening publish to broad subjects — ONLY for
	// roles that do request/reply. A visitor is strictly subscribe-only: a response permission would let
	// an inbound event's `reply` subject become a one-shot publish path past the static PubDeny (cross-review).
	if grant.AllowResponses {
		uc.Resp = &jwt.ResponsePermission{MaxMsgs: 1}
	}
	if e := r.cappedExpiry(id); e != 0 {
		uc.Expires = e
	}
	userJWT, err := uc.Encode(r.issuer)
	if err != nil {
		return "", fmt.Errorf("encode user jwt: %w", err)
	}
	return r.respond(req, userJWT, "")
}

// cappedExpiry returns the Unix expiry a minted credential should carry. It keys off ROLE, not
// ExpiresAt: a VISITOR credential MUST always be time-bounded (ADR-0012 §4 short-lived + revocable)
// and can never bypass the cap — bounded by the RP ceiling and further by the vis_ token's exp WHEN
// present. Non-visitors (agents/backends) get 0 (no RP-imposed expiry); their own token auth gates
// the connect.
func (r *Responder) cappedExpiry(id signaling.Identity) int64 {
	if id.Role != signaling.RoleVisitor {
		return 0
	}
	exp := r.now().Add(r.visitorTTLCap)
	if !id.ExpiresAt.IsZero() && id.ExpiresAt.Before(exp) {
		exp = id.ExpiresAt // the vis_ exp is tighter than the ceiling
	}
	return exp.Unix()
}

func (r *Responder) deny(reqJWT []byte, reason string) (string, error) {
	req, err := jwt.DecodeAuthorizationRequestClaims(string(reqJWT))
	if err != nil {
		return "", err
	}
	return r.respond(req, "", reason)
}

// respond signs the AuthorizationResponse to the server NKEY (the request subject of the response is
// the server's id, per the auth-callout protocol).
func (r *Responder) respond(req *jwt.AuthorizationRequestClaims, userJWT, errMsg string) (string, error) {
	rc := jwt.NewAuthorizationResponseClaims(req.UserNkey)
	rc.Audience = req.Server.ID
	rc.Jwt = userJWT
	rc.Error = errMsg
	return rc.Encode(r.issuer)
}
