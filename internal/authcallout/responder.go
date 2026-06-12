package authcallout

import (
	"fmt"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// AuthRequestSubject is the NATS auth-callout request subject the server publishes to.
const AuthRequestSubject = "$SYS.REQ.USER.AUTH"

// Responder is the NATS adapter for the auth-callout: it answers $SYS.REQ.USER.AUTH by verifying the
// token (Verifier port) and minting a per-connection user JWT whose ACLs are pinned to that identity
// (GrantsFor policy). It mints the reply prefix `<conn>` itself, so `_INBOX_<conn>` is bound to this
// connection and not client-chosen. The NATS signing/decoding lives ONLY here (loose-coupling HARD RULE).
type Responder struct {
	verify  Verifier
	issuer  nkeys.KeyPair // account signing-key seed — the auth_callout issuer
	account string        // named account minted users land in (user JWT aud)
	connID  func() string
}

// NewResponder builds the responder. issuerSeed is the account signing-key SEED (the trust root; from
// a secret, never committed); account is the NAME of the account minted users are placed in.
func NewResponder(v Verifier, issuerSeed []byte, account string, opts ...ResponderOption) (*Responder, error) {
	kp, err := nkeys.FromSeed(issuerSeed)
	if err != nil {
		return nil, fmt.Errorf("authcallout: bad issuer seed: %w", err)
	}
	r := &Responder{verify: v, issuer: kp, account: account, connID: defaultConnID}
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

func defaultConnID() string { return nats.NewInbox()[len("_INBOX."):] }

// Subscribe wires the responder to the auth-callout request subject. The connection MUST be one of
// the config's `auth_users` (a trusted identity exempt from the callout) so the responder itself is
// not locked out at cutover.
func (r *Responder) Subscribe(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(AuthRequestSubject, func(m *nats.Msg) {
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
	userJWT, err := uc.Encode(r.issuer)
	if err != nil {
		return "", fmt.Errorf("encode user jwt: %w", err)
	}
	return r.respond(req, userJWT, "")
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
