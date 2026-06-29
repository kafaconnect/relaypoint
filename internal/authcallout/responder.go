package authcallout

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// traceparentOf is nil-safe (a header-less message yields "") so the auth decision can join an inbound trace if one is propagated.
func traceparentOf(m *nats.Msg) string {
	if m == nil || m.Header == nil {
		return ""
	}
	return m.Header.Get("traceparent")
}

const AuthRequestSubject = "$SYS.REQ.USER.AUTH"

// authQueue: replicas>=2 share this queue group so exactly one responder answers each request (HA without double-minting).
const authQueue = "authsvc"

// defaultVisitorTTLCap bounds a minted visitor credential regardless of the vis_ exp — defense-in-depth ceiling (ADR-0012 §4: short-lived + revocable).
const defaultVisitorTTLCap = time.Hour

// Responder mints the reply prefix `<conn>` itself so `_INBOX_<conn>` is bound to the connection, not client-chosen; NATS signing/decoding lives ONLY here (loose-coupling HARD RULE).
type Responder struct {
	verify        Verifier
	issuer        nkeys.KeyPair
	account       string
	connID        func() string
	visitorTTLCap time.Duration
	now           func() time.Time
	log           *slog.Logger
}

// issuerSeed is the account signing-key SEED (trust root; from a secret, never committed); account is the NAME minted users are placed in.
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
		log:           slog.Default(),
	}
	for _, o := range opts {
		o(r)
	}
	return r, nil
}

func WithLogger(l *slog.Logger) ResponderOption {
	return func(r *Responder) {
		if l != nil {
			r.log = l
		}
	}
}

type ResponderOption func(*Responder)

func WithConnIDGen(gen func() string) ResponderOption {
	return func(r *Responder) { r.connID = gen }
}

func WithVisitorTTLCap(d time.Duration) ResponderOption {
	return func(r *Responder) {
		if d > 0 {
			r.visitorTTLCap = d
		}
	}
}

func WithClock(now func() time.Time) ResponderOption {
	return func(r *Responder) {
		if now != nil {
			r.now = now
		}
	}
}

func defaultConnID() string { return nats.NewInbox()[len("_INBOX."):] }

// The connection MUST be one of the config's `auth_users` (exempt from the callout) or the responder locks itself out at cutover.
func (r *Responder) Subscribe(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.QueueSubscribe(AuthRequestSubject, authQueue, func(m *nats.Msg) {
		// Open a span correlated to any inbound trace so each auth decision is observable (no-op export when no OTLP endpoint / inbound trace).
		ctx := obs.WithCorrelation(obs.ContextFromTraceparent(context.Background(), traceparentOf(m)), r.log)
		ctx, end := obs.StartSpan(ctx, "authcallout.handle")
		defer end()

		token, id, err := r.handle(m.Data)
		if err != nil {
			// Reason only — NEVER the token or any credential material.
			obs.Logger(ctx).Warn("authcallout.deny", "reason", err.Error())
			token, _ = r.deny(m.Data, err.Error())
		} else {
			obs.Logger(ctx).Info("authcallout.allow",
				"tenant", id.TenantID, "user", id.UserID, "role", string(signaling.RoleOf(id)))
		}
		if token != "" {
			_ = m.Respond([]byte(token))
		}
	})
}

// A verify/grant failure returns an error the caller turns into a signed DENY, not a timeout.
func (r *Responder) handle(reqJWT []byte) (string, signaling.Identity, error) {
	req, err := jwt.DecodeAuthorizationRequestClaims(string(reqJWT))
	if err != nil {
		return "", signaling.Identity{}, fmt.Errorf("decode auth request: %w", err)
	}
	id, err := r.verify.Verify(req.ConnectOptions.Token)
	if err != nil {
		return "", signaling.Identity{}, err
	}
	conn := r.connID()
	grant, err := GrantsFor(id, conn)
	if err != nil {
		return "", id, err
	}

	uc := jwt.NewUserClaims(req.UserNkey)
	uc.Name = id.TenantID + "/" + id.UserID
	uc.Audience = r.account
	uc.Pub.Allow = grant.PubAllow
	uc.Pub.Deny = grant.PubDeny
	uc.Sub.Allow = grant.SubAllow
	uc.Sub.Deny = grant.SubDeny
	// Response perm only for roles that do request/reply: a visitor is subscribe-only, else an inbound reply becomes a one-shot publish path past PubDeny (cross-review).
	if grant.AllowResponses {
		uc.Resp = &jwt.ResponsePermission{MaxMsgs: 1}
	}
	if e := r.cappedExpiry(id); e != 0 {
		uc.Expires = e
	}
	userJWT, err := uc.Encode(r.issuer)
	if err != nil {
		return "", id, fmt.Errorf("encode user jwt: %w", err)
	}
	token, err := r.respond(req, userJWT, "")
	return token, id, err
}

// Keys off ROLE not ExpiresAt: a VISITOR is ALWAYS capped (ADR-0012 §4), never bypassed; non-visitors get 0 (their own token auth gates the connect).
func (r *Responder) cappedExpiry(id signaling.Identity) int64 {
	if id.Role != signaling.RoleVisitor {
		return 0
	}
	exp := r.now().Add(r.visitorTTLCap)
	if !id.ExpiresAt.IsZero() && id.ExpiresAt.Before(exp) {
		exp = id.ExpiresAt
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

// Signs the AuthorizationResponse to the server NKEY — Audience is the server's id, per the auth-callout protocol.
func (r *Responder) respond(req *jwt.AuthorizationRequestClaims, userJWT, errMsg string) (string, error) {
	rc := jwt.NewAuthorizationResponseClaims(req.UserNkey)
	rc.Audience = req.Server.ID
	rc.Jwt = userJWT
	rc.Error = errMsg
	return rc.Encode(r.issuer)
}
