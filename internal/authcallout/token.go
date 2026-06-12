package authcallout

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

// Verifier is the SECURE source of the connection identity the responder pins ACLs to — an owned
// port so the token format is swappable (a real deployment backs it with the issuer's JWKS/OIDC).
// Never trust the client-asserted subject/payload, only what Verify returns.
type Verifier interface {
	Verify(token string) (signaling.Identity, error)
}

// ChainVerifier is the F1 verify ladder: RP is the SOLE responder, so one connection token may be an
// agent/trusted-backend token OR a desk visitor `vis_` token. It tries each Verifier in order and returns
// the first success; if every link rejects, the LAST error is returned (the responder turns it into a
// signed DENY). The order is fixed at construction — put the cheapest/most-common first. No link's failure
// short-circuits the others, but a success is final (fail closed only when ALL reject).
type ChainVerifier struct {
	links []Verifier
}

// NewChainVerifier builds the ladder. With zero links every Verify denies (fail closed).
func NewChainVerifier(links ...Verifier) *ChainVerifier {
	return &ChainVerifier{links: links}
}

func (c *ChainVerifier) Verify(token string) (signaling.Identity, error) {
	var lastErr error = fmt.Errorf("authcallout: no verifier accepted the token")
	for _, l := range c.links {
		id, err := l.Verify(token)
		if err == nil {
			return id, nil
		}
		lastErr = err
	}
	return signaling.Identity{}, lastErr
}

// claims is the dev token body, an HMAC-signed `<base64(json)>.<base64(hmac)>` bearer. The expiry is
// server-validated against Now, never a client wall-clock (signaling-core time-authority rule).
type claims struct {
	Tenant  string `json:"tenant"`
	User    string `json:"user"`
	Role    string `json:"role,omitempty"`
	ExpUnix int64  `json:"exp"`
}

// HMACVerifier validates the dev bearer with an operator-supplied shared secret (env, never
// committed) — the trust root that makes `<self>` airtight.
type HMACVerifier struct {
	Secret []byte
	Now    func() time.Time
}

func NewHMACVerifier(secret []byte) *HMACVerifier {
	return &HMACVerifier{Secret: secret, Now: time.Now}
}

func (v *HMACVerifier) Verify(token string) (signaling.Identity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return signaling.Identity{}, fmt.Errorf("authcallout: malformed token")
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return signaling.Identity{}, fmt.Errorf("authcallout: bad token body: %w", err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return signaling.Identity{}, fmt.Errorf("authcallout: bad token sig: %w", err)
	}
	mac := hmac.New(sha256.New, v.Secret)
	mac.Write([]byte(parts[0]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return signaling.Identity{}, fmt.Errorf("authcallout: token signature mismatch")
	}
	var c claims
	if err := json.Unmarshal(body, &c); err != nil {
		return signaling.Identity{}, fmt.Errorf("authcallout: bad claims: %w", err)
	}
	now := time.Now
	if v.Now != nil {
		now = v.Now
	}
	if c.ExpUnix != 0 && now().Unix() > c.ExpUnix {
		return signaling.Identity{}, fmt.Errorf("authcallout: token expired")
	}
	if c.Tenant == "" || c.User == "" {
		return signaling.Identity{}, fmt.Errorf("authcallout: token missing tenant/user")
	}
	if err := validSubjectToken(c.Tenant); err != nil {
		return signaling.Identity{}, fmt.Errorf("authcallout: invalid tenant: %w", err)
	}
	if err := validSubjectToken(c.User); err != nil {
		return signaling.Identity{}, fmt.Errorf("authcallout: invalid user: %w", err)
	}
	role := signaling.RoleAgent
	if c.Role == string(signaling.RoleTrustedBackend) {
		role = signaling.RoleTrustedBackend
	}
	return signaling.Identity{TenantID: c.Tenant, UserID: c.User, Role: role}, nil
}

// validSubjectToken rejects a tenant/user that cannot safely be a single NATS subject token: the
// claims are interpolated into ACL subjects, so a `.`/`*`/`>`/whitespace/control value would inject
// extra tokens or wildcards and break the `<self>`-pinned grants (openspec change agent-feed-fanout, A6).
func validSubjectToken(s string) error {
	if s == "" {
		return fmt.Errorf("empty")
	}
	for _, r := range s {
		switch {
		case r == '.' || r == '*' || r == '>':
			return fmt.Errorf("contains NATS subject metacharacter %q", r)
		case r <= ' ' || r == 0x7f:
			return fmt.Errorf("contains control/whitespace")
		}
	}
	return nil
}

// MintDevToken builds a dev bearer for tests/tooling, NOT production issuance (that is the issuer's
// signed JWT).
func MintDevToken(secret []byte, id signaling.Identity, ttl time.Duration) (string, error) {
	c := claims{Tenant: id.TenantID, User: id.UserID, Role: string(signaling.RoleOf(id))}
	if ttl > 0 {
		c.ExpUnix = time.Now().Add(ttl).Unix()
	}
	body, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	b64 := base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(b64))
	return b64 + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}
