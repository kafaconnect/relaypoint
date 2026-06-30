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

// Verifier is the owned port for the SECURE connection identity (swappable token format); never trust the client-asserted payload, only what Verify returns.
type Verifier interface {
	Verify(token string) (signaling.Identity, error)
}

// ChainVerifier is the F1 verify ladder (one token may be agent/backend OR a desk vis_): tries each in order, first success wins, returns the LAST error if all reject (fail closed).
type ChainVerifier struct {
	links []Verifier
}

// With zero links every Verify denies (fail closed).
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

// claims is the dev token body; expiry is server-validated against Now, never the client wall-clock (signaling-core time-authority rule).
type claims struct {
	Tenant  string `json:"tenant"`
	User    string `json:"user"`
	Role    string `json:"role,omitempty"`
	ExpUnix int64  `json:"exp"`
}

// HMACVerifier validates the dev bearer with an operator-supplied shared secret (env, never committed) — the trust root that makes `<self>` airtight.
type HMACVerifier struct {
	Secret []byte
	Now    func() time.Time
	// allowTrustedBackend permits role=trusted-backend over the process-wide HMAC secret; the zero value is the SECURE posture (rejected), set only in the dev posture (RH-08).
	allowTrustedBackend bool
}

// HMACOption configures the dev HMAC link; the zero verifier is the SECURE posture (no trusted-backend self-assertion).
type HMACOption func(*HMACVerifier)

// AllowHMACTrustedBackend permits role=trusted-backend over the process-wide HMAC secret. DEV ONLY — a holder could self-assert ANY tenant as a privileged backend; main.go enables it solely when JWKS is unconfigured (the dev posture) (RH-08).
func AllowHMACTrustedBackend() HMACOption {
	return func(v *HMACVerifier) { v.allowTrustedBackend = true }
}

func NewHMACVerifier(secret []byte, opts ...HMACOption) *HMACVerifier {
	v := &HMACVerifier{Secret: secret, Now: time.Now}
	for _, o := range opts {
		o(v)
	}
	return v
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
	// Fail closed on role: an unknown/empty/visitor claim must NOT silently become agent — that would
	// bypass the RH-08 grant-layer default (which only ever sees RoleAgent on this HMAC path) and violate
	// authcallout.role.fail-closed-unknown. The sole minter, MintDevToken, always emits a concrete role.
	var role signaling.Role
	switch c.Role {
	case string(signaling.RoleAgent):
		role = signaling.RoleAgent
	case string(signaling.RoleTrustedBackend):
		// Fail closed in prod: the process-wide HMAC secret must not self-assert the privileged trusted-backend role; main.go allows it only in the dev posture (no JWKS) (RH-08).
		if !v.allowTrustedBackend {
			return signaling.Identity{}, fmt.Errorf("authcallout: trusted-backend role not permitted over HMAC")
		}
		role = signaling.RoleTrustedBackend
	default:
		return signaling.Identity{}, fmt.Errorf("authcallout: role %q not permitted over HMAC", c.Role)
	}
	return signaling.Identity{TenantID: c.Tenant, UserID: c.User, Role: role}, nil
}

// Rejects a tenant/user that isn't a single safe NATS subject token: it's interpolated into ACL subjects, so a metachar would inject tokens/wildcards past the `<self>` pin (A6).
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

// MintDevToken builds a dev bearer for tests/tooling, NOT production issuance (that is the issuer's signed JWT).
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
