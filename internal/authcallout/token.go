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

// Verifier turns a connection's presented token into a trusted Identity. It is the SECURE source
// of the connection identity the responder pins the ACLs to — the same verify→identity seam the
// router consumes. RelayPoint owns this port; the concrete token format is swappable (a real
// deployment would back it with the issuer's JWKS / OIDC). NEVER trust the client-asserted subject
// or payload — only what Verify returns.
type Verifier interface {
	Verify(token string) (signaling.Identity, error)
}

// claims is the dev token body: an HMAC-signed `<base64(json)>.<base64(hmac)>` bearer. It carries
// the tenant/user/role the responder mints ACLs for and a relative expiry (server-validated, never
// client wall-clock — signaling-core time-authority rule). A production deployment swaps this for
// the issuer's verified JWT; the responder only depends on the Verifier port.
type claims struct {
	Tenant  string `json:"tenant"`
	User    string `json:"user"`
	Role    string `json:"role,omitempty"`
	ExpUnix int64  `json:"exp"`
}

// HMACVerifier validates the dev bearer token with a shared secret. The secret is operator-supplied
// (env, never committed) — it is the trust root that makes `<self>` airtight.
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
	role := signaling.RoleAgent
	if c.Role == string(signaling.RoleTrustedBackend) {
		role = signaling.RoleTrustedBackend
	}
	return signaling.Identity{TenantID: c.Tenant, UserID: c.User, Role: role}, nil
}

// MintDevToken builds a dev bearer for tests/tooling. NOT for production token issuance (that is the
// issuer's signed JWT). ttl is relative (server-validated), never a client wall-clock absolute.
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
