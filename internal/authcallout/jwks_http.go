package authcallout

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// HTTPJWKSSource fetches desk's published visitor JWKS over HTTP (the in-cluster desk-api Service's
// GET /ingress/rp-jwks endpoint, design §3 / desk visitor_exchange.go JWKS()). It is the only network
// adapter behind the JWKSSource port; the verifier's caching/rotation policy never touches the wire, so
// unit tests inject a fake source and stay offline (loose-coupling HARD RULE).
type HTTPJWKSSource struct {
	url    string
	client *http.Client
}

// NewHTTPJWKSSource builds the source for the configured URL (DESK_INGRESS_JWKS_URL). A nil client uses
// http.DefaultClient; callers pass a timeout-bounded client in production.
func NewHTTPJWKSSource(url string, client *http.Client) *HTTPJWKSSource {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPJWKSSource{url: url, client: client}
}

// Fetch GETs the JWKS bytes. Any non-2xx, transport error, or read error returns a non-nil error so the
// verifier fails the token closed. The body is NOT logged (it is public key material, but the no-material
// logging rule applies regardless).
func (s *HTTPJWKSSource) Fetch(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("authcallout: jwks fetch status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}
