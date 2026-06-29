package authcallout

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// HTTPJWKSSource is the only network adapter behind the JWKSSource port (loose-coupling HARD RULE); the verifier's caching/rotation never touches the wire, so tests inject a fake and stay offline.
type HTTPJWKSSource struct {
	url    string
	client *http.Client
}

// A nil client uses http.DefaultClient; production callers pass a timeout-bounded one.
func NewHTTPJWKSSource(url string, client *http.Client) *HTTPJWKSSource {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPJWKSSource{url: url, client: client}
}

// Any non-2xx/transport/read error returns non-nil so the verifier fails closed; the body is never logged (no-material logging rule).
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
