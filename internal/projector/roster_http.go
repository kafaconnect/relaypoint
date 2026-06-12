package projector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// DeskRoster is the live Roster adapter: it HTTP-pulls desk's tenant-roster endpoint
// (GET <baseURL>/tenants/{tid}/agents) with the shared service token, and caches the result per
// tenant for ttl. A cache miss or an expired entry refreshes; a successful refresh replaces the
// entry. Errors are not cached (a transient desk/Zitadel blip must not pin an empty roster). The
// concrete HTTP coupling lives only here — the projector core sees the Roster port (loose coupling).
type DeskRoster struct {
	baseURL string
	token   string
	client  *http.Client
	ttl     time.Duration
	now     func() time.Time

	mu      sync.Mutex
	entries map[string]rosterEntry
}

type rosterEntry struct {
	agents  []string
	expires time.Time
}

// NewDeskRoster fails closed on missing base URL or token (either would silently dark every feed).
func NewDeskRoster(baseURL, token string, ttl time.Duration, client *http.Client) (*DeskRoster, error) {
	if baseURL == "" || token == "" {
		return nil, fmt.Errorf("projector: desk roster base url and service token are both required")
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &DeskRoster{baseURL: baseURL, token: token, client: client, ttl: ttl, now: time.Now, entries: map[string]rosterEntry{}}, nil
}

type agentsResponse struct {
	TenantID string   `json:"tenant_id"`
	Agents   []string `json:"agents"`
}

func (d *DeskRoster) Agents(ctx context.Context, tenantID string) ([]string, error) {
	now := d.now()
	d.mu.Lock()
	if e, ok := d.entries[tenantID]; ok && now.Before(e.expires) {
		d.mu.Unlock()
		return e.agents, nil
	}
	d.mu.Unlock()

	agents, err := d.fetch(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.entries[tenantID] = rosterEntry{agents: agents, expires: now.Add(d.ttl)}
	d.mu.Unlock()
	return agents, nil
}

func (d *DeskRoster) fetch(ctx context.Context, tenantID string) ([]string, error) {
	endpoint := fmt.Sprintf("%s/tenants/%s/agents", d.baseURL, url.PathEscape(tenantID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("projector: build roster request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+d.token)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("projector: roster request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("projector: roster status %d for tenant %s", resp.StatusCode, tenantID)
	}
	var out agentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("projector: decode roster: %w", err)
	}
	return out.Agents, nil
}
