package projector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDeskRoster_FetchesAndCaches(t *testing.T) {
	var hits int
	var gotPath, gotAuth string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(agentsResponse{TenantID: "t1", Agents: []string{"sub-a", "sub-b"}})
	}))
	defer stub.Close()

	dr, err := NewDeskRoster(stub.URL, "svc-tok", time.Minute, stub.Client())
	if err != nil {
		t.Fatalf("NewDeskRoster: %v", err)
	}
	now := time.Unix(0, 0)
	dr.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		got, err := dr.Agents(context.Background(), "t1")
		if err != nil {
			t.Fatalf("Agents: %v", err)
		}
		if len(got) != 2 || got[0] != "sub-a" || got[1] != "sub-b" {
			t.Fatalf("agents = %v", got)
		}
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1 (cached within TTL)", hits)
	}
	if gotPath != "/tenants/t1/agents" {
		t.Fatalf("path = %q, want /tenants/t1/agents", gotPath)
	}
	if gotAuth != "Bearer svc-tok" {
		t.Fatalf("auth = %q", gotAuth)
	}

	now = now.Add(2 * time.Minute) // past TTL → refresh
	if _, err := dr.Agents(context.Background(), "t1"); err != nil {
		t.Fatalf("Agents after TTL: %v", err)
	}
	if hits != 2 {
		t.Fatalf("upstream hits = %d, want 2 (refresh after TTL)", hits)
	}
}

func TestDeskRoster_Non200IsError(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer stub.Close()

	dr, _ := NewDeskRoster(stub.URL, "svc-tok", time.Minute, stub.Client())
	if _, err := dr.Agents(context.Background(), "t1"); err == nil {
		t.Fatal("expected error on non-200 roster response")
	}
}

func TestNewDeskRoster_FailsClosed(t *testing.T) {
	if _, err := NewDeskRoster("", "tok", time.Minute, nil); err == nil {
		t.Fatal("expected error on empty base url")
	}
	if _, err := NewDeskRoster("http://x", "", time.Minute, nil); err == nil {
		t.Fatal("expected error on empty token")
	}
}
