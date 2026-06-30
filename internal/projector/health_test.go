package projector

import (
	"context"
	"strings"
	"testing"
)

// @spec:obs.health.readiness-reflects-lease
func TestReadyReflectsLeaseState(t *testing.T) {
	p := New(nil, nil, nil, nil, Config{})

	if err := p.Ready(); err == nil {
		t.Fatal("Ready() must FAIL while wedged/standby (no lease held yet)")
	}

	f := newFence(context.Background())
	p.fenceP.Store(f)
	if err := p.Ready(); err != nil {
		t.Fatalf("Ready() as the active leader = %v, want nil", err)
	}

	f.pause()
	if err := p.Ready(); err == nil {
		t.Fatal("Ready() must FAIL while paused (lease renew stalled)")
	}

	f.resume()
	if err := p.Ready(); err != nil {
		t.Fatalf("Ready() after resume = %v, want nil", err)
	}

	f.fail(context.Canceled)
	if err := p.Ready(); err == nil || !strings.Contains(err.Error(), "lease lost") {
		t.Fatalf("Ready() after lease loss = %v, want a lease-lost error", err)
	}
}
