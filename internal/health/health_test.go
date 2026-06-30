package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type errString string

func (e errString) Error() string { return string(e) }

// @spec:obs.health.liveness-nats-js
func TestLivenessHealthyOnlyWhenNATSAndJetStreamReachable(t *testing.T) {
	var natsUp, jsUp bool
	live := func() error {
		if !natsUp {
			return errString("nats disconnected")
		}
		if !jsUp {
			return errString("jetstream unreachable")
		}
		return nil
	}
	h := Handler(live, live)

	for _, c := range []struct {
		nats, js bool
		want     int
	}{
		{true, true, http.StatusOK},
		{false, true, http.StatusServiceUnavailable},
		{true, false, http.StatusServiceUnavailable},
		{false, false, http.StatusServiceUnavailable},
	} {
		natsUp, jsUp = c.nats, c.js
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, livePath, nil))
		if rec.Code != c.want {
			t.Errorf("nats=%v js=%v: /healthz = %d, want %d", c.nats, c.js, rec.Code, c.want)
		}
	}
}

// @spec:obs.health.liveness-nats-js
func TestReadinessIndependentOfLiveness(t *testing.T) {
	h := Handler(func() error { return nil }, func() error { return errString("not ready") })

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, readyPath, nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 when readiness fails", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, livePath, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200 (liveness independent of readiness)", rec.Code)
	}
}
