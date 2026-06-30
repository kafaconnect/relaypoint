package obs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// @spec:obs.metrics.surface
func TestMetricsSurfaceExposesNamedSeries(t *testing.T) {
	// Drive each instrument so the exposition carries a non-zero sample, proving the wiring not just registration.
	DLQRoutes.Inc()
	Naks.Inc()
	PublishRetries.Inc()
	LeaseRenewRetries.Inc()
	RosterErrors.Inc()
	FanoutLatency.Observe(0.42)

	rec := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s = %d, want 200", MetricsPath, rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	got := string(body)

	for _, series := range []string{
		"relaypoint_projector_dlq_routes_total",
		"relaypoint_projector_naks_total",
		"relaypoint_publish_retries_total",
		"relaypoint_projector_lease_renew_retries_total",
		"relaypoint_projector_roster_errors_total",
		"relaypoint_projector_fanout_latency_seconds",
	} {
		if !strings.Contains(got, series) {
			t.Errorf("metrics surface is missing series %q", series)
		}
	}
	// the histogram must expose its derived count series so fan-out lag is alertable
	if !strings.Contains(got, "relaypoint_projector_fanout_latency_seconds_count") {
		t.Error("fan-out latency histogram exposed no _count series")
	}
}
