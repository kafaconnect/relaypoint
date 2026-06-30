package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsPath is mounted on the RH-06 health listener so the realtime plane's failure modes are scraped on the port already exposed for probes — no second port (RH-09).
const MetricsPath = "/metrics"

// Own registry (not the global default) so a scrape returns ONLY these named series, deterministic across binaries.
var metricsRegistry = prometheus.NewRegistry()

var (
	DLQRoutes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "relaypoint", Subsystem: "projector", Name: "dlq_routes_total",
		Help: "Facts routed to the dead-letter stream after exhausting redelivery.",
	})
	Naks = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "relaypoint", Subsystem: "projector", Name: "naks_total",
		Help: "Facts Nak'd for redelivery (roster blip, fence, transient publish failure).",
	})
	// Shared by the projector (feed publish) and the router (log-append OCC re-fold); a single binary scrapes its own meaning.
	PublishRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "relaypoint", Name: "publish_retries_total",
		Help: "JetStream publish attempts beyond the first (feed fan-out or log append).",
	})
	LeaseRenewRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "relaypoint", Subsystem: "projector", Name: "lease_renew_retries_total",
		Help: "Lease-renew attempts that failed and were retried within the fencing budget.",
	})
	RosterErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "relaypoint", Subsystem: "projector", Name: "roster_errors_total",
		Help: "Roster lookups that returned an error (desk roster 5xx/timeout).",
	})
	FanoutLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "relaypoint", Subsystem: "projector", Name: "fanout_latency_seconds",
		Help: "Wall time to fan a single fact out to all recipient feeds.", Buckets: prometheus.DefBuckets,
	})
)

func init() {
	metricsRegistry.MustRegister(DLQRoutes, Naks, PublishRetries, LeaseRenewRetries, RosterErrors, FanoutLatency)
}

// MetricsHandler serves the Prometheus exposition; read-only and fail-open like the rest of obs (ADR-0011 §9).
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{})
}
