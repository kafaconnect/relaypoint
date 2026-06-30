// Package health serves the liveness/readiness surface for the RelayPoint binaries: liveness reflects NATS + JetStream reachability, readiness reflects each binary's owned ports (e.g. the projector's leader lease) — RH-06.
package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

const (
	// WHY: a fixed default port (not an env var) honours the no-new-env rule; k8s/compose probes target it.
	DefaultAddr = ":8222"
	livePath    = "/healthz"
	readyPath   = "/readyz"
	metricsPath = "/metrics"
	probeFlag   = "-healthcheck"
)

type Check func() error

// metrics is mounted on this same listener (not a second port) so RH-09's scrape rides the probe port; nil leaves only the probes.
func Handler(live, ready Check, metrics http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(livePath, probe(live))
	mux.HandleFunc(readyPath, probe(ready))
	if metrics != nil {
		mux.Handle(metricsPath, metrics)
	}
	return mux
}

func probe(c Check) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if c != nil {
			if err := c(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// WHY: return the bind error (never panic) so the caller can log-and-continue — telemetry must not crash the service.
func Serve(ctx context.Context, addr string, live, ready Check, metrics http.Handler) error {
	if addr == "" {
		addr = DefaultAddr
	}
	srv := &http.Server{Addr: addr, Handler: Handler(live, ready, metrics), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// WHY: distroless images carry no shell/wget, so the binary self-probes its own liveness for the container HEALTHCHECK.
func IsProbe(args []string) bool {
	for _, a := range args {
		if a == probeFlag {
			return true
		}
	}
	return false
}

func RunProbe(addr string) int {
	if addr == "" {
		addr = DefaultAddr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 1
	}
	if host == "" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + livePath)
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
