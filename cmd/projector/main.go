// Command projector runs the RelayPoint participation/fan-out service: a leased single-active worker projecting each interaction fact into every participating agent's feed (openspec change agent-feed-fanout).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/kafaconnect/relaypoint/internal/health"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/projector"
)

const (
	defaultNATSUser     = "projector"
	defaultNATSPassword = "projector-dev"
)

func main() {
	if health.IsProbe(os.Args) {
		os.Exit(health.RunProbe(health.DefaultAddr))
	}
	slog.SetDefault(obs.New("relaypoint-projector"))

	// OTLP trace export (M1.5 F5b) — no-op when the OTLP endpoint is unset; fail-open on a setup error.
	tracerShutdown, terr := obs.InitTracer(context.Background(), "relaypoint-projector")
	if terr != nil {
		slog.Default().Warn("otel.init_failed_continuing_log_only", "err", terr)
		tracerShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = tracerShutdown(context.Background()) }()

	url := envOr("NATS_URL", nats.DefaultURL)
	user := envOr("NATS_USER", defaultNATSUser)
	pass := envOr("NATS_PASSWORD", defaultNATSPassword)

	nc, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name("relaypoint-projector"))
	must("connect", err)
	defer nc.Drain()

	js, err := nc.JetStream()
	must("jetstream", err)
	jsKV, err := jetstream.New(nc)
	must("jetstream-kv", err)

	must("feed-stream", projector.EnsureFeedStream(js, time.Hour, 10*time.Minute))

	const maxDeliver = 5
	const leaseTTL = 5 * time.Second // shared by the lease and the renew budget so they cannot drift
	src, err := projector.NewLogSource(js, maxDeliver, 30*time.Second)
	must("log-source", err)
	sink := projector.NewFeedSink(js)
	lease, err := projector.NewLeaseStore(jsKV, workerID(), leaseTTL)
	must("lease", err)
	snaps, err := projector.NewSnapshotStore(jsKV)
	must("snapshot-store", err)

	cfg := projector.Config{MaxDeliver: maxDeliver, LeaseTTL: leaseTTL, HealthAddr: health.DefaultAddr}
	switch os.Getenv("PROJECTOR_FANOUT_MODE") {
	case "tenant-roster":
		// production: a tenant's agents come from desk's real roster (its Zitadel org membership), never hardcoded.
		dr, err := projector.NewDeskRoster(
			mustEnv("DESK_ROSTER_URL"),
			mustEnv("DESK_ROSTER_TOKEN"),
			rosterCacheTTL(),
			&http.Client{Timeout: 10 * time.Second})
		must("desk-roster", err)
		cfg.Roster = dr
		slog.Info("projector.tenant-roster", "url", os.Getenv("DESK_ROSTER_URL"), "cache_ttl", rosterCacheTTL().String())
	case "tenant-wide":
		// dev shortcut: a static roster bypasses the participation gate, which stays empty until desk emits facts.
		cfg.TenantWideAgents = parseTenantAgents(os.Getenv("PROJECTOR_TENANT_AGENTS"))
		slog.Warn("projector.tenant-wide", "tenants", len(cfg.TenantWideAgents), "note", "participation gate bypassed")
	}
	p := projector.New(src, sink, lease, snaps, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	live := func() error {
		if !nc.IsConnected() {
			return errors.New("nats disconnected")
		}
		if _, jerr := js.AccountInfo(); jerr != nil {
			return fmt.Errorf("jetstream unreachable: %w", jerr)
		}
		return nil
	}
	go func() {
		if herr := health.Serve(ctx, cfg.HealthAddr, live, p.Ready, obs.MetricsHandler()); herr != nil {
			slog.Error("health.serve", "err", herr)
		}
	}()

	slog.Info("projector.up", "url", url, "stream", "INTERACTION_LOGS", "feed_stream", "AGENT_FEED")
	if err := p.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("projector.exit", "err", err)
		os.Exit(1)
	}
}

func parseTenantAgents(csv string) map[string][]string {
	out := map[string][]string{}
	for _, pair := range strings.Split(csv, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		tid, agent, ok := strings.Cut(pair, ":")
		tid, agent = strings.TrimSpace(tid), strings.TrimSpace(agent)
		if !ok || tid == "" || agent == "" {
			continue
		}
		out[tid] = append(out[tid], agent)
	}
	return out
}

func workerID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%s", host, uuid.Must(uuid.NewV7()))
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("fatal", "at", "config", "err", "missing required env var: "+k)
		os.Exit(1)
	}
	return v
}

func rosterCacheTTL() time.Duration {
	if raw := os.Getenv("DESK_ROSTER_TTL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			return d
		}
		slog.Warn("projector.roster_ttl.invalid_env", "value", raw)
	}
	return 60 * time.Second
}

func must(label string, err error) {
	if err != nil {
		slog.Error("fatal", "at", label, "err", err)
		os.Exit(1)
	}
}
