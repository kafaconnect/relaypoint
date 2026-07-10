// Command projector runs the RelayPoint participation/fan-out service: a leased single-active worker projecting each interaction fact into every participating agent's feed (openspec change agent-feed-fanout).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/kafaconnect/relaypoint/internal/health"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/projector"
)

const defaultNATSUser = "projector"

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
	pass := mustEnv("NATS_PASSWORD") // RH-11h: fail loud, never default to a shared dev credential

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

	// Fan-out is structural-only: recipients come from folded participation (coveredBy). The former
	// tenant-wide roster override (a desk product rule) was removed at the substrate boundary (SBI-03);
	// desk now emits participation facts, so no roster pull remains on this path.
	cfg := projector.Config{MaxDeliver: maxDeliver, LeaseTTL: leaseTTL, HealthAddr: health.DefaultAddr}
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

func must(label string, err error) {
	if err != nil {
		slog.Error("fatal", "at", label, "err", err)
		os.Exit(1)
	}
}
