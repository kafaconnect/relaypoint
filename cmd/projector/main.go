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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/kafaconnect/relaypoint/internal/health"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"github.com/kafaconnect/relaypoint/internal/participationclientnats"
	"github.com/kafaconnect/relaypoint/internal/participationnats"
	"github.com/kafaconnect/relaypoint/internal/participationpg"
	"github.com/kafaconnect/relaypoint/internal/projector"
)

const defaultNATSUser = "projector"

func main() {
	if health.IsProbe(os.Args) {
		os.Exit(health.RunProbe(health.DefaultAddr))
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	slog.SetDefault(obs.New("relaypoint-projector"))

	// OTLP trace export (M1.5 F5b) — no-op when the OTLP endpoint is unset; fail-open on a setup error.
	tracerShutdown, terr := obs.InitTracer(context.Background(), "relaypoint-projector")
	if terr != nil {
		slog.Default().Warn("otel.init_failed_continuing_log_only", "err", terr)
		tracerShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = tracerShutdown(context.Background()) }()
	dsnPath, err := postgresDSNPath(os.Args[1:])
	must("postgres-config", err)
	pool, err := openProjectorPostgres(ctx, dsnPath)
	must("postgres", err)
	defer pool.Close()

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
	must("participation-stream", participationnats.EnsureStream(js))

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
	participationConsumer, err := newParticipationRuntime(nc, js, pool, slog.Default())
	must("participation-consumer", err)
	defer participationConsumer.Close()

	live := func() error {
		if !nc.IsConnected() {
			return errors.New("nats disconnected")
		}
		if _, jerr := js.AccountInfo(); jerr != nil {
			return fmt.Errorf("jetstream unreachable: %w", jerr)
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return pool.Ping(pingCtx)
	}
	ready := func() error {
		if err := p.Ready(); err != nil {
			return err
		}
		if err := participationConsumer.Ready(); err != nil {
			return err
		}
		return live()
	}
	go func() {
		if herr := health.Serve(ctx, cfg.HealthAddr, live, ready, obs.MetricsHandler()); herr != nil {
			slog.Error("health.serve", "err", herr)
		}
	}()

	slog.Info("projector.up", "url", url, "stream", "INTERACTION_LOGS", "feed_stream", "AGENT_FEED")
	if err := runProjectors(ctx, p.Run, participationConsumer.Run); err != nil && ctx.Err() == nil {
		slog.Error("projector.exit", "err", err)
		os.Exit(1)
	}
}

func newParticipationRuntime(nc *nats.Conn, js nats.JetStreamContext, pool *pgxpool.Pool, logger *slog.Logger) (*participationnats.Consumer, error) {
	readGrant := participation.TransportGrant{ServiceID: "relaypoint", Capability: participation.CapabilityRead}
	authority, err := participationclientnats.New(nc, 5*time.Second, readGrant)
	if err != nil {
		return nil, err
	}
	projector := participation.NewProjector(
		participationpg.New(pool), authority,
		func() string { return uuid.Must(uuid.NewV7()).String() },
	)
	writeGrant := participation.TransportGrant{ServiceID: "corex", Capability: participation.CapabilityWrite}
	return participationnats.NewConsumer(js, projector, writeGrant, logger)
}

func runProjectors(ctx context.Context, runners ...func(context.Context) error) error {
	if len(runners) == 0 {
		return errors.New("no projector runtimes configured")
	}
	for _, run := range runners {
		if run == nil {
			return errors.New("nil projector runtime")
		}
	}
	parent := ctx
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	results := make(chan error, len(runners))
	for _, run := range runners {
		go func(run func(context.Context) error) { results <- run(ctx) }(run)
	}
	collected := []error{<-results}
	cancel()
	for range runners[1:] {
		collected = append(collected, <-results)
	}
	var result error
	for _, err := range collected {
		if err != nil && !errors.Is(err, context.Canceled) {
			result = errors.Join(result, err)
		}
	}
	if result != nil {
		return result
	}
	if parent.Err() != nil {
		return nil
	}
	return errors.New("projector runtime stopped without an error")
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
