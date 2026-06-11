// Command projector runs the RelayPoint Participation/Fan-out service: a leased single-active
// worker that tails tenant.*.interaction.*.log and projects each fact into the feed of every
// currently-participating agent (openspec change agent-feed-fanout). Standby replicas contend for
// the NATS KV leader lease; only the holder projects.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/projector"
)

func main() {
	slog.SetDefault(obs.New("relaypoint-projector"))

	url := envOr("NATS_URL", nats.DefaultURL)
	user := envOr("NATS_USER", "router")
	pass := envOr("NATS_PASSWORD", "router-dev")

	nc, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name("relaypoint-projector"))
	must("connect", err)
	defer nc.Drain()

	js, err := nc.JetStream()
	must("jetstream", err)

	must("feed-stream", projector.EnsureFeedStream(js, time.Hour, 10*time.Minute))

	const maxDeliver = 5
	src, err := projector.NewLogSource(js, maxDeliver, 30*time.Second)
	must("log-source", err)
	sink := projector.NewFeedSink(js)
	lease, err := projector.NewLeaseStore(js, workerID(), 5*time.Second)
	must("lease", err)
	snaps, err := projector.NewSnapshotStore(js)
	must("snapshot-store", err)

	p := projector.New(src, sink, lease, snaps, projector.Config{MaxDeliver: maxDeliver})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

func must(label string, err error) {
	if err != nil {
		slog.Error("fatal", "at", label, "err", err)
		os.Exit(1)
	}
}
