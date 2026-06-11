// Command router runs the RelayPoint interaction service — the sole writer of `.log`.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func main() {
	slog.SetDefault(obs.New("relaypoint-router"))

	url := envOr("NATS_URL", nats.DefaultURL)
	user := envOr("NATS_USER", "router")
	pass := envOr("NATS_PASSWORD", "router-dev")

	nc, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name("relaypoint-router"))
	must("connect", err)
	defer nc.Drain()

	js, err := nc.JetStream()
	must("jetstream", err)
	// ADR-0002 protobuf cutover: a one-shot, destructive reset of INTERACTION_LOGS that purges any
	// JSON-era facts (a protobuf router fails closed on them). Opt-in so a normal restart never
	// wipes the log.
	if os.Getenv("RP_RESET_LOG_STREAM") == "1" {
		must("reset-stream", signaling.ResetLogStream(js))
		slog.Warn("router.log-stream-reset", "stream", "INTERACTION_LOGS")
	}
	must("stream", signaling.EnsureLogStream(js))

	// core depends only on the LogStore port; NATS is the adapter (loose coupling).
	r := signaling.NewRouter(signaling.NewJetStreamStore(js))
	// The .cmd subject now carries an identity suffix (tenant.*.interaction.*.cmd.<identity>); the
	// router reads the publisher from the last token (openspec change agent-feed-fanout, Decision 1).
	_, err = nc.QueueSubscribe("tenant.*.interaction.*.cmd.*", "router", func(m *nats.Msg) {
		// Seed the per-message context from the publisher's `traceparent` (subscribe side of
		// @spec:obs.nats-traceparent-propagated): the router's logs share the publisher's
		// trace_id. A missing/malformed header mints a fresh trace — never a drop.
		ctx := obs.ContextFromTraceparent(context.Background(), traceparentOf(m))
		ctx = obs.WithCorrelation(ctx, slog.Default())
		// Phase-1 has no per-connection identity (shared `client` user) → empty Identity, so the
		// router validates against the subject tenant. Auth-callout will mint a real one here.
		ctx = signaling.WithIdentity(ctx, signaling.Identity{})
		res := r.HandleCommand(ctx, m.Subject, m.Data)
		if m.Reply != "" {
			b, _ := proto.Marshal(res)
			_ = m.Respond(b)
		}
	})
	must("subscribe", err)

	slog.Info("router.up", "url", url, "stream", "INTERACTION_LOGS")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

// traceparentOf reads the inbound W3C trace header; nats.Msg.Header is nil for a header-less
// publish, so guard it rather than dereference.
func traceparentOf(m *nats.Msg) string {
	if m.Header == nil {
		return ""
	}
	return m.Header.Get("traceparent")
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
