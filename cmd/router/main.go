// Command router runs the RelayPoint interaction service — the sole writer of `.log`.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/kafaconnect/relaypoint/internal/health"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func main() {
	if health.IsProbe(os.Args) {
		os.Exit(health.RunProbe(health.DefaultAddr))
	}
	slog.SetDefault(obs.New("relaypoint-router"))

	// OTLP trace export (M1.5 F5b) — no-op when the OTLP endpoint is unset; fail-open on a setup error.
	tracerShutdown, terr := obs.InitTracer(context.Background(), "relaypoint-router")
	if terr != nil {
		slog.Default().Warn("otel.init_failed_continuing_log_only", "err", terr)
		tracerShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = tracerShutdown(context.Background()) }()

	url := envOr("NATS_URL", nats.DefaultURL)
	user := envOr("NATS_USER", "router")
	pass := envOr("NATS_PASSWORD", "router-dev")

	nc, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name("relaypoint-router"))
	must("connect", err)
	defer nc.Drain()

	js, err := nc.JetStream()
	must("jetstream", err)
	// ADR-0002: opt-in destructive reset purges JSON-era facts a protobuf router can't read; off by default so a normal restart never wipes the log.
	if os.Getenv("RP_RESET_LOG_STREAM") == "1" {
		must("reset-stream", signaling.ResetLogStream(js))
		slog.Warn("router.log-stream-reset", "stream", "INTERACTION_LOGS")
	}
	must("stream", signaling.EnsureLogStream(js))

	// RP_DEV_NO_AUTH=1 is the permissive pre-auth-callout posture; unset (prod) fails an unauthenticated command closed (A1).
	devMode := os.Getenv("RP_DEV_NO_AUTH") == "1"
	var ropts []signaling.Option
	if devMode {
		ropts = append(ropts, signaling.WithDevMode())
		slog.Warn("router.dev-no-auth", "note", "participation/role gates disabled")
	}
	r := signaling.NewRouter(signaling.NewJetStreamStore(js), ropts...)

	// the auth-callout ACL pins each connection to publish only `…cmd.<self>`, so the subject suffix IS the authenticated user; trusted backends are operator-listed (agent-feed-fanout Decision 1 / A1).
	trusted := trustedSet(os.Getenv("RP_TRUSTED_BACKENDS"))
	_, err = nc.QueueSubscribe("tenant.*.interaction.*.cmd.*", "router", func(m *nats.Msg) {
		ctx := obs.ContextFromTraceparent(context.Background(), traceparentOf(m))
		ctx = obs.WithCorrelation(ctx, slog.Default())
		if devMode {
			// dev anonymous bus: an operator-listed service suffix (e.g. desk-svc) is folded as a trusted backend so it may publish for agents (actor_id != suffix) before auth-callout mints identities.
			if id := devTrustedIdentity(m.Subject, trusted); id.Role != "" {
				ctx = signaling.WithIdentity(ctx, id)
			}
		} else {
			ctx = signaling.WithIdentity(ctx, identityFromSubject(m.Subject, trusted))
		}
		res := r.HandleCommand(ctx, m.Subject, m.Data)
		if m.Reply != "" {
			b, _ := proto.Marshal(res)
			_ = m.Respond(b)
		}
	})
	must("subscribe", err)

	healthCheck := func() error {
		if !nc.IsConnected() {
			return errors.New("nats disconnected")
		}
		if _, jerr := js.AccountInfo(); jerr != nil {
			return fmt.Errorf("jetstream unreachable: %w", jerr)
		}
		return nil
	}
	go func() {
		if herr := health.Serve(context.Background(), health.DefaultAddr, healthCheck, healthCheck, obs.MetricsHandler()); herr != nil {
			slog.Error("health.serve", "err", herr)
		}
	}()

	slog.Info("router.up", "url", url, "stream", "INTERACTION_LOGS")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

// the auth-callout ACL guarantees a connection can only publish its own `<self>` suffix, so the subject suffix is trusted as the user; a malformed subject yields a zero Identity (rejected as unauthenticated).
func identityFromSubject(subject string, trusted map[string]bool) signaling.Identity {
	p := strings.Split(subject, ".")
	if len(p) != 6 || p[1] == "" || p[5] == "" {
		return signaling.Identity{}
	}
	role := signaling.RoleAgent
	if trusted[p[5]] {
		role = signaling.RoleTrustedBackend
	}
	return signaling.Identity{TenantID: p[1], UserID: p[5], Role: role}
}

func devTrustedIdentity(subject string, trusted map[string]bool) signaling.Identity {
	p := strings.Split(subject, ".")
	if len(p) != 6 || p[1] == "" || p[5] == "" || !trusted[p[5]] {
		return signaling.Identity{}
	}
	return signaling.Identity{TenantID: p[1], UserID: p[5], Role: signaling.RoleTrustedBackend}
}

func trustedSet(csv string) map[string]bool {
	set := map[string]bool{}
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			set[s] = true
		}
	}
	return set
}

// nats.Msg.Header is nil for a header-less publish, so guard rather than dereference.
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
