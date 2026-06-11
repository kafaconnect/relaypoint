// Command router runs the RelayPoint interaction service — the sole writer of `.log`.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
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

	// RP_DEV_NO_AUTH=1 runs the permissive pre-auth-callout posture (no identity → suffix advisory,
	// gates off). Production leaves it unset: an unauthenticated command fails closed (A1).
	devMode := os.Getenv("RP_DEV_NO_AUTH") == "1"
	var ropts []signaling.Option
	if devMode {
		ropts = append(ropts, signaling.WithDevMode())
		slog.Warn("router.dev-no-auth", "note", "participation/role gates disabled")
	}
	r := signaling.NewRouter(signaling.NewJetStreamStore(js), ropts...)

	// The auth-callout ACL pins each connection to publish only `…cmd.<self>`, so the subject suffix
	// IS the authenticated user; trusted-backend identities are operator-listed in RP_TRUSTED_BACKENDS
	// (anything else is RoleAgent). See openspec change agent-feed-fanout, Decision 1 / A1.
	trusted := trustedSet(os.Getenv("RP_TRUSTED_BACKENDS"))
	_, err = nc.QueueSubscribe("tenant.*.interaction.*.cmd.*", "router", func(m *nats.Msg) {
		ctx := obs.ContextFromTraceparent(context.Background(), traceparentOf(m))
		ctx = obs.WithCorrelation(ctx, slog.Default())
		if !devMode {
			ctx = signaling.WithIdentity(ctx, identityFromSubject(m.Subject, trusted))
		}
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

// identityFromSubject derives the authenticated identity from tenant.<tid>.interaction.<iid>.cmd.<self>:
// the auth-callout ACL guarantees a connection can only publish its own `<self>` suffix, so the suffix
// is the trusted user and p[1] the trusted tenant. A malformed subject yields a zero Identity, which
// the router rejects as unauthenticated. The role is RoleTrustedBackend only for an operator-listed
// identity; everything else is RoleAgent.
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

func trustedSet(csv string) map[string]bool {
	set := map[string]bool{}
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			set[s] = true
		}
	}
	return set
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
