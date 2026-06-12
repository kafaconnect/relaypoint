// Command projector runs the RelayPoint Participation/Fan-out service: a leased single-active
// worker that tails tenant.*.interaction.*.log and projects each fact into the feed of every
// currently-participating agent (openspec change agent-feed-fanout). Standby replicas contend for
// the NATS KV leader lease; only the holder projects.
package main

import (
	"context"
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

	cfg := projector.Config{MaxDeliver: maxDeliver}
	switch os.Getenv("PROJECTOR_FANOUT_MODE") {
	case "tenant-roster":
		// PRODUCTION tenant-shared fan-out: resolve a tenant's agents from desk's REAL roster (its
		// Zitadel org membership), no hardcode. Every fact of the tenant fans to ALL its agents.
		dr, err := projector.NewDeskRoster(
			mustEnv("DESK_ROSTER_URL"),
			mustEnv("DESK_ROSTER_TOKEN"),
			rosterCacheTTL(),
			&http.Client{Timeout: 10 * time.Second})
		must("desk-roster", err)
		cfg.Roster = dr
		slog.Info("projector.tenant-roster", "url", os.Getenv("DESK_ROSTER_URL"), "cache_ttl", rosterCacheTTL().String())
	case "tenant-wide":
		// Dev/test shortcut (off by default): tenant-wide fan-out to a STATIC agent roster, bypassing
		// the participation gate that stays empty until desk emits participation facts.
		cfg.TenantWideAgents = parseTenantAgents(os.Getenv("PROJECTOR_TENANT_AGENTS"))
		slog.Warn("projector.tenant-wide", "tenants", len(cfg.TenantWideAgents), "note", "participation gate bypassed")
	}
	p := projector.New(src, sink, lease, snaps, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("projector.up", "url", url, "stream", "INTERACTION_LOGS", "feed_stream", "AGENT_FEED")
	if err := p.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("projector.exit", "err", err)
		os.Exit(1)
	}
}

// parseTenantAgents reads PROJECTOR_TENANT_AGENTS="<tid>:<agent>[,<tid>:<agent>...]" into the
// per-tenant roster the tenant-wide shortcut fans to. Repeated tids accumulate agents.
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

// rosterCacheTTL bounds how long a tenant's roster is cached before a refresh (DESK_ROSTER_TTL, a
// Go duration; default 60s).
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
