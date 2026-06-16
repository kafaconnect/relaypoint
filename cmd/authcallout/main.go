// Auth-callout responder: verifies a connection's presented token and mints its per-connection,
// identity-pinned NATS ACLs (openspec change agent-feed-fanout, Decisions 1/2b/4/9). It replaces
// the shared-`client` dev user, making the `.cmd.<self>` / `feed.<self>` / `_INBOX_<conn>` pins
// airtight. The issuer signing-key SEED and the token-verify secret are SECRETS — env only, never
// committed.
package main

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/kafaconnect/relaypoint/internal/authcallout"
	"github.com/kafaconnect/relaypoint/internal/obs"
)

func main() {
	slog.SetDefault(obs.New("relaypoint-authcallout"))

	// OTLP trace export (M1.5 F5b) — no-op when the OTLP endpoint is unset; fail-open on a setup error.
	tracerShutdown, terr := obs.InitTracer(context.Background(), "relaypoint-authcallout")
	if terr != nil {
		slog.Default().Warn("otel.init_failed_continuing_log_only", "err", terr)
		tracerShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = tracerShutdown(context.Background()) }()

	url := envOr("NATS_URL", nats.DefaultURL)
	user := envOr("NATS_AUTH_USER", "authsvc")
	pass := mustEnv("NATS_AUTH_PASSWORD")
	account := envOr("AUTH_CALLOUT_ACCOUNT", "RP")
	issuerSeed := decodeSeed(mustEnv("AUTH_CALLOUT_ISSUER_SEED"))
	tokenSecret := []byte(mustEnv("AUTH_TOKEN_SECRET"))

	// F1: RP is the SOLE responder. The verify ladder is agent/trusted-backend (HMAC dev token) →
	// desk visitor `vis_` (EdDSA, verified against desk's published JWKS). When DESK_INGRESS_JWKS_URL
	// is unset the visitor link is omitted — a `vis_` then simply has no accepting verifier and is denied
	// (fail closed), so RP never mints a visitor grant without an explicit desk JWKS source configured.
	var verifier authcallout.Verifier = authcallout.NewHMACVerifier(tokenSecret)
	if jwksURL := os.Getenv("DESK_INGRESS_JWKS_URL"); jwksURL != "" {
		ttl := envDuration("DESK_INGRESS_JWKS_TTL", 5*time.Minute)
		src := authcallout.NewHTTPJWKSSource(jwksURL, &http.Client{Timeout: 5 * time.Second})
		visitor := authcallout.NewVisitorVerifier(src, ttl, authcallout.WithVisitorFetchTimeout(5*time.Second))
		verifier = authcallout.NewChainVerifier(verifier, visitor)
		slog.Info("authcallout.visitor.enabled", "jwks_ttl", ttl.String())
	}
	resp, err := authcallout.NewResponder(verifier, issuerSeed, account)
	must("responder", err)

	nc, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name("relaypoint-authcallout"))
	must("connect", err)
	defer nc.Drain()

	if _, err := resp.Subscribe(nc); err != nil {
		must("subscribe", err)
	}

	slog.Info("authcallout.up", "url", url, "account", account, "subject", authcallout.AuthRequestSubject)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

// decodeSeed accepts the nkey seed either raw (`SA…`) or base64-wrapped, so it can be carried in a
// k8s Secret/env without quoting issues.
func decodeSeed(v string) []byte {
	if len(v) > 0 && v[0] == 'S' {
		return []byte(v)
	}
	b, err := base64.StdEncoding.DecodeString(v)
	must("decode issuer seed", err)
	return b
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envDuration(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			return parsed
		}
		slog.Warn("authcallout.env.bad-duration", "key", k, "fallback", d.String())
	}
	return d
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("fatal", "at", "env", "missing", k)
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
