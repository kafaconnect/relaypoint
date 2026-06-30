// Command authcallout verifies a connection's token and mints its per-connection, identity-pinned NATS ACLs (openspec change agent-feed-fanout).
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/kafaconnect/relaypoint/internal/authcallout"
	"github.com/kafaconnect/relaypoint/internal/health"
	"github.com/kafaconnect/relaypoint/internal/obs"
)

func main() {
	if health.IsProbe(os.Args) {
		os.Exit(health.RunProbe(health.DefaultAddr))
	}
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

	// Posture derives from the EXISTING DESK_INGRESS_JWKS_URL: set ⇒ production (real identities come from the desk issuer via JWKS; the HMAC dev secret may NOT self-assert the privileged trusted-backend role); unset ⇒ dev (no JWKS, so no verifier accepts a `vis_` token — visitor grants fail closed (F1) — and the HMAC dev secret may self-assert trusted-backend for local wiring) (RH-08).
	var verifier authcallout.Verifier
	if jwksURL := os.Getenv("DESK_INGRESS_JWKS_URL"); jwksURL != "" {
		ttl := envDuration("DESK_INGRESS_JWKS_TTL", 5*time.Minute)
		src := authcallout.NewHTTPJWKSSource(jwksURL, &http.Client{Timeout: 5 * time.Second})
		visitor := authcallout.NewVisitorVerifier(src, ttl, authcallout.WithVisitorFetchTimeout(5*time.Second))
		verifier = authcallout.NewChainVerifier(authcallout.NewHMACVerifier(tokenSecret), visitor)
		slog.Info("authcallout.visitor.enabled", "jwks_ttl", ttl.String())
	} else {
		verifier = authcallout.NewHMACVerifier(tokenSecret, authcallout.AllowHMACTrustedBackend())
		slog.Warn("authcallout.hmac.dev-trusted-backend", "note", "HMAC may self-assert trusted-backend (no JWKS configured)")
	}
	resp, err := authcallout.NewResponder(verifier, issuerSeed, account)
	must("responder", err)

	nc, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name("relaypoint-authcallout"))
	must("connect", err)
	defer nc.Drain()

	if _, err := resp.Subscribe(nc); err != nil {
		must("subscribe", err)
	}

	healthCheck := func() error {
		if !nc.IsConnected() {
			return errors.New("nats disconnected")
		}
		return nil
	}
	go func() {
		if herr := health.Serve(context.Background(), health.DefaultAddr, healthCheck, healthCheck); herr != nil {
			slog.Error("health.serve", "err", herr)
		}
	}()

	slog.Info("authcallout.up", "url", url, "account", account, "subject", authcallout.AuthRequestSubject)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

// accepts the seed raw (`SA…`) or base64-wrapped so it survives a k8s Secret/env without quoting issues.
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
