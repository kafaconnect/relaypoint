// Auth-callout responder: verifies a connection's presented token and mints its per-connection,
// identity-pinned NATS ACLs (openspec change agent-feed-fanout, Decisions 1/2b/4/9). It replaces
// the shared-`client` dev user, making the `.cmd.<self>` / `feed.<self>` / `_INBOX_<conn>` pins
// airtight. The issuer signing-key SEED and the token-verify secret are SECRETS — env only, never
// committed.
package main

import (
	"encoding/base64"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/kafaconnect/relaypoint/internal/authcallout"
	"github.com/kafaconnect/relaypoint/internal/obs"
)

func main() {
	slog.SetDefault(obs.New("relaypoint-authcallout"))

	url := envOr("NATS_URL", nats.DefaultURL)
	user := envOr("NATS_AUTH_USER", "authsvc")
	pass := mustEnv("NATS_AUTH_PASSWORD")
	account := envOr("AUTH_CALLOUT_ACCOUNT", "RP")
	issuerSeed := decodeSeed(mustEnv("AUTH_CALLOUT_ISSUER_SEED"))
	tokenSecret := []byte(mustEnv("AUTH_TOKEN_SECRET"))

	verifier := authcallout.NewHMACVerifier(tokenSecret)
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
