// Command router is the RelayPoint authoritative interaction service (chat subset):
// the sole writer of `interaction.<id>.log`. It validates client commands on
// `.cmd`, assigns sequence, appends the fact to JetStream, and acks the issuer.
package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/kafaconnect/relaypoint/internal/signaling"
)

func main() {
	url := envOr("NATS_URL", nats.DefaultURL)
	user := envOr("NATS_USER", "router")
	pass := envOr("NATS_PASSWORD", "router-dev")

	nc, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name("relaypoint-router"))
	must("connect", err)
	defer nc.Drain()

	js, err := nc.JetStream()
	must("jetstream", err)
	must("stream", signaling.EnsureLogStream(js))

	r := signaling.NewRouter(nc, js)
	_, err = nc.QueueSubscribe("tenant.*.interaction.*.cmd", "router", func(m *nats.Msg) {
		res := r.HandleCommand(m.Subject, m.Data)
		if m.Reply != "" {
			b, _ := json.Marshal(res)
			_ = m.Respond(b) // ephemeral CommandResult to the issuer's inbox only
		}
	})
	must("subscribe", err)

	slog.Info("relaypoint router up", "url", url, "stream", "INTERACTION_LOG")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
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
