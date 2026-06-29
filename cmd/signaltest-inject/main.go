// Command signaltest-inject publishes one message.created Command as an arbitrary actor to prove a browser-unseen message flows producer→router→projector→feed→browser; dev/test only.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	interactionpb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
)

func main() {
	url := env("NATS_URL", "nats://nats.infra.svc.cluster.local:4222")
	tenant := os.Getenv("TENANT")
	iid := os.Getenv("INTERACTION")
	actor := env("ACTOR", "customer-inbound")
	text := env("TEXT", "inbound via RP")
	cmdID := env("CMD_ID", fmt.Sprintf("inject-%d", time.Now().UnixNano()))

	opts := []nats.Option{
		nats.Name("signaltest-inject"),
		nats.Timeout(5 * time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(time.Second),
	}
	if u := os.Getenv("NATS_USER"); u != "" {
		opts = append(opts, nats.UserInfo(u, os.Getenv("NATS_PASSWORD")))
	}
	var nc *nats.Conn
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		nc, err = nats.Connect(url, opts...)
		if err == nil {
			break
		}
		fmt.Fprintf(os.Stderr, "connect attempt %d: %v\n", attempt, err)
		time.Sleep(time.Second)
	}
	must(err)
	defer nc.Drain()

	data, err := proto.Marshal(&interactionpb.ChatMessage{Text: text})
	must(err)
	cmd := &interactionpb.Command{
		CommandId: cmdID, TenantId: tenant, ActorId: actor,
		Type: "message.created", Medium: "chat", RefId: iid, Data: data,
	}
	payload, err := proto.Marshal(cmd)
	must(err)

	subject := fmt.Sprintf("tenant.%s.interaction.%s.cmd.desk-svc", tenant, iid)
	reply, err := nc.Request(subject, payload, 5*time.Second)
	must(err)
	var res interactionpb.CommandResult
	must(proto.Unmarshal(reply.Data, &res))
	fmt.Printf("subject=%s status=%s reason=%q command_id=%s\n", subject, res.GetStatus(), res.GetReason(), cmdID)
	if res.GetStatus() != interactionpb.CommandResult_STATUS_ACCEPTED {
		os.Exit(1)
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERR:", err)
		os.Exit(1)
	}
}
