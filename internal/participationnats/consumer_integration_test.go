//go:build integration

package participationnats

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	consumerTenant      = "018f6000-0000-7000-8000-000000000001"
	consumerInteraction = "018f6000-0000-7000-8000-000000000002"
	consumerParticipant = "018f6000-0000-7000-8000-000000000003"
	consumerEvent       = "018f6000-0000-7000-8000-000000000004"
	consumerTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

type appliedCommand struct {
	principal participation.Principal
	command   *interactionv1.ParticipationCommand
}

type integrationCommandHandler struct {
	applied chan appliedCommand
}

func (h *integrationCommandHandler) Apply(_ context.Context, principal participation.Principal, command *interactionv1.ParticipationCommand) (participation.Result, error) {
	h.applied <- appliedCommand{principal: principal, command: command}
	return participation.Applied, nil
}

func TestAuthenticatedFileJetStreamConsumerBindsInjectedGrant(t *testing.T) {
	projector := connectIntegrationNATS(t, "NATS_URL_PROJECTOR")
	defer projector.Close()
	js, err := projector.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = js.DeleteStream(StreamName)
	t.Cleanup(func() { _ = js.DeleteStream(StreamName) })
	if err := EnsureStream(js); err != nil {
		t.Fatal(err)
	}
	stream, err := js.StreamInfo(StreamName)
	if err != nil || stream.Config.Storage != nats.FileStorage || stream.Config.Retention != nats.WorkQueuePolicy {
		t.Fatalf("stream=%+v err=%v", stream, err)
	}
	handler := &integrationCommandHandler{applied: make(chan appliedCommand, 1)}
	grant := participation.TransportGrant{ServiceID: "corex", Capability: participation.CapabilityWrite}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	for _, invalid := range []participation.TransportGrant{
		{},
		{ServiceID: "corex", Capability: participation.CapabilityRead},
		{ServiceID: "corex", Capability: participation.CapabilityWrite, Role: "writer"},
	} {
		if consumer, constructionErr := NewConsumer(js, handler, invalid, slog.New(slog.NewTextHandler(io.Discard, nil))); constructionErr == nil || consumer != nil {
			t.Fatalf("invalid grant accepted: %+v", invalid)
		}
	}
	consumer, err := NewConsumer(js, handler, grant, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	command := &interactionv1.ParticipationCommand{
		EventId: consumerEvent, AggregateVersion: 1, TenantId: consumerTenant,
		InteractionId: consumerInteraction, ParticipantId: consumerParticipant,
		DesiredState: interactionv1.ParticipationDesiredState_PARTICIPATION_DESIRED_STATE_ASSIGNED,
		OccurredAt:   timestamppb.New(time.Unix(100, 0).UTC()), Traceparent: consumerTraceparent,
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	router := connectIntegrationNATS(t, "NATS_URL_ROUTER")
	defer router.Close()
	routerJS, err := router.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	subject, err := interactionv1.ParticipationCommandAddress(consumerTenant, consumerInteraction)
	if err != nil {
		t.Fatal(err)
	}
	message := nats.NewMsg(subject)
	message.Data = payload
	message.Header.Set(nats.MsgIdHdr, consumerEvent)
	message.Header.Set("traceparent", consumerTraceparent)
	if _, err := routerJS.PublishMsg(message); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-handler.applied:
		if got.principal.TenantID != consumerTenant || got.principal.Grant != grant || !proto.Equal(got.command, command) {
			t.Fatalf("applied=%+v", got)
		}
	case runErr := <-done:
		t.Fatalf("consumer stopped before command: %v: %s", runErr, logs.String())
	case <-time.After(5 * time.Second):
		t.Fatalf("command was not applied: %s", logs.String())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func connectIntegrationNATS(t *testing.T, name string) *nats.Conn {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		t.Fatalf("%s is required", name)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User == nil {
		t.Fatalf("%s must contain credentials", name)
	}
	username := parsed.User.Username()
	password, ok := parsed.User.Password()
	if username == "" || !ok {
		t.Fatalf("%s must contain user/password credentials", name)
	}
	parsed.User = nil
	connection, err := nats.Connect(parsed.String(), nats.UserInfo(username, password), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return connection
}
