//go:build integration

package participationclientnats

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

func TestAuthenticatedNATSAuthorityClientUsesConfiguredGrantAndW3C(t *testing.T) {
	projector := connectAuthorityNATS(t, "NATS_URL_PROJECTOR")
	defer projector.Close()
	grant := participation.TransportGrant{ServiceID: "relaypoint", Capability: participation.CapabilityRead}
	client, err := New(projector, 5*time.Second, grant)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []participation.TransportGrant{
		{},
		{ServiceID: "relaypoint", Capability: participation.CapabilityWrite},
		{ServiceID: "relaypoint", Capability: participation.CapabilityRead, Role: "reader"},
	} {
		if rejected, constructionErr := New(projector, time.Second, invalid); constructionErr == nil || rejected != nil {
			t.Fatalf("invalid grant accepted: %+v", invalid)
		}
	}
	router := connectAuthorityNATS(t, "NATS_URL_ROUTER")
	defer router.Close()
	tenant := "018f6100-0000-7000-8000-000000000001"
	interaction := "018f6100-0000-7000-8000-000000000002"
	requestID := "018f6100-0000-7000-8000-000000000003"
	subject, err := interactionv1.ReplayParticipationAddress(tenant, interaction)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := router.Subscribe(subject, func(message *nats.Msg) {
		request := new(interactionv1.ReplayParticipationRequest)
		if proto.Unmarshal(message.Data, request) != nil || request.GetTenantId() != tenant || request.GetInteractionId() != interaction {
			return
		}
		if _, valid := obs.ParseTraceparent(message.Header.Get("traceparent")); !valid {
			return
		}
		reply := &interactionv1.ReplayParticipationReply{
			RequestId: request.GetRequestId(),
			Outcome: &interactionv1.ReplayParticipationReply_Response{Response: &interactionv1.ReplayParticipationResponse{
				HeadVersion: 1, HistoryFloor: 1, Provenance: "corex-participation-history-v1",
			}},
		}
		payload, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(reply)
		if marshalErr != nil {
			return
		}
		response := nats.NewMsg(message.Reply)
		response.Data = payload
		response.Header.Set("traceparent", message.Header.Get("traceparent"))
		_ = router.PublishMsg(response)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	if err := router.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx := obs.ContextFromTraceparent(context.Background(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response, err := client.Replay(ctx, &interactionv1.ReplayParticipationRequest{
		TenantId: tenant, InteractionId: interaction, FromVersion: 1, ToVersion: 1, RequestId: requestID,
	})
	if err != nil || response.GetHeadVersion() != 1 || response.GetProvenance() != "corex-participation-history-v1" {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func connectAuthorityNATS(t *testing.T, name string) *nats.Conn {
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
