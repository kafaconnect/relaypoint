package delivery

import (
	"testing"

	deliverypb "github.com/kafaconnect/relaypoint/gen/go/relaypoint/delivery/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestCanonicalDeliveryContract(t *testing.T) {
	file := deliverypb.File_relaypoint_delivery_v1_delivery_proto
	if got := string(file.Package()); got != "relaypoint.delivery.v1" {
		t.Fatalf("package=%s", got)
	}
	service := file.Services().ByName("DeliveryService")
	if service == nil || service.Methods().Len() != 1 {
		t.Fatalf("DeliveryService=%v", service)
	}
	method := service.Methods().ByName("AcceptDelivery")
	if method == nil || method.Input().FullName() != "relaypoint.delivery.v1.DeliveryAuthorization" || method.Output().FullName() != "relaypoint.delivery.v1.DeliveryReceipt" {
		t.Fatalf("AcceptDelivery=%v", method)
	}
	assertFieldNumbers(t, file.Messages().ByName("DeliveryAuthorization"), map[protoreflect.Name]protoreflect.FieldNumber{
		"delivery_authorization_id": 1, "event_id": 2, "tenant_id": 3, "reservation_id": 4, "interaction_id": 5,
		"target_subscriber_id": 6, "fencing_token": 7, "route_version": 8, "delivery_deadline": 9, "traceparent": 10,
	})
	assertFieldNumbers(t, file.Messages().ByName("DeliveryReceipt"), map[protoreflect.Name]protoreflect.FieldNumber{
		"receipt_id": 1, "delivery_authorization_id": 2, "event_id": 3, "tenant_id": 4, "reservation_id": 5, "interaction_id": 6,
		"target_subscriber_id": 7, "accepted_at": 8, "delivery_deadline": 9, "log_sequence": 10, "issuer_service_id": 11, "visibility": 12,
	})
}

func assertFieldNumbers(t *testing.T, message protoreflect.MessageDescriptor, fields map[protoreflect.Name]protoreflect.FieldNumber) {
	t.Helper()
	if message == nil || message.Fields().Len() != len(fields) {
		t.Fatalf("message=%v field count", message)
	}
	for name, number := range fields {
		field := message.Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Errorf("%s=%v want field %d", name, field, number)
		}
	}
}
