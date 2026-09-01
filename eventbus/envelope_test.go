package eventbus

import (
	"context"
	"testing"
	"time"

	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	identityv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/identity/v1"
)

func TestDisabledBusIsNilSafe(t *testing.T) {
	t.Parallel()
	var bus *Bus
	if err := bus.Publish(context.Background(), "platform.test.v1", &commonv1.EventEnvelope{EventId: "event-1"}); err == nil {
		t.Fatal("Publish() error = nil")
	}
	if err := bus.Consume(context.Background(), "consumer", "platform.test.v1", func(context.Context, *commonv1.EventEnvelope) error { return nil }); err == nil {
		t.Fatal("Consume() error = nil")
	}
	if err := bus.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()
	payload := &identityv1.UserStatusChangedEvent{UserId: "user-1", Reason: "security"}
	envelope, err := NewEnvelope(Metadata{
		EventID: "event-1", EventType: "platform.identity.user.status-changed.v1",
		AggregateID: "user-1", AggregateType: "user", SchemaVersion: 1,
		ApplicationID: "application-1",
		RequestID:     "request-1", TraceID: "trace-1", OccurredAt: time.Unix(1, 0).UTC(),
	}, payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(identityv1.UserStatusChangedEvent)
	if err := DecodePayload(envelope, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.UserId != payload.UserId || envelope.Context.RequestId != "request-1" || envelope.GetApplicationId() != "application-1" {
		t.Fatalf("decoded payload or context mismatch: %+v %+v", decoded, envelope.Context)
	}
}

func TestNewEnvelopeRejectsIncompleteMetadata(t *testing.T) {
	t.Parallel()
	if _, err := NewEnvelope(Metadata{}, &identityv1.UserStatusChangedEvent{}); err == nil {
		t.Fatal("NewEnvelope() error = nil")
	}
}
