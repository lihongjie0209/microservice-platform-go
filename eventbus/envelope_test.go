package eventbus

import (
	"testing"
	"time"

	identityv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/identity/v1"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()
	payload := &identityv1.UserStatusChangedEvent{UserId: "user-1", Reason: "security"}
	envelope, err := NewEnvelope(Metadata{
		EventID: "event-1", EventType: "platform.identity.user.status-changed.v1",
		AggregateID: "user-1", AggregateType: "user", SchemaVersion: 1,
		RequestID: "request-1", TraceID: "trace-1", OccurredAt: time.Unix(1, 0).UTC(),
	}, payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(identityv1.UserStatusChangedEvent)
	if err := DecodePayload(envelope, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.UserId != payload.UserId || envelope.Context.RequestId != "request-1" {
		t.Fatalf("decoded payload or context mismatch: %+v %+v", decoded, envelope.Context)
	}
}

func TestNewEnvelopeRejectsIncompleteMetadata(t *testing.T) {
	t.Parallel()
	if _, err := NewEnvelope(Metadata{}, &identityv1.UserStatusChangedEvent{}); err == nil {
		t.Fatal("NewEnvelope() error = nil")
	}
}
