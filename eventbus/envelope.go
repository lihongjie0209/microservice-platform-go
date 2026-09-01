package eventbus

import (
	"errors"
	"time"

	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Metadata struct {
	EventID       string
	EventType     string
	AggregateID   string
	AggregateType string
	TenantID      string
	ApplicationID string
	SchemaVersion uint32
	RequestID     string
	TraceID       string
	ActorID       string
	ActorType     string
	OccurredAt    time.Time
}

func NewEnvelope(metadata Metadata, payload proto.Message) (*commonv1.EventEnvelope, error) {
	if metadata.EventID == "" || metadata.EventType == "" || metadata.AggregateID == "" || metadata.AggregateType == "" {
		return nil, errors.New("event id, type, aggregate id, and aggregate type are required")
	}
	if metadata.SchemaVersion == 0 {
		return nil, errors.New("event schema version must be positive")
	}
	if payload == nil {
		return nil, errors.New("event payload is required")
	}
	encoded, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}
	occurredAt := metadata.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	return &commonv1.EventEnvelope{
		EventId: metadata.EventID, EventType: metadata.EventType,
		AggregateId: metadata.AggregateID, AggregateType: metadata.AggregateType,
		TenantId: metadata.TenantID, ApplicationId: metadata.ApplicationID, SchemaVersion: metadata.SchemaVersion,
		OccurredAt: timestamppb.New(occurredAt),
		Context: &commonv1.RequestContext{
			RequestId: metadata.RequestID, TraceId: metadata.TraceID,
			ActorId: metadata.ActorID, ActorType: metadata.ActorType,
			TenantId: metadata.TenantID,
		},
		Payload: encoded,
	}, nil
}

func DecodePayload(envelope *commonv1.EventEnvelope, target proto.Message) error {
	if envelope == nil || target == nil {
		return errors.New("event envelope and target are required")
	}
	return proto.Unmarshal(envelope.Payload, target)
}
