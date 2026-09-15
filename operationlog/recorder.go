// Package operationlog publishes sanitized operation records to the platform
// durable event bus. Persistence and query ownership remain with audit-service.
package operationlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/microservice-platform-go/redact"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const EventType = "platform.operation-log.recorded.v1"

var ErrInvalidEntry = errors.New("invalid operation log entry")

type Publisher interface {
	Publish(context.Context, string, *commonv1.EventEnvelope) error
}

type Config struct {
	Enabled         bool
	Subject         string
	MaxPayloadBytes int
}

type Entry struct {
	Operation     string        `json:"operation"`
	ResourceType  string        `json:"resource_type,omitempty"`
	ResourceID    string        `json:"resource_id,omitempty"`
	ApplicationID string        `json:"application_id,omitempty"`
	Source        string        `json:"source"`
	Protocol      string        `json:"protocol"`
	Method        string        `json:"method,omitempty"`
	Route         string        `json:"route,omitempty"`
	Request       any           `json:"request,omitempty"`
	Duration      time.Duration `json:"-"`
	Succeeded     bool          `json:"succeeded"`
	ErrorCode     string        `json:"error_code,omitempty"`
	ErrorMessage  string        `json:"error_message,omitempty"`
	ClientIP      string        `json:"client_ip,omitempty"`
	UserAgent     string        `json:"user_agent,omitempty"`
	RequestID     string        `json:"-"`
	Extension     any           `json:"-"`
}

type payload struct {
	Entry
	RequestPayload string          `json:"request_payload"`
	DurationMS     int64           `json:"duration_ms"`
	RequestIDValue string          `json:"request_id"`
	TraceID        string          `json:"trace_id"`
	ActorID        string          `json:"actor_id"`
	ActorType      string          `json:"actor_type"`
	TenantID       string          `json:"tenant_id"`
	OccurredAt     time.Time       `json:"occurred_at"`
	ExtensionValue json.RawMessage `json:"extension"`
}

type Recorder interface {
	Enabled() bool
	Record(context.Context, Entry) error
}

type Service struct {
	config    Config
	publisher Publisher
	now       func() time.Time
}

func New(config Config, publisher Publisher) (*Service, error) {
	config.Subject = strings.TrimSpace(config.Subject)
	if !config.Enabled {
		return &Service{config: config, publisher: publisher, now: time.Now}, nil
	}
	if publisher == nil || config.Subject == "" || config.MaxPayloadBytes < 256 || config.MaxPayloadBytes > 1<<20 {
		return nil, fmt.Errorf("%w: enabled recorder requires publisher, subject, and payload limit between 256 bytes and 1 MiB", ErrInvalidEntry)
	}
	return &Service{config: config, publisher: publisher, now: time.Now}, nil
}

func (s *Service) Enabled() bool { return s != nil && s.config.Enabled }

func (s *Service) Record(ctx context.Context, entry Entry) error {
	if !s.Enabled() {
		return nil
	}
	entry.Operation = strings.TrimSpace(entry.Operation)
	if entry.Operation == "" || entry.Duration < 0 {
		return fmt.Errorf("%w: operation and non-negative duration are required", ErrInvalidEntry)
	}
	actor, ok := principal.FromContext(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return principal.ErrMissing
	}
	requestPayload, err := sanitize(entry.Request, s.config.MaxPayloadBytes, false)
	if err != nil {
		return fmt.Errorf("%w: sanitize request: %v", ErrInvalidEntry, err)
	}
	extension, err := sanitize(entry.Extension, s.config.MaxPayloadBytes, true)
	if err != nil {
		return fmt.Errorf("%w: sanitize extension: %v", ErrInvalidEntry, err)
	}
	now := s.now()
	span := trace.SpanContextFromContext(ctx)
	value := payload{Entry: entry, RequestPayload: string(requestPayload), DurationMS: entry.Duration.Milliseconds(), RequestIDValue: entry.RequestID, TraceID: span.TraceID().String(), ActorID: actor.ID, ActorType: string(actor.Type), TenantID: actor.TenantID, OccurredAt: now, ExtensionValue: extension}
	value.Request = nil
	value.Extension = nil
	value.RequestID = ""
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode operation log: %w", err)
	}
	envelope := &commonv1.EventEnvelope{EventId: uuid.NewString(), EventType: EventType, AggregateId: entry.ResourceID, AggregateType: "operation_log", TenantId: actor.TenantID, ApplicationId: entry.ApplicationID, SchemaVersion: 1, OccurredAt: timestamppb.New(now), Context: &commonv1.RequestContext{RequestId: entry.RequestID, TraceId: span.TraceID().String(), ActorId: actor.ID, ActorType: string(actor.Type), TenantId: actor.TenantID, MembershipId: actor.MembershipID}, Payload: data}
	if err := s.publisher.Publish(ctx, s.config.Subject, envelope); err != nil {
		return fmt.Errorf("publish operation log: %w", err)
	}
	return nil
}

func sanitize(value any, limit int, objectDefault bool) ([]byte, error) {
	if value == nil {
		if objectDefault {
			return []byte("{}"), nil
		}
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	data, err = redact.JSON(data)
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("payload is %d bytes; maximum is %d", len(data), limit)
	}
	return data, nil
}

var _ Recorder = (*Service)(nil)
