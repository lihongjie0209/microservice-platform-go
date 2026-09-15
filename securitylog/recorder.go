// Package securitylog publishes sanitized security events to the platform
// durable event bus. Persistence and query ownership remain with audit-service.
package securitylog

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/microservice-platform-go/redact"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const EnvelopeType = "platform.security-log.recorded.v1"

type EventType string

const (
	EventLogin                    EventType = "login"
	EventTokenRefresh             EventType = "token_refresh"
	EventLogout                   EventType = "logout"
	EventForcedLogout             EventType = "forced_logout"
	EventPasswordChanged          EventType = "password_changed"
	EventPasswordReset            EventType = "password_reset"
	EventSessionRevoked           EventType = "session_revoked"
	EventLogoutAll                EventType = "logout_all"
	EventTenantAuthorization      EventType = "tenant_authorization_changed"
	EventApplicationMenuPublished EventType = "application_menu_published"
	EventTenantApplicationGrant   EventType = "tenant_application_grant_changed"
)

var (
	ErrInvalidConfig = errors.New("invalid security log configuration")
	ErrInvalidEntry  = errors.New("invalid security log entry")
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
)

type Publisher interface {
	Publish(context.Context, string, *commonv1.EventEnvelope) error
}

type Config struct {
	Enabled         bool
	Subject         string
	MaxPayloadBytes int
	HashKey         string
	FailClosed      bool
}

type Entry struct {
	EventType     EventType `json:"event_type"`
	SubjectID     string    `json:"subject_id,omitempty"`
	SubjectType   string    `json:"subject_type,omitempty"`
	TenantID      string    `json:"tenant_id,omitempty"`
	ApplicationID string    `json:"application_id,omitempty"`
	Identifier    string    `json:"-"`
	TokenID       string    `json:"-"`
	SessionID     string    `json:"session_id,omitempty"`
	Succeeded     bool      `json:"succeeded"`
	Reason        string    `json:"reason,omitempty"`
	ErrorCode     string    `json:"error_code,omitempty"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	ClientIP      string    `json:"client_ip,omitempty"`
	UserAgent     string    `json:"user_agent,omitempty"`
	RequestID     string    `json:"-"`
	Metadata      any       `json:"-"`
}

type payload struct {
	EventType      EventType       `json:"event_type"`
	ActorID        string          `json:"actor_id"`
	ActorType      string          `json:"actor_type"`
	SubjectID      string          `json:"subject_id"`
	SubjectType    string          `json:"subject_type"`
	TenantID       string          `json:"tenant_id"`
	ApplicationID  string          `json:"application_id"`
	Succeeded      bool            `json:"succeeded"`
	Reason         string          `json:"reason"`
	ErrorCode      string          `json:"error_code"`
	ErrorMessage   string          `json:"error_message"`
	IdentifierHash string          `json:"identifier_hash"`
	TokenIDHash    string          `json:"token_id_hash"`
	SessionID      string          `json:"session_id"`
	RequestID      string          `json:"request_id"`
	TraceID        string          `json:"trace_id"`
	ClientIP       string          `json:"client_ip"`
	UserAgent      string          `json:"user_agent"`
	Metadata       json.RawMessage `json:"metadata"`
	OccurredAt     time.Time       `json:"occurred_at"`
}

type Recorder interface {
	Enabled() bool
	FailClosed() bool
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
	if publisher == nil || config.Subject == "" || config.MaxPayloadBytes < 256 || config.MaxPayloadBytes > 1<<20 || len(config.HashKey) < 32 {
		return nil, fmt.Errorf("%w: enabled recorder requires publisher, subject, a payload limit between 256 bytes and 1 MiB, and a hash key of at least 32 bytes", ErrInvalidConfig)
	}
	return &Service{config: config, publisher: publisher, now: time.Now}, nil
}

func (s *Service) Enabled() bool    { return s != nil && s.config.Enabled }
func (s *Service) FailClosed() bool { return s != nil && s.config.FailClosed }

func (s *Service) Record(ctx context.Context, entry Entry) error {
	if !s.Enabled() {
		return nil
	}
	entry.EventType = EventType(strings.TrimSpace(string(entry.EventType)))
	if !eventTypePattern.MatchString(string(entry.EventType)) {
		return fmt.Errorf("%w: event type must match %s", ErrInvalidEntry, eventTypePattern)
	}
	actor, _ := principal.FromContext(ctx)
	if strings.TrimSpace(entry.TenantID) == "" {
		entry.TenantID = actor.TenantID
	}
	metadata, err := safeMetadata(entry.Metadata, s.config.MaxPayloadBytes)
	if err != nil {
		return fmt.Errorf("%w: metadata: %v", ErrInvalidEntry, err)
	}
	now := s.now()
	span := trace.SpanContextFromContext(ctx)
	value := payload{
		EventType: entry.EventType, ActorID: actor.ID, ActorType: string(actor.Type),
		SubjectID: strings.TrimSpace(entry.SubjectID), SubjectType: truncate(entry.SubjectType, 128),
		TenantID: strings.TrimSpace(entry.TenantID), ApplicationID: strings.TrimSpace(entry.ApplicationID),
		Succeeded: entry.Succeeded, Reason: truncate(entry.Reason, 1024), ErrorCode: truncate(entry.ErrorCode, 128),
		ErrorMessage: truncate(entry.ErrorMessage, 2048), IdentifierHash: s.hash(strings.ToLower(strings.TrimSpace(entry.Identifier))),
		TokenIDHash: s.hash(entry.TokenID), SessionID: truncate(entry.SessionID, 256), RequestID: truncate(entry.RequestID, 256),
		TraceID: span.TraceID().String(), ClientIP: truncate(entry.ClientIP, 256), UserAgent: truncate(entry.UserAgent, 1024),
		Metadata: metadata, OccurredAt: now,
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode security log: %w", err)
	}
	envelope := &commonv1.EventEnvelope{
		EventId: uuid.NewString(), EventType: EnvelopeType, AggregateId: value.SubjectID,
		AggregateType: "security_log", TenantId: value.TenantID, ApplicationId: value.ApplicationID,
		SchemaVersion: 1, OccurredAt: timestamppb.New(now),
		Context: &commonv1.RequestContext{RequestId: value.RequestID, TraceId: value.TraceID, ActorId: actor.ID, ActorType: string(actor.Type), TenantId: value.TenantID, MembershipId: actor.MembershipID},
		Payload: data,
	}
	if err := s.publisher.Publish(ctx, s.config.Subject, envelope); err != nil {
		return fmt.Errorf("publish security log: %w", err)
	}
	return nil
}

func (s *Service) hash(value string) string {
	if value == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(s.config.HashKey))
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func safeMetadata(value any, limit int) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage("{}"), nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("payload is %d bytes; maximum is %d", len(data), limit)
	}
	sensitive, err := redact.ContainsSensitiveJSON(data)
	if err != nil {
		return nil, err
	}
	if sensitive {
		return nil, errors.New("contains a credential field")
	}
	return data, nil
}

func truncate(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

var _ Recorder = (*Service)(nil)
