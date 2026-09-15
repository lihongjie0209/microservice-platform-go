package securitylog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/principal"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
)

type publisherStub struct {
	subject  string
	envelope *commonv1.EventEnvelope
	err      error
}

func (p *publisherStub) Publish(_ context.Context, subject string, envelope *commonv1.EventEnvelope) error {
	p.subject, p.envelope = subject, envelope
	return p.err
}

func validConfig() Config {
	return Config{Enabled: true, Subject: EnvelopeType, MaxPayloadBytes: 4096, HashKey: strings.Repeat("k", 32), FailClosed: true}
}

func TestRecorderAcceptsUnauthenticatedLoginAndHashesIdentifier(t *testing.T) {
	publisher := &publisherStub{}
	recorder, err := New(validConfig(), publisher)
	if err != nil {
		t.Fatal(err)
	}
	recorder.now = func() time.Time { return time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC) }
	err = recorder.Record(t.Context(), Entry{EventType: EventLogin, Identifier: " Alice@Example.COM ", SubjectType: "user", Succeeded: false, RequestID: "request-1", Metadata: map[string]any{"method": "password"}})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.subject != EnvelopeType || publisher.envelope.GetContext().GetActorId() != "" || publisher.envelope.GetContext().GetRequestId() != "request-1" {
		t.Fatalf("envelope = %+v", publisher.envelope)
	}
	var value map[string]any
	if err := json.Unmarshal(publisher.envelope.GetPayload(), &value); err != nil {
		t.Fatal(err)
	}
	if value["identifier_hash"] == "" || strings.Contains(string(publisher.envelope.GetPayload()), "Alice@Example.COM") {
		t.Fatalf("payload = %s", publisher.envelope.GetPayload())
	}
}

func TestRecorderCarriesAuthenticatedScope(t *testing.T) {
	publisher := &publisherStub{}
	recorder, err := New(validConfig(), publisher)
	if err != nil {
		t.Fatal(err)
	}
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1", MembershipID: "member-1"})
	err = recorder.Record(ctx, Entry{EventType: EventTenantApplicationGrant, SubjectID: "grant-1", SubjectType: "tenant_application_grant", ApplicationID: "app-1", Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.envelope.GetTenantId() != "tenant-1" || publisher.envelope.GetApplicationId() != "app-1" || publisher.envelope.GetContext().GetMembershipId() != "member-1" {
		t.Fatalf("envelope = %+v", publisher.envelope)
	}
}

func TestRecorderRejectsBadConfigurationAndSensitiveMetadata(t *testing.T) {
	if _, err := New(Config{Enabled: true}, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() error = %v", err)
	}
	disabled, err := New(Config{}, nil)
	if err != nil || disabled.Enabled() || disabled.FailClosed() {
		t.Fatalf("disabled recorder = %#v, %v", disabled, err)
	}
	recorder, err := New(validConfig(), &publisherStub{})
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Record(t.Context(), Entry{EventType: "BAD EVENT", Succeeded: true}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("event type error = %v", err)
	}
	if err := recorder.Record(t.Context(), Entry{EventType: EventLogin, Metadata: map[string]any{"nested": map[string]any{"access_token": "secret"}}}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("metadata error = %v", err)
	}
}

func TestRecorderReturnsPublishFailure(t *testing.T) {
	recorder, err := New(validConfig(), &publisherStub{err: errors.New("unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Record(t.Context(), Entry{EventType: EventLogin}); err == nil || !strings.Contains(err.Error(), "publish security log") {
		t.Fatalf("publish error = %v", err)
	}
}
