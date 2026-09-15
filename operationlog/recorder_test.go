package operationlog

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

func TestRecorderPublishesScopedSanitizedEnvelope(t *testing.T) {
	publisher := &publisherStub{}
	recorder, err := New(Config{Enabled: true, Subject: "platform.operation-log.recorded.v1", MaxPayloadBytes: 4096}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	recorder.now = func() time.Time { return time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC) }
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1", MembershipID: "member-1"})
	err = recorder.Record(ctx, Entry{Operation: "scheduler.job.create", ResourceType: "scheduled_job", ResourceID: "job-1", ApplicationID: "app-1", Source: "backend", Protocol: "http", RequestID: "request-1", Request: map[string]any{"name": "daily", "password": "secret"}, Duration: time.Millisecond, Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.subject != "platform.operation-log.recorded.v1" || publisher.envelope.GetTenantId() != "tenant-1" || publisher.envelope.GetApplicationId() != "app-1" || publisher.envelope.GetContext().GetRequestId() != "request-1" {
		t.Fatalf("envelope = %+v", publisher.envelope)
	}
	var value map[string]any
	if err := json.Unmarshal(publisher.envelope.GetPayload(), &value); err != nil {
		t.Fatal(err)
	}
	requestPayload, _ := value["request_payload"].(string)
	if strings.Contains(requestPayload, "secret") || !strings.Contains(requestPayload, "[REDACTED]") {
		t.Fatalf("request_payload = %q", requestPayload)
	}
}

func TestRecorderValidatesConfigurationContextAndPublishFailure(t *testing.T) {
	if _, err := New(Config{Enabled: true}, nil); !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("New() error = %v", err)
	}
	disabled, err := New(Config{}, nil)
	if err != nil || disabled.Enabled() {
		t.Fatalf("disabled recorder = %#v, %v", disabled, err)
	}
	publisher := &publisherStub{err: errors.New("unavailable")}
	recorder, err := New(Config{Enabled: true, Subject: "operations", MaxPayloadBytes: 256}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Record(t.Context(), Entry{Operation: "create"}); !errors.Is(err, principal.ErrMissing) {
		t.Fatalf("missing principal error = %v", err)
	}
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser})
	if err := recorder.Record(ctx, Entry{Operation: "create"}); err == nil || !strings.Contains(err.Error(), "publish operation log") {
		t.Fatalf("publish error = %v", err)
	}
}
