package outbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/outbox"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
)

type store struct {
	events    []outbox.Event
	published []string
	failed    []string
}

func (s *store) Claim(context.Context, int, time.Duration) ([]outbox.Event, error) {
	return s.events, nil
}
func (s *store) MarkPublished(_ context.Context, event outbox.Event, _ time.Time) error {
	s.published = append(s.published, event.ID)
	return nil
}
func (s *store) MarkFailed(_ context.Context, event outbox.Event, _ string, _ time.Time) error {
	s.failed = append(s.failed, event.ID)
	return nil
}

type publisher struct{ fail string }

func (p publisher) Publish(_ context.Context, _ string, envelope *commonv1.EventEnvelope) error {
	if envelope.GetEventId() == p.fail {
		return errors.New("nats unavailable")
	}
	return nil
}

func TestDispatcher_RunOnceContinuesAfterPublishFailure(t *testing.T) {
	storage := &store{events: []outbox.Event{{ID: "event-1", Subject: "one", Envelope: &commonv1.EventEnvelope{EventId: "event-1"}}, {ID: "event-2", Subject: "two", Envelope: &commonv1.EventEnvelope{EventId: "event-2"}}}}
	dispatcher, err := outbox.New(storage, publisher{fail: "event-1"}, outbox.Config{})
	if err != nil {
		t.Fatal(err)
	}
	published, err := dispatcher.RunOnce(t.Context())
	if err == nil || published != 1 {
		t.Fatalf("RunOnce() = (%d, %v), want (1, error)", published, err)
	}
	if len(storage.failed) != 1 || storage.failed[0] != "event-1" || len(storage.published) != 1 || storage.published[0] != "event-2" {
		t.Fatalf("failed=%v published=%v", storage.failed, storage.published)
	}
}

func TestNewRequiresDependencies(t *testing.T) {
	if _, err := outbox.New(nil, publisher{}, outbox.Config{}); err == nil {
		t.Fatal("New(nil store) error = nil")
	}
}
