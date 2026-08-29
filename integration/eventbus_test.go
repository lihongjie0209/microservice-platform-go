//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	identityv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/identity/v1"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestJetStreamPublishConsumeAndDeduplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "nats:2.14.6-alpine", AlwaysPullImage: true, ExposedPorts: []string{"4222/tcp"},
			Cmd:        []string{"--jetstream", "--store_dir=/data"},
			WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "4222/tcp")
	if err != nil {
		t.Fatal(err)
	}
	bus, err := eventbus.New(ctx, eventbus.Config{
		URLs:       []string{fmt.Sprintf("nats://%s:%s", host, port.Port())},
		ClientName: "platform-go-integration", Storage: "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{
		EventID: "event-deduplicate-1", EventType: "platform.identity.user.status-changed.v1",
		AggregateID: "user-1", AggregateType: "user", SchemaVersion: 1,
	}, &identityv1.UserStatusChangedEvent{UserId: "user-1", Reason: "test"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := bus.Publish(ctx, "platform.identity.user.status-changed.v1", envelope); err != nil {
			t.Fatal(err)
		}
	}
	consumeCtx, stopConsume := context.WithCancel(ctx)
	var received atomic.Int32
	consumeDone := make(chan error, 1)
	first := make(chan struct{}, 1)
	go func() {
		consumeDone <- bus.Consume(consumeCtx, "platform-go-test", "platform.identity.user.status-changed.v1", func(_ context.Context, got *commonv1.EventEnvelope) error {
			if got.EventId != envelope.EventId {
				return fmt.Errorf("event id = %q", got.EventId)
			}
			received.Add(1)
			select {
			case first <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	select {
	case <-first:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for event")
	}
	time.Sleep(500 * time.Millisecond)
	stopConsume()
	select {
	case <-consumeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
	if got := received.Load(); got != 1 {
		t.Fatalf("received %d events, want one deduplicated event", got)
	}
}
