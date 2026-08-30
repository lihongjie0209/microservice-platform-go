//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	identityv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/identity/v1"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel/trace"
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
		ConsumerAckWait: 2 * time.Second, ConsumerAckTimeout: 500 * time.Millisecond,
		ConsumerHandlerTimeout: time.Second, ConsumerRetryDelay: 50 * time.Millisecond,
		ConsumerMaxRetryDelay: 50 * time.Millisecond, ConsumerMaxDeliver: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{
		EventID: "event-deduplicate-1", EventType: "platform.identity.user.status-changed.v1",
		AggregateID: "user-1", AggregateType: "user", SchemaVersion: 1,
		RequestID: "request-integration-1",
	}, &identityv1.UserStatusChangedEvent{UserId: "user-1", Reason: "test"})
	if err != nil {
		t.Fatal(err)
	}
	traceID, err := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("b7ad6b7169203331")
	if err != nil {
		t.Fatal(err)
	}
	publishContext := trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	}))
	for range 2 {
		if err := bus.Publish(publishContext, "platform.identity.user.status-changed.v1", envelope); err != nil {
			t.Fatal(err)
		}
	}
	consumeCtx, stopConsume := context.WithCancel(ctx)
	var received atomic.Int32
	consumeDone := make(chan error, 1)
	first := make(chan struct{}, 1)
	go func() {
		consumeDone <- bus.Consume(consumeCtx, "platform-go-test", "platform.identity.user.status-changed.v1", func(handlerContext context.Context, got *commonv1.EventEnvelope) error {
			if got.EventId != envelope.EventId {
				return fmt.Errorf("event id = %q", got.EventId)
			}
			if eventbus.RequestIDFromContext(handlerContext) != "request-integration-1" {
				return fmt.Errorf("request id = %q", eventbus.RequestIDFromContext(handlerContext))
			}
			if trace.SpanContextFromContext(handlerContext).TraceID() != traceID {
				return fmt.Errorf("trace id = %q", trace.SpanContextFromContext(handlerContext).TraceID())
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

	dlqCtx, stopDLQ := context.WithCancel(ctx)
	t.Cleanup(stopDLQ)
	deadLetters := make(chan *commonv1.DeadLetterEvent, 1)
	consumerErrors := make(chan error, 2)
	go func() {
		consumerErrors <- bus.Consume(dlqCtx, "platform-go-dead-letter-observer", eventbus.DeadLetterSubject, func(_ context.Context, envelope *commonv1.EventEnvelope) error {
			payload := new(commonv1.DeadLetterEvent)
			if err := eventbus.DecodePayload(envelope, payload); err != nil {
				return err
			}
			deadLetters <- payload
			return nil
		})
	}()
	go func() {
		consumerErrors <- bus.Consume(dlqCtx, "platform-go-failing-consumer", "platform.identity.user.dead-letter-test.v1", func(context.Context, *commonv1.EventEnvelope) error {
			return errors.New("forced integration failure")
		})
	}()

	deadLetterEnvelope, err := eventbus.NewEnvelope(eventbus.Metadata{
		EventID: "event-dead-letter-1", EventType: "platform.identity.user.dead-letter-test.v1",
		AggregateID: "user-1", AggregateType: "user", SchemaVersion: 1,
	}, &identityv1.UserStatusChangedEvent{UserId: "user-1", Reason: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, "platform.identity.user.dead-letter-test.v1", deadLetterEnvelope); err != nil {
		t.Fatal(err)
	}

	select {
	case deadLetter := <-deadLetters:
		if deadLetter.OriginalEvent.GetEventId() != deadLetterEnvelope.EventId || deadLetter.DeliveryCount != 2 || deadLetter.Reason != "forced integration failure" {
			t.Fatalf("dead letter = %+v", deadLetter)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for dead letter")
	}
	stopDLQ()
	for range 2 {
		select {
		case err := <-consumerErrors:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("consumer stop error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("consumer did not stop")
		}
	}
}
