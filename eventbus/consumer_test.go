package eventbus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type fakeConsumerMessage struct {
	data        []byte
	headers     nats.Header
	subject     string
	metadata    *jetstream.MsgMetadata
	metadataErr error
	ackErr      error
	nakErr      error
	termErr     error
	acked       bool
	nakDelay    time.Duration
	termReason  string
}

func (m *fakeConsumerMessage) Data() []byte         { return m.data }
func (m *fakeConsumerMessage) Headers() nats.Header { return m.headers }
func (m *fakeConsumerMessage) Subject() string      { return m.subject }
func (m *fakeConsumerMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return m.metadata, m.metadataErr
}
func (m *fakeConsumerMessage) DoubleAck(context.Context) error {
	m.acked = true
	return m.ackErr
}
func (m *fakeConsumerMessage) NakWithDelay(delay time.Duration) error {
	m.nakDelay = delay
	return m.nakErr
}
func (m *fakeConsumerMessage) TermWithReason(reason string) error {
	m.termReason = reason
	return m.termErr
}

type fakeEnvelopePublisher struct {
	err      error
	subject  string
	envelope *commonv1.EventEnvelope
}

func (p *fakeEnvelopePublisher) Publish(_ context.Context, subject string, envelope *commonv1.EventEnvelope) error {
	p.subject = subject
	p.envelope = envelope
	return p.err
}

func testProcessor(handler Handler, publisher envelopePublisher) consumerProcessor {
	config := Config{}
	config.defaults()
	return consumerProcessor{
		config: config, durable: "test-consumer", handler: handler,
		publisher: publisher, now: func() time.Time { return time.Unix(100, 0).UTC() },
	}
}

func encodedEnvelope(t *testing.T, eventType string) []byte {
	t.Helper()
	value, err := proto.Marshal(&commonv1.EventEnvelope{
		EventId: "event-1", EventType: eventType, AggregateId: "aggregate-1",
		AggregateType: "example", SchemaVersion: 1,
		Context: &commonv1.RequestContext{RequestId: "request-1", TraceId: "trace-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestConsumerProcessorAcknowledgesSuccessfulDelivery(t *testing.T) {
	t.Parallel()
	publisher := new(fakeEnvelopePublisher)
	processor := testProcessor(func(ctx context.Context, _ *commonv1.EventEnvelope) error {
		if RequestIDFromContext(ctx) != "request-1" {
			t.Fatalf("RequestIDFromContext() = %q", RequestIDFromContext(ctx))
		}
		return nil
	}, publisher)
	message := &fakeConsumerMessage{
		data: encodedEnvelope(t, "platform.example.changed.v1"), headers: make(nats.Header),
		subject: "platform.example.changed.v1", metadata: &jetstream.MsgMetadata{NumDelivered: 1},
	}

	processor.process(t.Context(), message)

	if !message.acked || message.nakDelay != 0 || message.termReason != "" || publisher.envelope != nil {
		t.Fatalf("delivery state = ack=%v nak=%s term=%q deadLetter=%v", message.acked, message.nakDelay, message.termReason, publisher.envelope != nil)
	}
}

func TestConsumerProcessorRetriesWithExponentialDelay(t *testing.T) {
	t.Parallel()
	var reported error
	processor := testProcessor(func(context.Context, *commonv1.EventEnvelope) error {
		return errors.New("dependency unavailable")
	}, new(fakeEnvelopePublisher))
	processor.onError = func(err error) { reported = err }
	message := &fakeConsumerMessage{
		data: encodedEnvelope(t, "platform.example.changed.v1"), headers: make(nats.Header),
		subject: "platform.example.changed.v1", metadata: &jetstream.MsgMetadata{NumDelivered: 3},
	}

	processor.process(t.Context(), message)

	if message.nakDelay != 4*time.Second || reported == nil || message.acked || message.termReason != "" {
		t.Fatalf("retry state = delay=%s reported=%v ack=%v term=%q", message.nakDelay, reported, message.acked, message.termReason)
	}
}

func TestConsumerProcessorPublishesDeadLetterOnFinalFailure(t *testing.T) {
	t.Parallel()
	publisher := new(fakeEnvelopePublisher)
	processor := testProcessor(func(context.Context, *commonv1.EventEnvelope) error {
		return errors.New("permanent failure")
	}, publisher)
	message := &fakeConsumerMessage{
		data: encodedEnvelope(t, "platform.example.changed.v1"), headers: make(nats.Header),
		subject:  "platform.example.changed.v1",
		metadata: &jetstream.MsgMetadata{NumDelivered: uint64(processor.config.ConsumerMaxDeliver)},
	}

	processor.process(t.Context(), message)

	if publisher.subject != DeadLetterSubject || publisher.envelope == nil || message.termReason != "permanent failure" {
		t.Fatalf("dead letter state = subject=%q envelope=%v term=%q", publisher.subject, publisher.envelope != nil, message.termReason)
	}
	payload := new(commonv1.DeadLetterEvent)
	if err := DecodePayload(publisher.envelope, payload); err != nil {
		t.Fatal(err)
	}
	if payload.OriginalEvent.GetEventId() != "event-1" || payload.DeliveryCount != uint64(processor.config.ConsumerMaxDeliver) || payload.DecodingFailed {
		t.Fatalf("dead letter payload = %+v", payload)
	}
}

func TestConsumerProcessorDeadLettersMalformedEnvelope(t *testing.T) {
	t.Parallel()
	publisher := new(fakeEnvelopePublisher)
	processor := testProcessor(func(context.Context, *commonv1.EventEnvelope) error {
		t.Fatal("handler called for malformed envelope")
		return nil
	}, publisher)
	processor.config.DeadLetterMaxDataBytes = 4
	message := &fakeConsumerMessage{
		data: []byte("not-a-protobuf-envelope"), headers: nats.Header{HeaderRequestID: []string{"request-raw"}},
		subject: "platform.example.changed.v1", metadata: &jetstream.MsgMetadata{NumDelivered: 1},
	}

	processor.process(t.Context(), message)

	if message.termReason != "invalid event envelope" || publisher.envelope == nil {
		t.Fatalf("malformed state = term=%q envelope=%v", message.termReason, publisher.envelope != nil)
	}
	payload := new(commonv1.DeadLetterEvent)
	if err := DecodePayload(publisher.envelope, payload); err != nil {
		t.Fatal(err)
	}
	if !payload.DecodingFailed || !payload.OriginalDataTruncated || string(payload.OriginalData) != "not-" {
		t.Fatalf("malformed dead letter payload = %+v", payload)
	}
}

func TestConsumerProcessorRetriesWhenDeadLetterPublishFails(t *testing.T) {
	t.Parallel()
	publisher := &fakeEnvelopePublisher{err: errors.New("NATS unavailable")}
	processor := testProcessor(func(context.Context, *commonv1.EventEnvelope) error {
		return errors.New("permanent failure")
	}, publisher)
	message := &fakeConsumerMessage{
		data: encodedEnvelope(t, "platform.example.changed.v1"), headers: make(nats.Header),
		subject:  "platform.example.changed.v1",
		metadata: &jetstream.MsgMetadata{NumDelivered: uint64(processor.config.ConsumerMaxDeliver)},
	}

	processor.process(t.Context(), message)

	if message.nakDelay == 0 || message.termReason != "" {
		t.Fatalf("failed dead letter state = delay=%s term=%q", message.nakDelay, message.termReason)
	}
}

func TestConsumerProcessorDoesNotRecursivelyDeadLetter(t *testing.T) {
	t.Parallel()
	publisher := new(fakeEnvelopePublisher)
	processor := testProcessor(func(context.Context, *commonv1.EventEnvelope) error {
		return errors.New("dead letter sink failed")
	}, publisher)
	message := &fakeConsumerMessage{
		data: encodedEnvelope(t, DeadLetterEventType), headers: make(nats.Header),
		subject:  DeadLetterSubject,
		metadata: &jetstream.MsgMetadata{NumDelivered: uint64(processor.config.ConsumerMaxDeliver)},
	}

	processor.process(t.Context(), message)

	if publisher.envelope != nil || message.termReason == "" {
		t.Fatalf("recursive dead letter state = envelope=%v term=%q", publisher.envelope != nil, message.termReason)
	}
}

func TestConsumerProcessorExtractsTraceContext(t *testing.T) {
	t.Parallel()
	processor := testProcessor(func(ctx context.Context, _ *commonv1.EventEnvelope) error {
		spanContext := trace.SpanContextFromContext(ctx)
		if !spanContext.IsValid() || spanContext.TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
			t.Fatalf("span context = %v", spanContext)
		}
		return nil
	}, new(fakeEnvelopePublisher))
	message := &fakeConsumerMessage{
		data:    encodedEnvelope(t, "platform.example.changed.v1"),
		headers: nats.Header{HeaderTraceParent: []string{"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}},
		subject: "platform.example.changed.v1", metadata: &jetstream.MsgMetadata{NumDelivered: 1},
	}

	processor.process(t.Context(), message)
	if !message.acked {
		t.Fatal("trace-context delivery was not acknowledged")
	}
}

func TestLimitedReasonPreservesValidUTF8(t *testing.T) {
	t.Parallel()
	reason := limitedReason(errors.New(strings.Repeat("界", 2000)))
	if len(reason) > 4096 || !utf8.ValidString(reason) {
		t.Fatalf("limited reason length=%d", len(reason))
	}
}

func TestNewRejectsInvalidConsumerTiming(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config Config
	}{
		{
			name:   "handler timeout reaches ack wait",
			config: Config{ConsumerAckWait: time.Second, ConsumerHandlerTimeout: time.Second},
		},
		{
			name:   "retry delay exceeds maximum",
			config: Config{ConsumerRetryDelay: 2 * time.Second, ConsumerMaxRetryDelay: time.Second},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := New(t.Context(), test.config); err == nil {
				t.Fatal("New() error = nil")
			}
		})
	}
}
