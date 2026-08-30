package eventbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
)

const (
	HeaderRequestID   = "X-Request-ID"
	HeaderTraceID     = "X-Trace-ID"
	HeaderTraceParent = "traceparent"
)

type Handler func(context.Context, *commonv1.EventEnvelope) error

type Bus struct {
	config Config
	conn   *nats.Conn
	js     jetstream.JetStream
	stream jetstream.Stream
	mu     sync.Mutex
	closed bool
}

func New(ctx context.Context, config Config) (*Bus, error) {
	config.defaults()
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("validate event bus config: %w", err)
	}
	connection, err := nats.Connect(strings.Join(config.URLs, ","),
		nats.Name(config.ClientName), nats.Timeout(config.ConnectTimeout),
		nats.MaxReconnects(-1), nats.ReconnectWait(config.ReconnectWait),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("create JetStream context: %w", err)
	}
	storage := jetstream.FileStorage
	if config.Storage == "memory" {
		storage = jetstream.MemoryStorage
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: config.StreamName, Subjects: config.Subjects, Storage: storage,
		Retention: jetstream.LimitsPolicy, MaxAge: config.MaxAge,
		Duplicates: config.DuplicateWindow,
	})
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("provision JetStream stream: %w", err)
	}
	return &Bus{config: config, conn: connection, js: js, stream: stream}, nil
}

func (b *Bus) Publish(ctx context.Context, subject string, envelope *commonv1.EventEnvelope) error {
	if b == nil {
		return errors.New("event bus is disabled")
	}
	if envelope == nil || envelope.EventId == "" {
		return errors.New("event envelope with event_id is required")
	}
	data, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode event envelope: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, b.config.PublishTimeout)
	defer cancel()
	message := nats.NewMsg(subject)
	message.Data = data
	if envelope.Context != nil {
		message.Header.Set(HeaderRequestID, envelope.Context.RequestId)
		message.Header.Set(HeaderTraceID, envelope.Context.TraceId)
	}
	propagation.TraceContext{}.Inject(ctx, natsHeaderCarrier(message.Header))
	if _, err := b.js.PublishMsg(publishCtx, message, jetstream.WithMsgID(envelope.EventId)); err != nil {
		return fmt.Errorf("publish event %q: %w", envelope.EventId, err)
	}
	return nil
}

func (b *Bus) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	if err := b.conn.Drain(); err != nil {
		b.conn.Close()
		return fmt.Errorf("drain NATS connection: %w", err)
	}
	b.conn.Close()
	return nil
}
