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
	"google.golang.org/protobuf/proto"
)

const (
	HeaderRequestID = "X-Request-ID"
	HeaderTraceID   = "Traceparent"
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
	if _, err := b.js.PublishMsg(publishCtx, message, jetstream.WithMsgID(envelope.EventId)); err != nil {
		return fmt.Errorf("publish event %q: %w", envelope.EventId, err)
	}
	return nil
}

func (b *Bus) Consume(ctx context.Context, durable, filterSubject string, handler Handler) error {
	if b == nil {
		return errors.New("event bus is disabled")
	}
	if durable == "" || filterSubject == "" || handler == nil {
		return errors.New("durable name, filter subject, and handler are required")
	}
	consumer, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: durable, FilterSubject: filterSubject,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: b.config.ConsumerAckWait,
		MaxDeliver: b.config.ConsumerMaxDeliver,
	})
	if err != nil {
		return fmt.Errorf("provision consumer %q: %w", durable, err)
	}
	consumeContext, err := consumer.Consume(func(message jetstream.Msg) {
		envelope := new(commonv1.EventEnvelope)
		if err := proto.Unmarshal(message.Data(), envelope); err != nil {
			_ = message.Term()
			return
		}
		messageContext := ctx
		if err := handler(messageContext, envelope); err != nil {
			_ = message.Nak()
			return
		}
		_ = message.Ack()
	})
	if err != nil {
		return fmt.Errorf("start consumer %q: %w", durable, err)
	}
	defer consumeContext.Stop()
	<-ctx.Done()
	return ctx.Err()
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
