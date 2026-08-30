package eventbus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DeadLetterSubject   = "platform.system.event.dead-lettered.v1"
	DeadLetterEventType = "platform.system.event.dead-lettered.v1"
)

type ConsumerOptions struct {
	Durable       string
	FilterSubject string
	Handler       Handler
	OnError       func(error)
}

type consumerMessage interface {
	Data() []byte
	Headers() nats.Header
	Subject() string
	Metadata() (*jetstream.MsgMetadata, error)
	DoubleAck(context.Context) error
	NakWithDelay(time.Duration) error
	TermWithReason(string) error
}

type envelopePublisher interface {
	Publish(context.Context, string, *commonv1.EventEnvelope) error
}

type consumerProcessor struct {
	config    Config
	durable   string
	handler   Handler
	publisher envelopePublisher
	onError   func(error)
	now       func() time.Time
}

type requestIDContextKey struct{}

func RequestIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(requestIDContextKey{}).(string)
	return value
}

func (b *Bus) Consume(ctx context.Context, durable, filterSubject string, handler Handler) error {
	return b.ConsumeWithOptions(ctx, ConsumerOptions{
		Durable: durable, FilterSubject: filterSubject, Handler: handler,
	})
}

func (b *Bus) ConsumeWithOptions(ctx context.Context, options ConsumerOptions) error {
	if b == nil {
		return errors.New("event bus is disabled")
	}
	if options.Durable == "" || options.FilterSubject == "" || options.Handler == nil {
		return errors.New("durable name, filter subject, and handler are required")
	}
	consumer, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       options.Durable,
		FilterSubject: options.FilterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       b.config.ConsumerAckWait,
		MaxDeliver:    b.config.ConsumerMaxDeliver,
		MaxAckPending: b.config.ConsumerMaxAckPending,
	})
	if err != nil {
		return fmt.Errorf("provision consumer %q: %w", options.Durable, err)
	}
	processor := consumerProcessor{
		config: b.config, durable: options.Durable, handler: options.Handler,
		publisher: b, onError: options.OnError, now: time.Now,
	}
	consumeContext, err := consumer.Consume(func(message jetstream.Msg) {
		processor.process(ctx, message)
	})
	if err != nil {
		return fmt.Errorf("start consumer %q: %w", options.Durable, err)
	}
	defer consumeContext.Stop()
	<-ctx.Done()
	return ctx.Err()
}

func (p *consumerProcessor) process(ctx context.Context, message consumerMessage) {
	metadata, err := message.Metadata()
	if err != nil {
		p.report(fmt.Errorf("read consumer metadata: %w", err))
		p.retry(message, 1)
		return
	}
	envelope := new(commonv1.EventEnvelope)
	if err := proto.Unmarshal(message.Data(), envelope); err != nil {
		if publishErr := p.publishDeadLetter(ctx, message, metadata, nil, err, true); publishErr != nil {
			p.report(publishErr)
			p.retry(message, metadata.NumDelivered)
			return
		}
		p.terminate(message, "invalid event envelope")
		return
	}

	messageContext := propagation.TraceContext{}.Extract(ctx, natsHeaderCarrier(message.Headers()))
	if envelope.Context != nil && envelope.Context.RequestId != "" {
		messageContext = context.WithValue(messageContext, requestIDContextKey{}, envelope.Context.RequestId)
	}
	handlerContext, cancel := context.WithTimeout(messageContext, p.config.ConsumerHandlerTimeout)
	err = p.handler(handlerContext, envelope)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if metadata.NumDelivered >= uint64(p.config.ConsumerMaxDeliver) {
			if envelope.EventType != DeadLetterEventType {
				if publishErr := p.publishDeadLetter(ctx, message, metadata, envelope, err, false); publishErr != nil {
					p.report(publishErr)
					p.retry(message, metadata.NumDelivered)
					return
				}
			}
			p.terminate(message, limitedReason(err))
			return
		}
		p.report(fmt.Errorf("handle event %q delivery %d: %w", envelope.EventId, metadata.NumDelivered, err))
		p.retry(message, metadata.NumDelivered)
		return
	}

	ackContext, cancel := context.WithTimeout(ctx, p.config.ConsumerAckTimeout)
	defer cancel()
	if err := message.DoubleAck(ackContext); err != nil {
		p.report(fmt.Errorf("ack event %q: %w", envelope.EventId, err))
	}
}

func (p *consumerProcessor) publishDeadLetter(
	ctx context.Context,
	message consumerMessage,
	metadata *jetstream.MsgMetadata,
	original *commonv1.EventEnvelope,
	cause error,
	decodingFailed bool,
) error {
	deadLetter := &commonv1.DeadLetterEvent{
		OriginalEvent: original, OriginalSubject: message.Subject(), Consumer: p.durable,
		Reason: limitedReason(cause), DeliveryCount: metadata.NumDelivered,
		DeadLetteredAt: timestamppb.New(p.now().UTC()), DecodingFailed: decodingFailed,
	}
	metadataForEnvelope := Metadata{
		EventType: DeadLetterEventType, AggregateType: "event", SchemaVersion: 1,
		OccurredAt: p.now().UTC(),
	}
	if original != nil {
		metadataForEnvelope.EventID = "dead-letter:" + p.durable + ":" + original.EventId
		metadataForEnvelope.AggregateID = original.AggregateId
		metadataForEnvelope.TenantID = original.TenantId
		if original.Context != nil {
			metadataForEnvelope.RequestID = original.Context.RequestId
			metadataForEnvelope.TraceID = original.Context.TraceId
			metadataForEnvelope.ActorID = original.Context.ActorId
			metadataForEnvelope.ActorType = original.Context.ActorType
		}
	} else {
		raw := message.Data()
		if len(raw) > p.config.DeadLetterMaxDataBytes {
			raw = raw[:p.config.DeadLetterMaxDataBytes]
			deadLetter.OriginalDataTruncated = true
		}
		deadLetter.OriginalData = append([]byte(nil), raw...)
		digest := sha256.Sum256(message.Data())
		identifier := hex.EncodeToString(digest[:16])
		metadataForEnvelope.EventID = "dead-letter:" + p.durable + ":" + identifier
		metadataForEnvelope.AggregateID = identifier
		metadataForEnvelope.RequestID = message.Headers().Get(HeaderRequestID)
	}
	envelope, err := NewEnvelope(metadataForEnvelope, deadLetter)
	if err != nil {
		return fmt.Errorf("build dead letter envelope: %w", err)
	}
	if err := p.publisher.Publish(ctx, p.config.DeadLetterSubject, envelope); err != nil {
		return fmt.Errorf("publish dead letter for consumer %q: %w", p.durable, err)
	}
	return nil
}

func (p *consumerProcessor) retry(message consumerMessage, delivery uint64) {
	delay := p.retryDelay(delivery)
	if err := message.NakWithDelay(delay); err != nil {
		p.report(fmt.Errorf("schedule event redelivery after %s: %w", delay, err))
	}
}

func (p *consumerProcessor) terminate(message consumerMessage, reason string) {
	if err := message.TermWithReason(reason); err != nil {
		p.report(fmt.Errorf("terminate event delivery: %w", err))
	}
}

func (p *consumerProcessor) retryDelay(delivery uint64) time.Duration {
	var shift uint64
	if delivery > 1 {
		shift = delivery - 1
	}
	if shift > 16 {
		shift = 16
	}
	delay := p.config.ConsumerRetryDelay
	for range shift {
		if delay >= p.config.ConsumerMaxRetryDelay/2 {
			return p.config.ConsumerMaxRetryDelay
		}
		delay *= 2
	}
	return delay
}

func (p *consumerProcessor) report(err error) {
	if err != nil && p.onError != nil {
		p.onError(err)
	}
}

func limitedReason(err error) string {
	if err == nil {
		return "unknown consumer failure"
	}
	reason := strings.TrimSpace(err.Error())
	reason = strings.ToValidUTF8(reason, "�")
	if len(reason) > 4096 {
		reason = reason[:4096]
		for !utf8.ValidString(reason) {
			reason = reason[:len(reason)-1]
		}
	}
	return reason
}

type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string { return nats.Header(c).Get(key) }
func (c natsHeaderCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }
func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}
