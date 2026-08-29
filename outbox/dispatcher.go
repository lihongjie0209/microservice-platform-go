// Package outbox provides a reusable transactional-outbox dispatcher. Domain
// services own their SQL schema and implement Store; the dispatcher owns retry
// flow and publishes only the persisted shared event envelope.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
)

type Event struct {
	ID       string
	Subject  string
	Envelope *commonv1.EventEnvelope
}

type Store interface {
	Claim(context.Context, int, time.Duration) ([]Event, error)
	MarkPublished(context.Context, Event, time.Time) error
	MarkFailed(context.Context, Event, string, time.Time) error
}

type Publisher interface {
	Publish(context.Context, string, *commonv1.EventEnvelope) error
}

type Dispatcher struct {
	store      Store
	publisher  Publisher
	batchSize  int
	lease      time.Duration
	retryDelay time.Duration
	now        func() time.Time
}

type Config struct {
	BatchSize  int
	Lease      time.Duration
	RetryDelay time.Duration
}

func New(store Store, publisher Publisher, config Config) (*Dispatcher, error) {
	if store == nil || publisher == nil {
		return nil, errors.New("outbox store and publisher are required")
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.Lease <= 0 {
		config.Lease = 30 * time.Second
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 5 * time.Second
	}
	return &Dispatcher{store: store, publisher: publisher, batchSize: config.BatchSize, lease: config.Lease, retryDelay: config.RetryDelay, now: time.Now}, nil
}

// RunOnce dispatches one claimed batch. Individual publish failures are
// persisted and do not prevent unrelated events in the batch from progressing.
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	events, err := d.store.Claim(ctx, d.batchSize, d.lease)
	if err != nil {
		return 0, fmt.Errorf("claim outbox events: %w", err)
	}
	published := 0
	var dispatchErrors []error
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return published, errors.Join(append(dispatchErrors, err)...)
		}
		if err := d.publisher.Publish(ctx, event.Subject, event.Envelope); err != nil {
			retryAt := d.now().Add(d.retryDelay)
			if markErr := d.store.MarkFailed(ctx, event, err.Error(), retryAt); markErr != nil {
				dispatchErrors = append(dispatchErrors, errors.Join(err, fmt.Errorf("mark event %q failed: %w", event.ID, markErr)))
			} else {
				dispatchErrors = append(dispatchErrors, fmt.Errorf("publish event %q: %w", event.ID, err))
			}
			continue
		}
		if err := d.store.MarkPublished(ctx, event, d.now()); err != nil {
			dispatchErrors = append(dispatchErrors, fmt.Errorf("mark event %q published: %w", event.ID, err))
			continue
		}
		published++
	}
	return published, errors.Join(dispatchErrors...)
}
