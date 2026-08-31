package outbox

import (
	"context"
	"errors"
	"time"
)

// PublishedStore owns a service-local outbox table. It must delete only rows
// which have completed publication; implementations must never remove pending
// or failed events.
type PublishedStore interface {
	DeletePublishedBefore(context.Context, time.Time, int) (int64, error)
}

type RetentionConfig struct {
	Retention time.Duration
	BatchSize int
}

// RetentionCleaner applies bounded cleanup for published events. Services run
// it on their own lifecycle and choose a retention window no shorter than the
// event stream's required replay horizon.
type RetentionCleaner struct {
	store     PublishedStore
	retention time.Duration
	batchSize int
	now       func() time.Time
}

func NewRetentionCleaner(store PublishedStore, config RetentionConfig) (*RetentionCleaner, error) {
	if store == nil || config.Retention <= 0 || config.BatchSize <= 0 {
		return nil, errors.New("published outbox store, retention, and batch size are required")
	}
	return &RetentionCleaner{store: store, retention: config.Retention, batchSize: config.BatchSize, now: time.Now}, nil
}

// RunOnce drains all currently eligible bounded batches. It returns after the
// first short batch so a busy service yields between scheduled cleanups.
func (c *RetentionCleaner) RunOnce(ctx context.Context) (int64, error) {
	var total int64
	cutoff := c.now().Add(-c.retention)
	for {
		deleted, err := c.store.DeletePublishedBefore(ctx, cutoff, c.batchSize)
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted < int64(c.batchSize) {
			return total, nil
		}
	}
}
