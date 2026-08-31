package inbox

import (
	"context"
	"errors"
	"time"
)

type CompletedStore interface {
	DeleteCompletedBefore(context.Context, time.Time, int) (int64, error)
}

type RetentionConfig struct {
	Retention time.Duration
	BatchSize int
}

type RetentionCleaner struct {
	store     CompletedStore
	retention time.Duration
	batchSize int
	now       func() time.Time
}

func NewRetentionCleaner(store CompletedStore, config RetentionConfig) (*RetentionCleaner, error) {
	if store == nil || config.Retention <= 0 || config.BatchSize <= 0 {
		return nil, errors.New("completed inbox store, retention, and batch size are required")
	}
	return &RetentionCleaner{store: store, retention: config.Retention, batchSize: config.BatchSize, now: time.Now}, nil
}

func (c *RetentionCleaner) RunOnce(ctx context.Context) (int64, error) {
	var total int64
	cutoff := c.now().Add(-c.retention)
	for {
		deleted, err := c.store.DeleteCompletedBefore(ctx, cutoff, c.batchSize)
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted < int64(c.batchSize) {
			return total, nil
		}
	}
}
