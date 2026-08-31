package inbox

import (
	"context"
	"testing"
	"time"
)

type completedStoreStub struct {
	counts []int64
	before []time.Time
}

func (s *completedStoreStub) DeleteCompletedBefore(_ context.Context, before time.Time, _ int) (int64, error) {
	s.before = append(s.before, before)
	count := s.counts[0]
	s.counts = s.counts[1:]
	return count, nil
}

func TestRetentionCleanerRunOnceUsesBoundedBatches(t *testing.T) {
	t.Parallel()
	store := &completedStoreStub{counts: []int64{2, 1}}
	cleaner, err := NewRetentionCleaner(store, RetentionConfig{Retention: 30 * 24 * time.Hour, BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	cleaner.now = func() time.Time { return now }

	deleted, err := cleaner.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if deleted != 3 || len(store.before) != 2 {
		t.Fatalf("deleted/calls = %d/%d, want 3/2", deleted, len(store.before))
	}
}

func TestNewRetentionCleanerRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	if _, err := NewRetentionCleaner(nil, RetentionConfig{Retention: time.Hour, BatchSize: 1}); err == nil {
		t.Fatal("NewRetentionCleaner() error = nil")
	}
}
