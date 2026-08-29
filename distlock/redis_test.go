package distlock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/lihongjie0209/microservice-platform-go/distlock"
	"github.com/redis/go-redis/v9"
)

func TestRedisLocker_ContentionOwnershipAndReuse(t *testing.T) {
	locker := newLocker(t)
	first, acquired, err := locker.TryLock(t.Context(), "job", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("first TryLock() = (_, %v, %v), want acquired", acquired, err)
	}
	if _, acquired, err = locker.TryLock(t.Context(), "job", time.Minute); err != nil || acquired {
		t.Fatalf("contended TryLock() = (_, %v, %v), want not acquired", acquired, err)
	}
	if err = first.Unlock(t.Context()); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
	second, acquired, err := locker.TryLock(t.Context(), "job", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("reused TryLock() = (_, %v, %v), want acquired", acquired, err)
	}
	if err = first.Unlock(t.Context()); err == nil {
		t.Fatal("stale owner Unlock() error = nil, want ownership error")
	}
	if err = second.Unlock(t.Context()); err != nil {
		t.Fatalf("current owner Unlock() error = %v", err)
	}
}

func TestRedisLocker_WaitHonorsContext(t *testing.T) {
	locker := newLocker(t)
	lock, acquired, err := locker.TryLock(t.Context(), "job", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("TryLock() = (_, %v, %v), want acquired", acquired, err)
	}
	t.Cleanup(func() { _ = lock.Unlock(context.Background()) })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = locker.Lock(ctx, "job", time.Minute, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Lock() error = %v, want context.Canceled", err)
	}
}

func TestRedisLocker_Validation(t *testing.T) {
	locker := newLocker(t)
	tests := []struct {
		name string
		key  string
		ttl  time.Duration
	}{
		{name: "empty key", ttl: time.Second},
		{name: "blank key", key: "  ", ttl: time.Second},
		{name: "invalid ttl", key: "key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := locker.TryLock(t.Context(), tt.key, tt.ttl)
			if err == nil {
				t.Fatal("TryLock() error = nil, want error")
			}
		})
	}
}

func newLocker(t *testing.T) *distlock.RedisLocker {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return distlock.NewRedisLocker(client)
}
