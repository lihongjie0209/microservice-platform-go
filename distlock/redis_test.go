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

func TestWithLockRenewsLeaseDuringCallback(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	locker := distlock.NewRedisLocker(client)
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- distlock.WithLock(t.Context(), locker, "long-job", 120*time.Millisecond, 10*time.Millisecond, func(ctx context.Context) error {
			close(started)
			timer := time.NewTimer(260 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-timer.C:
				return nil
			}
		})
	}()
	<-started
	time.Sleep(180 * time.Millisecond)
	_, acquired, err := locker.TryLock(t.Context(), "long-job", time.Second)
	if err != nil || acquired {
		t.Fatalf("contending TryLock acquired=%v err=%v", acquired, err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestWithLockValidatesDependencies(t *testing.T) {
	err := distlock.WithLock(t.Context(), nil, "job", time.Second, time.Millisecond, func(context.Context) error { return nil })
	if !errors.Is(err, distlock.ErrInvalid) {
		t.Fatalf("WithLock error=%v", err)
	}
	locker := newLocker(t)
	err = distlock.WithLock(t.Context(), locker, "job", time.Nanosecond, time.Millisecond, func(context.Context) error { return nil })
	if !errors.Is(err, distlock.ErrInvalid) {
		t.Fatalf("short ttl error=%v", err)
	}
}

func TestTryWithLockSkipsContention(t *testing.T) {
	locker := newLocker(t)
	first, acquired, err := locker.TryLock(t.Context(), "scheduled-job", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("first lock acquired=%v err=%v", acquired, err)
	}
	t.Cleanup(func() { _ = first.Unlock(context.Background()) })
	called := false
	acquired, err = distlock.TryWithLock(t.Context(), locker, "scheduled-job", time.Minute, func(context.Context) error {
		called = true
		return nil
	})
	if err != nil || acquired || called {
		t.Fatalf("TryWithLock acquired=%v called=%v err=%v", acquired, called, err)
	}
}

func TestTryWithLockRenewsAcquiredLease(t *testing.T) {
	locker := newLocker(t)
	acquired, err := distlock.TryWithLock(t.Context(), locker, "scheduled-job", 90*time.Millisecond, func(ctx context.Context) error {
		timer := time.NewTimer(210 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			return nil
		}
	})
	if err != nil || !acquired {
		t.Fatalf("TryWithLock acquired=%v err=%v", acquired, err)
	}
}

func TestExtendReportsOwnershipLoss(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	locker := distlock.NewRedisLocker(client)
	lock, acquired, err := locker.TryLock(t.Context(), "lost", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquired=%v err=%v", acquired, err)
	}
	server.Del("lock:lost")
	if err := lock.Extend(t.Context()); !errors.Is(err, distlock.ErrNotOwned) {
		t.Fatalf("Extend error=%v", err)
	}
}

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
