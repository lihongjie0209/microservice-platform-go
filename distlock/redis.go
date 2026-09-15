// Package distlock provides the platform's distributed locking contract and
// its Redis implementation. Locks are leases: callers must finish before the
// lease expires or extend it while they still own it.
package distlock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

const defaultPrefix = "lock:"

var (
	ErrInvalid  = errors.New("invalid distributed lock request")
	ErrNotOwned = errors.New("distributed lock is no longer owned")
)

// Mutex is an acquired, ownership-token-protected lease.
type Mutex interface {
	Extend(context.Context) error
	Unlock(context.Context) error
	Until() time.Time
}

// Locker is the service-facing distributed lock contract.
type Locker interface {
	TryLock(context.Context, string, time.Duration) (Mutex, bool, error)
	Lock(context.Context, string, time.Duration, time.Duration) (Mutex, error)
}

// RedisLocker implements Locker with Redsync and a go-redis client.
type RedisLocker struct {
	redsync *redsync.Redsync
	prefix  string
}

type redisMutex struct{ mutex *redsync.Mutex }

// NewRedisLocker creates a locker using the platform lock key namespace.
func NewRedisLocker(client redis.UniversalClient) *RedisLocker {
	return NewRedisLockerWithPrefix(client, defaultPrefix)
}

// NewRedisLockerWithPrefix creates a locker with an application-specific key
// namespace. The prefix must be non-empty to prevent collisions with data keys.
func NewRedisLockerWithPrefix(client redis.UniversalClient, prefix string) *RedisLocker {
	if strings.TrimSpace(prefix) == "" {
		prefix = defaultPrefix
	}
	return &RedisLocker{
		redsync: redsync.New(redsyncgoredis.NewPool(client)),
		prefix:  prefix,
	}
}

func (l *RedisLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (Mutex, bool, error) {
	mutex, err := l.newMutex(key, ttl, redsync.WithTries(1))
	if err != nil {
		return nil, false, err
	}
	if err := mutex.TryLockContext(ctx); err != nil {
		if isContention(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("try redis lock %q: %w", key, err)
	}
	return &redisMutex{mutex: mutex}, true, nil
}

func (l *RedisLocker) Lock(ctx context.Context, key string, ttl, retryDelay time.Duration) (Mutex, error) {
	if retryDelay <= 0 {
		return nil, fmt.Errorf("%w: retry delay must be positive", ErrInvalid)
	}
	mutex, err := l.newMutex(key, ttl, redsync.WithRetryDelay(retryDelay))
	if err != nil {
		return nil, err
	}
	if err := mutex.LockContext(ctx); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("wait for redis lock %q: %w", key, ctxErr)
		}
		return nil, fmt.Errorf("acquire redis lock %q: %w", key, err)
	}
	return &redisMutex{mutex: mutex}, nil
}

func (l *RedisLocker) newMutex(key string, ttl time.Duration, options ...redsync.Option) (*redsync.Mutex, error) {
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("%w: key must not be empty", ErrInvalid)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("%w: ttl must be positive", ErrInvalid)
	}
	options = append(options, redsync.WithExpiry(ttl))
	return l.redsync.NewMutex(l.prefix+key, options...), nil
}

func (l *redisMutex) Extend(ctx context.Context) error {
	ok, err := l.mutex.ExtendContext(ctx)
	if err != nil {
		return fmt.Errorf("extend redis lock %q: %w", l.mutex.Name(), err)
	}
	if !ok {
		return fmt.Errorf("extend redis lock %q: %w", l.mutex.Name(), ErrNotOwned)
	}
	return nil
}

func (l *redisMutex) Unlock(ctx context.Context) error {
	ok, err := l.mutex.UnlockContext(ctx)
	if err != nil {
		return fmt.Errorf("release redis lock %q: %w", l.mutex.Name(), err)
	}
	if !ok {
		return fmt.Errorf("release redis lock %q: %w", l.mutex.Name(), ErrNotOwned)
	}
	return nil
}

// WithLock acquires a lease, renews it every third of its TTL, and cancels the
// callback context if renewal proves that ownership was lost. The callback
// must honor cancellation and durable writes still need an optimistic version
// or fencing token because no lease can stop a paused process from resuming.
func WithLock(ctx context.Context, locker Locker, key string, ttl, retryDelay time.Duration, fn func(context.Context) error) error {
	if locker == nil || fn == nil {
		return fmt.Errorf("%w: locker and callback are required", ErrInvalid)
	}
	mutex, err := locker.Lock(ctx, key, ttl, retryDelay)
	if err != nil {
		return err
	}
	leaseCtx, cancel := context.WithCancelCause(ctx)
	extendErrors := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := ttl / 3
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				if extendErr := mutex.Extend(leaseCtx); extendErr != nil {
					extendErrors <- extendErr
					cancel(extendErr)
					return
				}
			}
		}
	}()
	callbackErr := fn(leaseCtx)
	cancel(nil)
	<-done
	var extendErr error
	select {
	case extendErr = <-extendErrors:
	default:
	}
	unlockTimeout := ttl
	if unlockTimeout > 5*time.Second {
		unlockTimeout = 5 * time.Second
	}
	unlockCtx, unlockCancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	unlockErr := mutex.Unlock(unlockCtx)
	unlockCancel()
	return errors.Join(callbackErr, extendErr, unlockErr)
}

func (l *redisMutex) Until() time.Time { return l.mutex.Until() }

func isContention(err error) bool {
	if errors.Is(err, redsync.ErrFailed) {
		return true
	}
	var taken *redsync.ErrTaken
	return errors.As(err, &taken)
}
