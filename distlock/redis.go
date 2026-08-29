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
		return nil, errors.New("lock retry delay must be positive")
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
		return nil, errors.New("lock key must not be empty")
	}
	if ttl <= 0 {
		return nil, errors.New("lock ttl must be positive")
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
		return fmt.Errorf("extend redis lock %q: lock is no longer owned", l.mutex.Name())
	}
	return nil
}

func (l *redisMutex) Unlock(ctx context.Context) error {
	ok, err := l.mutex.UnlockContext(ctx)
	if err != nil {
		return fmt.Errorf("release redis lock %q: %w", l.mutex.Name(), err)
	}
	if !ok {
		return fmt.Errorf("release redis lock %q: lock is no longer owned", l.mutex.Name())
	}
	return nil
}

func (l *redisMutex) Until() time.Time { return l.mutex.Until() }

func isContention(err error) bool {
	if errors.Is(err, redsync.ErrFailed) {
		return true
	}
	var taken *redsync.ErrTaken
	return errors.As(err, &taken)
}
