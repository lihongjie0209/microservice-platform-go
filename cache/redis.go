// Package cache provides the platform-wide distributed cache contract and a
// Redis implementation. Cached values must be disposable and reconstructable;
// durable business state belongs in a database or message log.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/redis/go-redis/v9"
)

var (
	ErrMiss          = errors.New("cache miss")
	ErrInvalidKey    = errors.New("invalid cache key")
	ErrInvalidTTL    = errors.New("invalid cache ttl")
	ErrValueTooLarge = errors.New("cache value too large")
	ErrUnavailable   = errors.New("cache backend unavailable")
)

const defaultMaxValueBytes = 16 << 20

type Store interface {
	Get(context.Context, string) ([]byte, error)
	Set(context.Context, string, []byte, time.Duration) error
	SetIfAbsent(context.Context, string, []byte, time.Duration) (bool, error)
	Delete(context.Context, ...string) error
	Exists(context.Context, string) (bool, error)
}

type RedisStore struct {
	client        redis.UniversalClient
	prefix        string
	maxValueBytes int
}

type Option func(*RedisStore)

func WithKeyPrefix(prefix string) Option { return func(store *RedisStore) { store.prefix = prefix } }
func WithMaxValueBytes(size int) Option {
	return func(store *RedisStore) { store.maxValueBytes = size }
}

func NewRedisStore(client redis.UniversalClient, options ...Option) *RedisStore {
	store := &RedisStore{client: client, maxValueBytes: defaultMaxValueBytes}
	for _, option := range options {
		option(store)
	}
	return store
}

func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
	qualified, err := s.qualifiedKey(key)
	if err != nil {
		return nil, err
	}
	value, err := s.client.Get(ctx, qualified).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrMiss
	}
	if err != nil {
		return nil, fmt.Errorf("get cache key %q: %w", qualified, err)
	}
	if len(value) > s.maxValueBytes {
		return nil, fmt.Errorf("%w: got %d bytes, maximum %d", ErrValueTooLarge, len(value), s.maxValueBytes)
	}
	return value, nil
}

func (s *RedisStore) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	qualified, err := s.validate(key, value, ttl)
	if err != nil {
		return err
	}
	if err := s.client.Set(ctx, qualified, value, ttl).Err(); err != nil {
		return fmt.Errorf("set cache key %q: %w", qualified, err)
	}
	return nil
}

func (s *RedisStore) SetIfAbsent(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	qualified, err := s.validate(key, value, ttl)
	if err != nil {
		return false, err
	}
	created, err := s.client.SetNX(ctx, qualified, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("set cache key %q if absent: %w", qualified, err)
	}
	return created, nil
}

func (s *RedisStore) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) > 1000 {
		return fmt.Errorf("%w: delete accepts at most 1000 keys", ErrInvalidKey)
	}
	qualified := make([]string, len(keys))
	for index, key := range keys {
		var err error
		qualified[index], err = s.qualifiedKey(key)
		if err != nil {
			return err
		}
	}
	if err := s.client.Del(ctx, qualified...).Err(); err != nil {
		return fmt.Errorf("delete cache keys: %w", err)
	}
	return nil
}

func (s *RedisStore) Exists(ctx context.Context, key string) (bool, error) {
	qualified, err := s.qualifiedKey(key)
	if err != nil {
		return false, err
	}
	count, err := s.client.Exists(ctx, qualified).Result()
	if err != nil {
		return false, fmt.Errorf("check cache key %q: %w", qualified, err)
	}
	return count > 0, nil
}

func (s *RedisStore) validate(key string, value []byte, ttl time.Duration) (string, error) {
	qualified, err := s.qualifiedKey(key)
	if err != nil {
		return "", err
	}
	if ttl < 0 {
		return "", ErrInvalidTTL
	}
	if len(value) > s.maxValueBytes {
		return "", fmt.Errorf("%w: got %d bytes, maximum %d", ErrValueTooLarge, len(value), s.maxValueBytes)
	}
	return qualified, nil
}

func (s *RedisStore) qualifiedKey(key string) (string, error) {
	if s == nil || s.client == nil {
		return "", ErrUnavailable
	}
	if key == "" || key != strings.TrimSpace(key) || len(key) > 1024 || strings.IndexFunc(key, unicode.IsControl) >= 0 {
		return "", ErrInvalidKey
	}
	return s.prefix + key, nil
}

func GetJSON[T any](ctx context.Context, store Store, key string) (T, error) {
	var value T
	if store == nil {
		return value, ErrUnavailable
	}
	data, err := store.Get(ctx, key)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("decode cache key %q: %w", key, err)
	}
	return value, nil
}

func SetJSON[T any](ctx context.Context, store Store, key string, value T, ttl time.Duration) error {
	if store == nil {
		return ErrUnavailable
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode cache key %q: %w", key, err)
	}
	return store.Set(ctx, key, data, ttl)
}

var _ Store = (*RedisStore)(nil)
