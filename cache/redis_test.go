package cache

import (
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisStoreNamespaceTTLAndAtomicCreate(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(client, WithKeyPrefix("orders:"))
	created, err := store.SetIfAbsent(t.Context(), "item:1", []byte("first"), time.Minute)
	if err != nil || !created {
		t.Fatalf("first create=%v err=%v", created, err)
	}
	created, err = store.SetIfAbsent(t.Context(), "item:1", []byte("second"), time.Minute)
	if err != nil || created {
		t.Fatalf("second create=%v err=%v", created, err)
	}
	if !server.Exists("orders:item:1") || server.TTL("orders:item:1") != time.Minute {
		t.Fatal("namespace or ttl not applied")
	}
	value, err := store.Get(t.Context(), "item:1")
	if err != nil || string(value) != "first" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

func TestRedisStoreValidationAndMiss(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(client, WithMaxValueBytes(4))
	if _, err := store.Get(t.Context(), "missing"); !errors.Is(err, ErrMiss) {
		t.Fatalf("miss err=%v", err)
	}
	for _, key := range []string{"", " key", "key\n"} {
		if err := store.Set(t.Context(), key, nil, time.Second); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("key %q err=%v", key, err)
		}
	}
	if err := store.Set(t.Context(), "key", nil, -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("ttl err=%v", err)
	}
	if err := store.Set(t.Context(), "key", []byte("large"), time.Second); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("size err=%v", err)
	}
}

func TestJSONHelpers(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(client)
	type payload struct {
		ID string `json:"id"`
	}
	if err := SetJSON(t.Context(), store, "payload", payload{ID: "42"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	value, err := GetJSON[payload](t.Context(), store, "payload")
	if err != nil || value.ID != "42" {
		t.Fatalf("value=%+v err=%v", value, err)
	}
}
