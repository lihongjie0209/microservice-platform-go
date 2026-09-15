package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testManager(t *testing.T, server *miniredis.Miniredis, service string) *Manager {
	t.Helper()
	return New(redis.NewClient(&redis.Options{Addr: server.Addr()}), Config{
		Enabled:       true,
		Service:       service,
		ProcessingTTL: time.Minute,
		ResultTTL:     time.Hour,
		FailureTTL:    time.Minute,
	})
}

func TestManagerStateTransitionsAndServiceIsolation(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	manager := testManager(t, server, "billing-service")
	ctx := context.Background()

	acquired, err := manager.Begin(ctx, "operation-1", "fingerprint-1")
	if err != nil || acquired.State != StateAcquired || acquired.Owner == "" {
		t.Fatalf("acquired=%+v error=%v", acquired, err)
	}
	processing, err := manager.Begin(ctx, "operation-1", "fingerprint-1")
	if err != nil || processing.State != StateProcessing {
		t.Fatalf("processing=%+v error=%v", processing, err)
	}
	conflict, err := manager.Begin(ctx, "operation-1", "fingerprint-2")
	if err != nil || conflict.State != StateConflict {
		t.Fatalf("conflict=%+v error=%v", conflict, err)
	}
	if err := manager.Complete(ctx, "operation-1", acquired.Owner, map[string]string{"id": "result-1"}); err != nil {
		t.Fatal(err)
	}
	completed, err := manager.Begin(ctx, "operation-1", "fingerprint-1")
	if err != nil || completed.State != StateCompleted {
		t.Fatalf("completed=%+v error=%v", completed, err)
	}
	var response map[string]string
	if err := json.Unmarshal(completed.Response, &response); err != nil || response["id"] != "result-1" {
		t.Fatalf("response=%v error=%v", response, err)
	}

	other := testManager(t, server, "workflow-service")
	isolated, err := other.Begin(ctx, "operation-1", "fingerprint-2")
	if err != nil || isolated.State != StateAcquired {
		t.Fatalf("isolated=%+v error=%v", isolated, err)
	}
}

func TestManagerPersistsFailureAndRejectsExpiredOwner(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	manager := testManager(t, server, "billing-service")
	ctx := context.Background()
	acquired, err := manager.Begin(ctx, "operation-2", "fingerprint-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Complete(ctx, "operation-2", "other-owner", nil); err == nil {
		t.Fatal("Complete() error = nil for stale owner")
	}
	failure := Failure{Code: 10001, Message: "invalid", HTTPStatus: 400, GRPCCode: 3}
	if err := manager.Fail(ctx, "operation-2", acquired.Owner, failure); err != nil {
		t.Fatal(err)
	}
	failed, err := manager.Begin(ctx, "operation-2", "fingerprint-1")
	if err != nil || failed.State != StateFailed || failed.Failure != failure {
		t.Fatalf("failed=%+v error=%v", failed, err)
	}
}

func TestManagerLeaseRenewsProcessingState(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	manager := New(redis.NewClient(&redis.Options{Addr: server.Addr()}), Config{
		Enabled: true, Service: "billing-service", ProcessingTTL: 90 * time.Millisecond,
		ResultTTL: time.Hour, FailureTTL: time.Minute,
	})
	acquired, err := manager.Begin(t.Context(), "operation-lease", "fingerprint-1")
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx, stop, err := manager.StartLease(t.Context(), "operation-lease", acquired.Owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stop() })
	time.Sleep(55 * time.Millisecond)
	if err := leaseCtx.Err(); err != nil {
		t.Fatalf("lease context canceled after renewal: %v", err)
	}
	if ttl := server.TTL(manager.storageKey("operation-lease")); ttl < 60*time.Millisecond {
		t.Fatalf("renewed TTL = %v, want at least 60ms", ttl)
	}
	if err := stop(); err != nil {
		t.Fatalf("stop lease: %v", err)
	}
}

func TestManagerLeaseCancelsWhenOwnershipIsLost(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	manager := New(redis.NewClient(&redis.Options{Addr: server.Addr()}), Config{
		Enabled: true, Service: "billing-service", ProcessingTTL: 30 * time.Millisecond,
		ResultTTL: time.Hour, FailureTTL: time.Minute,
	})
	acquired, err := manager.Begin(t.Context(), "operation-lost", "fingerprint-1")
	if err != nil {
		t.Fatal(err)
	}
	leaseCtx, stop, err := manager.StartLease(t.Context(), "operation-lost", acquired.Owner)
	if err != nil {
		t.Fatal(err)
	}
	server.Del(manager.storageKey("operation-lost"))
	select {
	case <-leaseCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("lease context was not canceled")
	}
	if !errors.Is(context.Cause(leaseCtx), ErrOwnershipExpired) {
		t.Fatalf("lease cause = %v, want ErrOwnershipExpired", context.Cause(leaseCtx))
	}
	if err := stop(); !errors.Is(err, ErrOwnershipExpired) {
		t.Fatalf("stop lease = %v, want ErrOwnershipExpired", err)
	}
}
