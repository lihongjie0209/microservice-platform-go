package idempotency

import (
	"context"
	"encoding/json"
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
