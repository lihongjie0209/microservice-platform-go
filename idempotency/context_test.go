package idempotency

import (
	"context"
	"testing"
)

func TestContextAndKeyValidation(t *testing.T) {
	t.Parallel()
	if !Valid("request-0001") || Valid("short") || Valid("request key with spaces") {
		t.Fatal("unexpected key validation result")
	}
	ctx := WithContext(context.Background(), "request-0001")
	if got, ok := FromContext(ctx); !ok || got != "request-0001" {
		t.Fatalf("FromContext() = %q, %v", got, ok)
	}
}
