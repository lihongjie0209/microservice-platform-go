package audit_test

import (
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/audit"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

func TestNew(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "account-1", Type: principal.TypeServiceAccount})
	fields, err := audit.New(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if fields.Version != 1 || fields.CreatedBy != "account-1" || fields.UpdatedBy != "account-1" || !fields.CreatedAt.Equal(now) {
		t.Fatalf("New() = %+v", fields)
	}
}
