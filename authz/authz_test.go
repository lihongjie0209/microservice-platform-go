package authz_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/authz"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

type authorizer struct{ err error }

func (a authorizer) Authorize(context.Context, principal.Principal, authz.Requirement) error {
	return a.err
}

func TestEnforce(t *testing.T) {
	requirement := authz.Requirement{Resource: "tenant", Action: "update"}
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser})
	if err := authz.Enforce(ctx, authorizer{}, requirement); err != nil {
		t.Fatalf("Enforce() error = %v", err)
	}
	if err := authz.Enforce(ctx, authorizer{err: errors.New("no binding")}, requirement); !errors.Is(err, authz.ErrDenied) {
		t.Fatalf("Enforce() error = %v, want ErrDenied", err)
	}
	if err := authz.Enforce(t.Context(), authorizer{}, requirement); !errors.Is(err, principal.ErrMissing) {
		t.Fatalf("Enforce() error = %v, want ErrMissing", err)
	}
}
