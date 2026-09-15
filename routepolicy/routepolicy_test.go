package routepolicy

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/authz"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

type authorizerFunc func(context.Context, principal.Principal, authz.Requirement) error

func (f authorizerFunc) Authorize(ctx context.Context, p principal.Principal, r authz.Requirement) error {
	return f(ctx, p, r)
}

func TestStableRouteAndPolicySnapshot(t *testing.T) {
	route, err := NewRoute("HTTP", "POST", "/api/v1/orders/:id", "orders-service", "v1")
	if err != nil {
		t.Fatal(err)
	}
	again, _ := NewRoute("http", "post", "/api/v1/orders/:id", "orders-service", "v2")
	if route.ID == "" || route.ID != again.ID {
		t.Fatalf("route ids = %q, %q", route.ID, again.ID)
	}
	otherService, _ := NewRoute("http", "post", "/api/v1/orders/:id", "billing-service", "v1")
	if route.ID == otherService.ID {
		t.Fatal("different services must not share a route id")
	}
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewSnapshot(compiler)
	definition := Definition{ID: "policy-1", RouteID: route.ID, Expression: `authenticated && permissions["orders.read"]`, Permissions: map[string]Permission{"orders.read": {Key: "orders.read", Resource: "orders", Action: "read", Scope: authz.ScopeTenant}}}
	if err := snapshot.Replace([]Definition{definition}); err != nil {
		t.Fatal(err)
	}
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1", MembershipID: "member-1"})
	allow := authorizerFunc(func(context.Context, principal.Principal, authz.Requirement) error { return nil })
	if err := snapshot.Evaluate(ctx, route.ID, allow); err != nil {
		t.Fatal(err)
	}
	deny := authorizerFunc(func(context.Context, principal.Principal, authz.Requirement) error { return authz.ErrDenied })
	if err := snapshot.Evaluate(ctx, route.ID, deny); !errors.Is(err, ErrDenied) {
		t.Fatalf("Evaluate() error = %v", err)
	}
}

func TestPoliciesSupportAnonymousAndFailClosed(t *testing.T) {
	compiler, _ := NewCompiler()
	snapshot := NewSnapshot(compiler)
	if err := snapshot.Replace([]Definition{{ID: "public", RouteID: "route-public", Expression: "anonymous || authenticated"}}); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Evaluate(t.Context(), "route-public", nil); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Evaluate(t.Context(), "missing", nil); !errors.Is(err, ErrMissing) {
		t.Fatalf("missing error = %v", err)
	}
	if err := snapshot.Replace([]Definition{{ID: "invalid", RouteID: "route-invalid", Expression: `permissions["missing.read"]`}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid error = %v", err)
	}
}
