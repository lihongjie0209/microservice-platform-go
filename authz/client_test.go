package authz_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/authz"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	authorizationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/authorization/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type checker struct {
	request    *authorizationv1.CheckRequest
	credential string
	allowed    bool
	err        error
}

func (c *checker) Check(ctx context.Context, request *authorizationv1.CheckRequest, _ ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	c.request = request
	values, _ := metadata.FromOutgoingContext(ctx)
	if authorization := values.Get("authorization"); len(authorization) > 0 {
		c.credential = authorization[0]
	}
	return &authorizationv1.CheckResponse{Allowed: c.allowed}, c.err
}

func TestGRPCAuthorizerMapsMembershipAndForwardsCredential(t *testing.T) {
	client := &checker{allowed: true}
	authorizer := authz.NewGRPCAuthorizer(client, time.Second)
	ctx := authz.WithCallerCredential(t.Context(), "Bearer caller-token")
	err := authorizer.Authorize(ctx, principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1", MembershipID: "membership-1"}, authz.Requirement{Resource: "application.menu", ResourceID: "menu-1", Action: "read", Attributes: map[string]string{"environment": "production"}})
	if err != nil {
		t.Fatal(err)
	}
	if client.request.GetSubject().GetId() != "membership-1" || client.request.GetSubject().GetType() != authorizationv1.SubjectType_SUBJECT_TYPE_MEMBERSHIP || client.request.GetTenantId() != "tenant-1" {
		t.Fatalf("request = %+v", client.request)
	}
	if client.request.GetResourceId() != "menu-1" || client.request.GetAttributes()["environment"] != "production" || client.credential != "Bearer caller-token" {
		t.Fatalf("request = %+v credential = %q", client.request, client.credential)
	}
}

func TestGRPCAuthorizerMapsGlobalUserToPlatformScope(t *testing.T) {
	client := &checker{allowed: true}
	authorizer := authz.NewGRPCAuthorizer(client, time.Second)
	err := authorizer.Authorize(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser}, authz.Requirement{Resource: "identity.user", Action: "list", Scope: authz.ScopePlatform})
	if err != nil {
		t.Fatal(err)
	}
	if client.request.GetTenantId() != authz.PlatformTenantID || client.request.GetSubject().GetId() != "user-1" || client.request.GetSubject().GetType() != authorizationv1.SubjectType_SUBJECT_TYPE_USER {
		t.Fatalf("request = %+v", client.request)
	}
}

func TestGRPCAuthorizerPrincipalScopePreservesTenantBoundary(t *testing.T) {
	for _, test := range []struct {
		name       string
		identity   principal.Principal
		wantTenant string
		wantID     string
		wantType   authorizationv1.SubjectType
	}{
		{name: "tenant membership", identity: principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1", MembershipID: "membership-1"}, wantTenant: "tenant-1", wantID: "membership-1", wantType: authorizationv1.SubjectType_SUBJECT_TYPE_MEMBERSHIP},
		{name: "platform user", identity: principal.Principal{ID: "user-1", Type: principal.TypeUser}, wantTenant: authz.PlatformTenantID, wantID: "user-1", wantType: authorizationv1.SubjectType_SUBJECT_TYPE_USER},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &checker{allowed: true}
			authorizer := authz.NewGRPCAuthorizer(client, time.Second)
			if err := authorizer.Authorize(t.Context(), test.identity, authz.Requirement{Resource: "authorization.role", Action: "list", Scope: authz.ScopePrincipal}); err != nil {
				t.Fatal(err)
			}
			if client.request.GetTenantId() != test.wantTenant || client.request.GetSubject().GetId() != test.wantID || client.request.GetSubject().GetType() != test.wantType {
				t.Fatalf("request = %+v", client.request)
			}
		})
	}
}

func TestGRPCAuthorizerFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		client   authz.CheckClient
		identity principal.Principal
		want     error
	}{
		{name: "missing client", identity: principal.Principal{ID: "service-1", Type: principal.TypeServiceAccount, TenantID: "tenant-1"}, want: authz.ErrDecisionUnavailable},
		{name: "user missing membership", client: &checker{allowed: true}, identity: principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1"}, want: authz.ErrInvalidPrincipal},
		{name: "missing tenant", client: &checker{allowed: true}, identity: principal.Principal{ID: "service-1", Type: principal.TypeServiceAccount}, want: authz.ErrInvalidPrincipal},
		{name: "denied", client: &checker{}, identity: principal.Principal{ID: "service-1", Type: principal.TypeServiceAccount, TenantID: "tenant-1"}, want: authz.ErrDenied},
		{name: "upstream failure", client: &checker{err: errors.New("unavailable")}, identity: principal.Principal{ID: "service-1", Type: principal.TypeServiceAccount, TenantID: "tenant-1"}, want: authz.ErrDecisionUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authorizer := authz.NewGRPCAuthorizer(test.client, time.Second)
			err := authorizer.Authorize(t.Context(), test.identity, authz.Requirement{Resource: "resource", Action: "read"})
			if !errors.Is(err, test.want) {
				t.Fatalf("Authorize() error = %v, want %v", err, test.want)
			}
		})
	}
}
