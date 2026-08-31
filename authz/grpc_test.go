package authz_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/authz"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryServerInterceptorClassifiesAuthorizationFailures(t *testing.T) {
	t.Parallel()
	requirement := func(string) (authz.Requirement, bool) {
		return authz.Requirement{Resource: "resource", Action: "read"}, true
	}
	if code := invokeAuthorizationInterceptor(t, authz.ErrDenied, requirement); code != codes.PermissionDenied {
		t.Fatalf("denied code = %s", code)
	}
	if code := invokeAuthorizationInterceptor(t, errors.New("upstream"), requirement); code != codes.Unavailable {
		t.Fatalf("upstream code = %s", code)
	}
}

func invokeAuthorizationInterceptor(t *testing.T, err error, resolver authz.GRPCResolver) codes.Code {
	t.Helper()
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "service-1", Type: principal.TypeServiceAccount, TenantID: "tenant-1"})
	_, callErr := authz.UnaryServerInterceptor(authorizer{err: err}, resolver)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Get"}, func(context.Context, any) (any, error) { return struct{}{}, nil })
	return status.Code(callErr)
}
