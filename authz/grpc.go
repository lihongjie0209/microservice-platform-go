package authz

import (
	"context"
	"errors"

	"github.com/lihongjie0209/microservice-platform-go/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCResolver func(fullMethod string) (Requirement, bool)

func UnaryServerInterceptor(authorizer Authorizer, resolve GRPCResolver) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		requirement, protected := resolve(info.FullMethod)
		if !protected {
			return handler(ctx, request)
		}
		if err := Enforce(ctx, authorizer, requirement); err != nil {
			if errors.Is(err, principal.ErrMissing) {
				return nil, status.Error(codes.Unauthenticated, "authenticated principal is required")
			}
			if errors.Is(err, ErrDenied) || errors.Is(err, ErrInvalidPrincipal) {
				return nil, status.Error(codes.PermissionDenied, "permission denied")
			}
			return nil, status.Error(codes.Unavailable, "authorization decision is unavailable")
		}
		return handler(ctx, request)
	}
}
