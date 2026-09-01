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
			return nil, grpcAuthorizationError(err)
		}
		return handler(ctx, request)
	}
}

// StreamServerInterceptor enforces authorization requirements for streaming RPCs.
func StreamServerInterceptor(authorizer Authorizer, resolve GRPCResolver) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		requirement, protected := resolve(info.FullMethod)
		if !protected {
			return handler(server, stream)
		}
		if err := Enforce(stream.Context(), authorizer, requirement); err != nil {
			return grpcAuthorizationError(err)
		}
		return handler(server, stream)
	}
}

func grpcAuthorizationError(err error) error {
	if errors.Is(err, principal.ErrMissing) {
		return status.Error(codes.Unauthenticated, "authenticated principal is required")
	}
	if errors.Is(err, ErrDenied) || errors.Is(err, ErrInvalidPrincipal) {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	return status.Error(codes.Unavailable, "authorization decision is unavailable")
}
