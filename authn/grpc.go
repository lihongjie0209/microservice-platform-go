package authn

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func UnaryServerInterceptor(policy Policy) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		authenticated, err := policy.Authenticate(ctx, info.FullMethod, authorizationFromMetadata(ctx))
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid credential")
		}
		return handler(authenticated, request)
	}
}

func StreamServerInterceptor(policy Policy) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := policy.Authenticate(stream.Context(), info.FullMethod, authorizationFromMetadata(stream.Context()))
		if err != nil {
			return status.Error(codes.Unauthenticated, "missing or invalid credential")
		}
		return handler(server, &contextServerStream{ServerStream: stream, ctx: ctx})
	}
}

func authorizationFromMetadata(ctx context.Context) string {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextServerStream) Context() context.Context { return s.ctx }
