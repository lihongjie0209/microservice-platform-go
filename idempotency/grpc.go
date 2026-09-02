package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type ManagerAPI interface {
	Enabled() bool
	Begin(context.Context, string, string) (Decision, error)
	Complete(context.Context, string, string, any) error
	Fail(context.Context, string, string, Failure) error
}

type cachedGRPCResponse struct {
	Payload []byte `json:"payload"`
}

func UnaryServerInterceptor(manager ManagerAPI, methods []string, logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		key, ok := FromContext(ctx)
		if !ok || manager == nil || !manager.Enabled() || !matchesAny(info.FullMethod, methods) {
			return handler(ctx, request)
		}
		fingerprint, err := grpcFingerprint(ctx, info.FullMethod, request)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid idempotent request")
		}
		decision, err := manager.Begin(ctx, key, fingerprint)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "idempotency is unavailable")
		}
		switch decision.State {
		case StateCompleted:
			return replayGRPCResponse(info.FullMethod, decision.Response)
		case StateFailed:
			return nil, status.Error(codes.Code(decision.Failure.GRPCCode), decision.Failure.Message)
		case StateProcessing:
			return nil, status.Error(codes.Aborted, "request is already processing")
		case StateConflict:
			return nil, status.Error(codes.AlreadyExists, "idempotency key belongs to a different request")
		case StateAcquired:
		default:
			return nil, status.Error(codes.Unavailable, "idempotency state is invalid")
		}

		response, handlerErr := handler(ctx, request)
		persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if handlerErr != nil {
			failureStatus := status.Convert(handlerErr)
			err = manager.Fail(persistCtx, key, decision.Owner, Failure{Message: failureStatus.Message(), GRPCCode: int(failureStatus.Code())})
		} else {
			message, messageOK := response.(proto.Message)
			if !messageOK {
				return nil, status.Error(codes.Internal, "idempotent response is not protobuf")
			}
			payload, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(message)
			if marshalErr != nil {
				return nil, status.Error(codes.Internal, "encode idempotent response")
			}
			err = manager.Complete(persistCtx, key, decision.Owner, cachedGRPCResponse{Payload: payload})
		}
		if err != nil && logger != nil {
			logger.ErrorContext(ctx, "persist grpc idempotency result", "error", err, "method", info.FullMethod)
		}
		return response, handlerErr
	}
}

func grpcFingerprint(ctx context.Context, method string, request any) (string, error) {
	message, ok := request.(proto.Message)
	if !ok {
		return "", errors.New("request is not protobuf")
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return "", err
	}
	caller, _ := principal.FromContext(ctx)
	hash := sha256.New()
	_, _ = hash.Write([]byte(caller.ID))
	_, _ = hash.Write([]byte("\x00" + method + "\x00"))
	_, _ = hash.Write(payload)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func replayGRPCResponse(method string, encoded json.RawMessage) (proto.Message, error) {
	var cached cachedGRPCResponse
	if err := json.Unmarshal(encoded, &cached); err != nil {
		return nil, status.Error(codes.Unavailable, "decode idempotent response")
	}
	output, err := grpcMethodOutput(method)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "resolve idempotent response")
	}
	response := dynamicpb.NewMessage(output)
	if err := proto.Unmarshal(cached.Payload, response); err != nil {
		return nil, status.Error(codes.Unavailable, "decode idempotent response payload")
	}
	return response, nil
}

func grpcMethodOutput(method string) (protoreflect.MessageDescriptor, error) {
	serviceName, methodName, ok := strings.Cut(strings.TrimPrefix(method, "/"), "/")
	if !ok {
		return nil, errors.New("invalid full method")
	}
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, err
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, errors.New("descriptor is not a service")
	}
	methodDescriptor := service.Methods().ByName(protoreflect.Name(methodName))
	if methodDescriptor == nil {
		return nil, errors.New("method descriptor not found")
	}
	return methodDescriptor.Output(), nil
}

func matchesAny(value string, patterns []string) bool {
	for _, pattern := range patterns {
		if matched, err := path.Match(pattern, value); err == nil && matched {
			return true
		}
	}
	return false
}
