package idempotency

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/principal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeGRPCManager struct {
	decision    Decision
	fingerprint string
	completed   *cachedGRPCResponse
	failed      *Failure
}

func (*fakeGRPCManager) Enabled() bool { return true }
func (m *fakeGRPCManager) Begin(_ context.Context, _, fingerprint string) (Decision, error) {
	m.fingerprint = fingerprint
	return m.decision, nil
}
func (m *fakeGRPCManager) Complete(_ context.Context, _, _ string, response any) error {
	value, ok := response.(cachedGRPCResponse)
	if ok {
		m.completed = &value
	}
	return nil
}
func (m *fakeGRPCManager) Fail(_ context.Context, _, _ string, failure Failure) error {
	m.failed = &failure
	return nil
}

func grpcTestContext(callerID string) context.Context {
	ctx := principal.WithContext(context.Background(), principal.Principal{ID: callerID, Type: principal.TypeUser})
	return WithContext(ctx, "operation-1")
}

func TestUnaryServerInterceptorCompletesAndReplays(t *testing.T) {
	t.Parallel()
	const method = "/grpc.health.v1.Health/Check"
	manager := &fakeGRPCManager{decision: Decision{State: StateAcquired, Owner: "owner-1"}}
	interceptor := UnaryServerInterceptor(manager, []string{"/grpc.health.v1.Health/*"}, nil)
	request := &grpc_health_v1.HealthCheckRequest{Service: "api"}
	expected := &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}
	response, err := interceptor(grpcTestContext("user-1"), request, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return expected, nil })
	if err != nil || response != expected || manager.fingerprint == "" || manager.completed == nil {
		t.Fatalf("response=%v error=%v fingerprint=%q completed=%+v", response, err, manager.fingerprint, manager.completed)
	}
	encoded, err := json.Marshal(*manager.completed)
	if err != nil {
		t.Fatal(err)
	}
	manager.decision = Decision{State: StateCompleted, Response: encoded}
	calls := 0
	replayed, err := interceptor(grpcTestContext("user-1"), request, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { calls++; return nil, nil })
	if err != nil || calls != 0 || !proto.Equal(replayed.(proto.Message), expected) {
		t.Fatalf("replayed=%v error=%v calls=%d", replayed, err, calls)
	}
}

func TestUnaryServerInterceptorPersistsFailure(t *testing.T) {
	t.Parallel()
	manager := &fakeGRPCManager{decision: Decision{State: StateAcquired, Owner: "owner-1"}}
	interceptor := UnaryServerInterceptor(manager, []string{"/grpc.health.v1.Health/Check"}, nil)
	_, err := interceptor(grpcTestContext("user-1"), &grpc_health_v1.HealthCheckRequest{}, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	})
	if status.Code(err) != codes.PermissionDenied || manager.failed == nil || manager.failed.GRPCCode != int(codes.PermissionDenied) || manager.failed.Message != "denied" {
		t.Fatalf("error=%v failed=%+v", err, manager.failed)
	}
}

func TestUnaryServerInterceptorRejectsExistingStates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state State
		code  codes.Code
	}{
		{state: StateProcessing, code: codes.Aborted},
		{state: StateConflict, code: codes.AlreadyExists},
		{state: StateFailed, code: codes.FailedPrecondition},
	}
	for _, test := range tests {
		manager := &fakeGRPCManager{decision: Decision{State: test.state, Failure: Failure{Message: "failed", GRPCCode: int(codes.FailedPrecondition)}}}
		interceptor := UnaryServerInterceptor(manager, []string{"/grpc.health.v1.Health/Check"}, nil)
		_, err := interceptor(grpcTestContext("user-1"), &grpc_health_v1.HealthCheckRequest{}, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, func(context.Context, any) (any, error) {
			t.Fatal("handler must not execute")
			return nil, nil
		})
		if status.Code(err) != test.code {
			t.Fatalf("state=%s code=%s", test.state, status.Code(err))
		}
	}
}

func TestGRPCFingerprintIncludesPrincipalAndBypassesUnconfiguredMethod(t *testing.T) {
	t.Parallel()
	request := &grpc_health_v1.HealthCheckRequest{Service: "api"}
	first, err := grpcFingerprint(grpcTestContext("user-1"), "/grpc.health.v1.Health/Check", request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := grpcFingerprint(grpcTestContext("user-2"), "/grpc.health.v1.Health/Check", request)
	if err != nil || first == second {
		t.Fatalf("first=%q second=%q error=%v", first, second, err)
	}
	manager := &fakeGRPCManager{decision: Decision{State: StateConflict}}
	calls := 0
	response, err := UnaryServerInterceptor(manager, []string{"/other.Service/Create"}, nil)(grpcTestContext("user-1"), request, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, func(context.Context, any) (any, error) {
		calls++
		return &grpc_health_v1.HealthCheckResponse{}, nil
	})
	if err != nil || calls != 1 || response == nil || manager.fingerprint != "" {
		t.Fatalf("response=%v error=%v calls=%d fingerprint=%q", response, err, calls, manager.fingerprint)
	}
}
