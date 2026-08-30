package serviceregistry

import (
	"context"
	"sync"
	"testing"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registrationStub struct {
	mu              sync.Mutex
	registrations   int
	registeredTwice chan struct{}
}

func (s *registrationStub) RegisterInstance(context.Context, *registryv1.RegisterInstanceRequest, ...grpc.CallOption) (*registryv1.RegisterInstanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registrations++
	if s.registrations == 2 {
		close(s.registeredTwice)
	}
	return &registryv1.RegisterInstanceResponse{LeaseToken: "token"}, nil
}
func (s *registrationStub) RenewLease(context.Context, *registryv1.RenewLeaseRequest, ...grpc.CallOption) (*registryv1.RenewLeaseResponse, error) {
	return nil, status.Error(codes.NotFound, "lease expired")
}
func (s *registrationStub) DeregisterInstance(context.Context, *registryv1.DeregisterInstanceRequest, ...grpc.CallOption) (*registryv1.DeregisterInstanceResponse, error) {
	return &registryv1.DeregisterInstanceResponse{}, nil
}
func (s *registrationStub) SetInstanceStatus(context.Context, *registryv1.SetInstanceStatusRequest, ...grpc.CallOption) (*registryv1.SetInstanceStatusResponse, error) {
	return &registryv1.SetInstanceStatusResponse{}, nil
}

func TestRegistrantReregistersAfterLeaseLoss(t *testing.T) {
	client := &registrationStub{registeredTwice: make(chan struct{})}
	registrant, err := NewRegistrant(client, RegistrantConfig{Instance: &registryv1.ServiceInstance{ServiceName: "tenant-service", InstanceId: "pod-1", Endpoint: "grpc://tenant:9090"}, Lease: 100 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond, CallTimeout: time.Second, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- registrant.Run(ctx) }()
	select {
	case <-client.registeredTwice:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("registrant did not recover lost lease")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("registrant did not stop")
	}
}
