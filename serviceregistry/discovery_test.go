package serviceregistry

import (
	"context"
	"errors"
	"testing"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type discoveryStub struct {
	response *registryv1.ListInstancesResponse
	err      error
}

func (s discoveryStub) ListInstances(context.Context, *registryv1.ListInstancesRequest, ...grpc.CallOption) (*registryv1.ListInstancesResponse, error) {
	return s.response, s.err
}
func (s discoveryStub) WatchService(context.Context, *registryv1.WatchServiceRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[registryv1.WatchServiceResponse], error) {
	return nil, errors.New("stopped")
}

func TestDiscoveryUsesFreshCacheAndPassiveEjection(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	one := &registryv1.ServiceInstance{InstanceId: "one", Status: registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY, Weight: 1, LeaseExpiresAt: timestamppb.New(now.Add(time.Minute))}
	two := &registryv1.ServiceInstance{InstanceId: "two", Status: registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY, Weight: 1, LeaseExpiresAt: timestamppb.New(now.Add(time.Minute))}
	discovery, err := NewDiscovery(discoveryStub{response: &registryv1.ListInstancesResponse{Instances: []*registryv1.ServiceInstance{one, two}, Revision: 3}}, DiscoveryConfig{ServiceName: "dictionary-service", MaxStale: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	discovery.now = func() time.Time { return now }
	if err = discovery.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := discovery.Pick()
	if err != nil {
		t.Fatal(err)
	}
	discovery.ReportFailure(first.InstanceId, time.Minute)
	second, err := discovery.Pick()
	if err != nil {
		t.Fatal(err)
	}
	if second.InstanceId == first.InstanceId {
		t.Fatal("ejected instance selected")
	}
	now = now.Add(2 * time.Minute)
	if _, err = discovery.Pick(); !errors.Is(err, ErrNoHealthyInstance) {
		t.Fatalf("stale cache error=%v", err)
	}
}

func TestFileSnapshotStoreRoundTrip(t *testing.T) {
	store := FileSnapshotStore{Directory: t.TempDir()}
	snapshot := &Snapshot{Revision: 7, SavedAt: time.Now(), Instances: []*registryv1.ServiceInstance{{InstanceId: "pod-1"}}}
	if err := store.Save(context.Background(), "tenant-service", snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), "tenant-service")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Revision != 7 || loaded.Instances[0].InstanceId != "pod-1" {
		t.Fatalf("loaded=%+v", loaded)
	}
}

func TestDiscoveryAllowsExpiredLeaseInsideBoundedStaleWindow(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	instance := &registryv1.ServiceInstance{InstanceId: "pod-1", Status: registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY, LeaseExpiresAt: timestamppb.New(now.Add(-time.Second))}
	discovery, err := NewDiscovery(discoveryStub{response: &registryv1.ListInstancesResponse{Instances: []*registryv1.ServiceInstance{instance}}}, DiscoveryConfig{ServiceName: "orders-service", MaxStale: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	discovery.now = func() time.Time { return now }
	if err := discovery.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if selected, err := discovery.Pick(); err != nil || selected.InstanceId != "pod-1" {
		t.Fatalf("Pick() = %+v, %v", selected, err)
	}
}

func TestDiscoveryRequiresRefreshBeforeMaxStale(t *testing.T) {
	_, err := NewDiscovery(discoveryStub{}, DiscoveryConfig{ServiceName: "orders-service", MaxStale: time.Minute, RefreshInterval: time.Minute})
	if err == nil {
		t.Fatal("refresh interval equal to max stale was accepted")
	}
	discovery, err := NewDiscovery(discoveryStub{}, DiscoveryConfig{ServiceName: "orders-service", MaxStale: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if discovery.config.RefreshInterval != 30*time.Second {
		t.Fatalf("default refresh interval = %s", discovery.config.RefreshInterval)
	}
}
