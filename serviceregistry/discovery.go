package serviceregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"google.golang.org/grpc"
)

var ErrNoHealthyInstance = errors.New("no healthy service instance")

type DiscoveryClient interface {
	ListInstances(context.Context, *registryv1.ListInstancesRequest, ...grpc.CallOption) (*registryv1.ListInstancesResponse, error)
	WatchService(context.Context, *registryv1.WatchServiceRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[registryv1.WatchServiceResponse], error)
}
type SnapshotStore interface {
	Load(context.Context, string) (*Snapshot, error)
	Save(context.Context, string, *Snapshot) error
}
type Snapshot struct {
	Revision  uint64                        `json:"revision"`
	SavedAt   time.Time                     `json:"saved_at"`
	Instances []*registryv1.ServiceInstance `json:"instances"`
}
type DiscoveryConfig struct {
	ServiceName   string
	Selector      map[string]string
	MaxStale      time.Duration
	RetryMin      time.Duration
	RetryMax      time.Duration
	SnapshotStore SnapshotStore
}

type Discovery struct {
	client   DiscoveryClient
	config   DiscoveryConfig
	now      func() time.Time
	mu       sync.RWMutex
	snapshot *Snapshot
	ejected  map[string]time.Time
	cursor   atomic.Uint64
}

func NewDiscovery(client DiscoveryClient, config DiscoveryConfig) (*Discovery, error) {
	if client == nil || (config.ServiceName == "" && len(config.Selector) == 0) {
		return nil, errors.New("registry client and service name or selector are required")
	}
	if config.MaxStale == 0 {
		config.MaxStale = 2 * time.Minute
	}
	if config.RetryMin == 0 {
		config.RetryMin = 250 * time.Millisecond
	}
	if config.RetryMax == 0 {
		config.RetryMax = 10 * time.Second
	}
	return &Discovery{client: client, config: config, now: time.Now, ejected: make(map[string]time.Time)}, nil
}
func (d *Discovery) Run(ctx context.Context) error {
	_ = d.restore(ctx)
	backoff := d.config.RetryMin
	for ctx.Err() == nil {
		if err := d.refresh(ctx); err != nil {
			if !waitJitter(ctx, backoff) {
				break
			}
			backoff = min(backoff*2, d.config.RetryMax)
			continue
		}
		backoff = d.config.RetryMin
		if err := d.watch(ctx); err != nil && ctx.Err() == nil {
			if !waitJitter(ctx, backoff) {
				break
			}
			backoff = min(backoff*2, d.config.RetryMax)
		}
	}
	return ctx.Err()
}
func (d *Discovery) refresh(ctx context.Context) error {
	response, err := d.client.ListInstances(ctx, &registryv1.ListInstancesRequest{ServiceName: d.config.ServiceName, Selector: &registryv1.MetadataSelector{Match: d.config.Selector}})
	if err != nil {
		return err
	}
	d.replace(&Snapshot{Revision: response.Revision, SavedAt: d.now(), Instances: response.Instances})
	return nil
}
func (d *Discovery) watch(ctx context.Context) error {
	d.mu.RLock()
	revision := d.snapshot.Revision
	d.mu.RUnlock()
	stream, err := d.client.WatchService(ctx, &registryv1.WatchServiceRequest{ServiceName: d.config.ServiceName, Selector: &registryv1.MetadataSelector{Match: d.config.Selector}, AfterRevision: revision})
	if err != nil {
		return err
	}
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		d.apply(response)
	}
}
func (d *Discovery) apply(response *registryv1.WatchServiceResponse) {
	d.mu.Lock()
	defer d.mu.Unlock()
	values := make(map[string]*registryv1.ServiceInstance)
	if d.snapshot != nil {
		for _, value := range d.snapshot.Instances {
			values[value.InstanceId] = value
		}
	}
	for _, change := range response.Changes {
		if change.Type == registryv1.InstanceChangeType_INSTANCE_CHANGE_TYPE_DELETE {
			delete(values, change.InstanceId)
		} else if change.Instance != nil {
			values[change.InstanceId] = change.Instance
		}
	}
	instances := make([]*registryv1.ServiceInstance, 0, len(values))
	for _, value := range values {
		instances = append(instances, value)
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].InstanceId < instances[j].InstanceId })
	d.snapshot = &Snapshot{Revision: response.Revision, SavedAt: d.now(), Instances: instances}
	d.persistLocked()
}
func (d *Discovery) replace(snapshot *Snapshot) {
	d.mu.Lock()
	d.snapshot = snapshot
	d.persistLocked()
	d.mu.Unlock()
}
func (d *Discovery) Pick() (*registryv1.ServiceInstance, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.snapshot == nil || d.now().Sub(d.snapshot.SavedAt) > d.config.MaxStale {
		return nil, ErrNoHealthyInstance
	}
	available := make([]*registryv1.ServiceInstance, 0, len(d.snapshot.Instances))
	now := d.now()
	for _, value := range d.snapshot.Instances {
		if value.Status == registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY && value.GetLeaseExpiresAt().AsTime().After(now) && !d.ejected[value.InstanceId].After(now) {
			weight := value.Weight
			if weight == 0 {
				weight = 1
			}
			for range weight {
				available = append(available, value)
			}
		}
	}
	if len(available) == 0 {
		return nil, ErrNoHealthyInstance
	}
	index := d.cursor.Add(1) - 1
	return available[index%uint64(len(available))], nil
}
func (d *Discovery) Instances() ([]*registryv1.ServiceInstance, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.snapshot == nil || d.now().Sub(d.snapshot.SavedAt) > d.config.MaxStale {
		return nil, ErrNoHealthyInstance
	}
	now := d.now()
	result := make([]*registryv1.ServiceInstance, 0, len(d.snapshot.Instances))
	for _, value := range d.snapshot.Instances {
		if value.Status == registryv1.InstanceStatus_INSTANCE_STATUS_HEALTHY && value.GetLeaseExpiresAt().AsTime().After(now) && !d.ejected[value.InstanceId].After(now) {
			result = append(result, value)
		}
	}
	if len(result) == 0 {
		return nil, ErrNoHealthyInstance
	}
	return result, nil
}
func (d *Discovery) ReportFailure(instanceID string, cooldown time.Duration) {
	d.mu.Lock()
	d.ejected[instanceID] = d.now().Add(cooldown)
	d.mu.Unlock()
}
func (d *Discovery) ReportSuccess(instanceID string) {
	d.mu.Lock()
	delete(d.ejected, instanceID)
	d.mu.Unlock()
}
func (d *Discovery) restore(ctx context.Context) error {
	if d.config.SnapshotStore == nil {
		return nil
	}
	snapshot, err := d.config.SnapshotStore.Load(ctx, d.config.ServiceName)
	if err != nil {
		return err
	}
	if d.now().Sub(snapshot.SavedAt) <= d.config.MaxStale {
		d.replace(snapshot)
	}
	return nil
}
func (d *Discovery) persistLocked() {
	if d.config.SnapshotStore != nil && d.snapshot != nil {
		_ = d.config.SnapshotStore.Save(context.Background(), d.config.ServiceName, d.snapshot)
	}
}

type FileSnapshotStore struct{ Directory string }

func (s FileSnapshotStore) Load(_ context.Context, service string) (*Snapshot, error) {
	payload, err := os.ReadFile(filepath.Join(s.Directory, service+".json"))
	if err != nil {
		return nil, err
	}
	var snapshot Snapshot
	if err = json.Unmarshal(payload, &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}
func (s FileSnapshotStore) Save(_ context.Context, service string, snapshot *Snapshot) error {
	if err := os.MkdirAll(s.Directory, 0o750); err != nil {
		return err
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	target := filepath.Join(s.Directory, service+".json")
	temporary, err := os.CreateTemp(s.Directory, ".registry-*.tmp")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(payload)
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write registry snapshot: %w", err)
	}
	return os.Rename(name, target)
}
