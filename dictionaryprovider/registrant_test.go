package dictionaryprovider

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/distlock"
	dictionaryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/dictionary/v1"
)

func TestRegistrant_StartAndStopOwnsRegistrationLease(t *testing.T) {
	registry := newFakeRegistry()
	runner, err := New(validConfig(), registry, nil, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err = runner.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitSignal(t, registry.registered)
	waitFor(t, runner.Active)
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = runner.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	waitSignal(t, registry.unregistered)
	if runner.Active() {
		t.Fatal("Active() = true after Stop()")
	}
	if registry.lastUnregister.GetProviderId() != "provider-1" || registry.lastUnregister.GetLeaseToken() != "lease-token" {
		t.Fatalf("unregister request = %+v", registry.lastUnregister)
	}
}

func TestRegistrant_LostLeadershipEndsProviderLease(t *testing.T) {
	registry := newFakeRegistry()
	mutex := &fakeMutex{extendErr: errors.New("ownership lost")}
	locker := &fakeLocker{mutex: mutex}
	config := validConfig()
	config.CallTimeout = 10 * time.Millisecond
	config.LeaderTTL = 30 * time.Millisecond
	config.RetryDelay = time.Hour
	runner, err := New(config, registry, locker, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err = runner.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitSignal(t, registry.registered)
	waitSignal(t, registry.unregistered)
	waitFor(t, func() bool { return !runner.Active() })
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = runner.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if mutex.extendCalls.Load() == 0 {
		t.Fatal("leadership lock was never extended")
	}
}

func TestNew_ValidatesProviderConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{name: "service", change: func(c *Config) { c.ServiceName = "" }},
		{name: "capability", change: func(c *Config) { c.Capabilities = nil }},
		{name: "lease", change: func(c *Config) { c.LeaseDuration = time.Second }},
		{name: "cache", change: func(c *Config) { c.CacheTTL = 2 * time.Hour }},
		{name: "provider timeout", change: func(c *Config) { c.ProviderTimeout = time.Minute }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.change(&config)
			if _, err := New(config, newFakeRegistry(), nil, discardLogger()); err == nil {
				t.Fatal("New() error = nil")
			}
		})
	}
}

func validConfig() Config {
	return Config{
		ServiceName:     "tenant-service",
		Target:          "tenant-service.platform.svc.cluster.local:9090",
		Capabilities:    []*dictionaryv1.ProviderCapability{{DictionaryCode: "tenant.departments", SupportsSearch: true}},
		CacheTTL:        time.Minute,
		CallTimeout:     time.Second,
		ProviderTimeout: 3 * time.Second,
		LeaseDuration:   60 * time.Second,
		LeaderTTL:       5 * time.Second,
	}
}

type fakeRegistry struct {
	registered     chan struct{}
	unregistered   chan struct{}
	registerOnce   sync.Once
	unregisterOnce sync.Once
	lastUnregister *dictionaryv1.UnregisterProviderRequest
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{registered: make(chan struct{}), unregistered: make(chan struct{})}
}

func (f *fakeRegistry) Register(context.Context, *dictionaryv1.RegisterProviderRequest) (*dictionaryv1.RegisterProviderResponse, error) {
	f.registerOnce.Do(func() { close(f.registered) })
	return &dictionaryv1.RegisterProviderResponse{Provider: &dictionaryv1.Provider{Id: "provider-1"}, LeaseToken: "lease-token"}, nil
}

func (*fakeRegistry) Heartbeat(context.Context, *dictionaryv1.HeartbeatProviderRequest) (*dictionaryv1.HeartbeatProviderResponse, error) {
	return &dictionaryv1.HeartbeatProviderResponse{}, nil
}

func (f *fakeRegistry) Unregister(_ context.Context, request *dictionaryv1.UnregisterProviderRequest) error {
	f.lastUnregister = request
	f.unregisterOnce.Do(func() { close(f.unregistered) })
	return nil
}

type fakeLocker struct{ mutex distlock.Mutex }

func (l *fakeLocker) TryLock(context.Context, string, time.Duration) (distlock.Mutex, bool, error) {
	return l.mutex, true, nil
}
func (*fakeLocker) Lock(context.Context, string, time.Duration, time.Duration) (distlock.Mutex, error) {
	return nil, errors.New("unexpected Lock call")
}

type fakeMutex struct {
	extendErr   error
	extendCalls atomic.Int32
}

func (m *fakeMutex) Extend(context.Context) error { m.extendCalls.Add(1); return m.extendErr }
func (*fakeMutex) Unlock(context.Context) error   { return nil }
func (*fakeMutex) Until() time.Time               { return time.Now().Add(time.Minute) }

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lifecycle signal")
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
