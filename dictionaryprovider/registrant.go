package dictionaryprovider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/distlock"
	dictionaryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/dictionary/v1"
)

const (
	defaultCallTimeout = 5 * time.Second
	defaultRetryDelay  = 3 * time.Second
	defaultLeaderTTL   = 30 * time.Second
)

// Config describes one logical provider. Target should normally be a stable
// Kubernetes Service DNS name, not a Pod address.
type Config struct {
	ServiceName     string
	Target          string
	Capabilities    []*dictionaryv1.ProviderCapability
	CacheTTL        time.Duration
	CallTimeout     time.Duration
	ProviderTimeout time.Duration
	LeaseDuration   time.Duration
	RetryDelay      time.Duration
	LeaderTTL       time.Duration
	LeaderLockKey   string
}

// Registrant registers one logical provider, renews its lease, and unregisters
// it during graceful shutdown. When Locker is supplied, only one replica runs
// the control-plane session at a time.
type Registrant struct {
	config   Config
	registry Registry
	locker   distlock.Locker
	logger   *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	active atomic.Bool
}

func New(config Config, registry Registry, locker distlock.Locker, logger *slog.Logger) (*Registrant, error) {
	config = defaults(config)
	if err := validate(config, registry); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Registrant{config: config, registry: registry, locker: locker, logger: logger}, nil
}

// Start begins leader election and registration. It is safe to call once.
func (r *Registrant) Start(parent context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return errors.New("dictionary provider registrant is already started")
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel, r.done = cancel, make(chan struct{})
	go func() {
		defer close(r.done)
		r.supervise(ctx)
	}()
	return nil
}

// Stop ends the session and waits for best-effort deregistration.
func (r *Registrant) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Active reports whether this replica currently owns a registered lease.
func (r *Registrant) Active() bool { return r.active.Load() }

func (r *Registrant) supervise(ctx context.Context) {
	for ctx.Err() == nil {
		if r.locker == nil {
			r.runSession(ctx, nil)
			return
		}
		mutex, acquired, err := r.locker.TryLock(ctx, r.config.LeaderLockKey, r.config.LeaderTTL)
		if err != nil {
			r.logger.Error("dictionary provider leader election failed", "error", err)
		} else if acquired {
			r.runSession(ctx, mutex)
		}
		if !wait(ctx, r.config.RetryDelay) {
			return
		}
	}
}

func (r *Registrant) runSession(parent context.Context, mutex distlock.Mutex) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if mutex != nil {
		defer func() { _ = mutex.Unlock(context.Background()) }()
		go r.extendLeadership(ctx, cancel, mutex)
	}

	providerID, token, err := r.register(ctx)
	if err != nil {
		r.logger.Error("register dynamic dictionary provider", "service", r.config.ServiceName, "error", err)
		return
	}
	r.active.Store(true)
	defer r.active.Store(false)
	defer r.unregister(providerID, token)
	r.logger.Info("dynamic dictionary provider registered", "service", r.config.ServiceName, "provider_id", providerID)

	interval := r.config.LeaseDuration / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.heartbeat(ctx, providerID, token); err != nil {
				r.logger.Error("renew dynamic dictionary provider lease", "service", r.config.ServiceName, "error", err)
			}
		}
	}
}

func (r *Registrant) extendLeadership(ctx context.Context, cancel context.CancelFunc, mutex distlock.Mutex) {
	ticker := time.NewTicker(r.config.LeaderTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			extendCtx, stop := context.WithTimeout(ctx, r.config.CallTimeout)
			err := mutex.Extend(extendCtx)
			stop()
			if err != nil {
				r.logger.Error("extend dictionary provider leadership", "service", r.config.ServiceName, "error", err)
				cancel()
				return
			}
		}
	}
}

func (r *Registrant) register(ctx context.Context) (string, string, error) {
	callCtx, cancel := context.WithTimeout(ctx, r.config.CallTimeout)
	defer cancel()
	response, err := r.registry.Register(callCtx, &dictionaryv1.RegisterProviderRequest{
		ServiceName:         r.config.ServiceName,
		Target:              r.config.Target,
		Capabilities:        r.config.Capabilities,
		CacheTtlSeconds:     durationSeconds(r.config.CacheTTL),
		TimeoutMilliseconds: uint32(r.config.ProviderTimeout.Milliseconds()),
		LeaseSeconds:        durationSeconds(r.config.LeaseDuration),
	})
	if err != nil {
		return "", "", err
	}
	if response.GetProvider().GetId() == "" || response.GetLeaseToken() == "" {
		return "", "", errors.New("dictionary registry returned an incomplete lease")
	}
	return response.GetProvider().GetId(), response.GetLeaseToken(), nil
}

func (r *Registrant) heartbeat(ctx context.Context, providerID, token string) error {
	callCtx, cancel := context.WithTimeout(ctx, r.config.CallTimeout)
	defer cancel()
	_, err := r.registry.Heartbeat(callCtx, &dictionaryv1.HeartbeatProviderRequest{ProviderId: providerID, LeaseToken: token, LeaseSeconds: durationSeconds(r.config.LeaseDuration)})
	return err
}

func (r *Registrant) unregister(providerID, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.CallTimeout)
	defer cancel()
	if err := r.registry.Unregister(ctx, &dictionaryv1.UnregisterProviderRequest{ProviderId: providerID, LeaseToken: token}); err != nil {
		r.logger.Warn("unregister dynamic dictionary provider", "service", r.config.ServiceName, "error", err)
	}
}

func defaults(config Config) Config {
	if config.CallTimeout <= 0 {
		config.CallTimeout = defaultCallTimeout
	}
	if config.ProviderTimeout <= 0 {
		config.ProviderTimeout = 3 * time.Second
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 60 * time.Second
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = defaultRetryDelay
	}
	if config.LeaderTTL <= 0 {
		config.LeaderTTL = defaultLeaderTTL
	}
	if strings.TrimSpace(config.LeaderLockKey) == "" {
		config.LeaderLockKey = "dictionary-provider:" + strings.TrimSpace(config.ServiceName)
	}
	return config
}

func validate(config Config, registry Registry) error {
	if registry == nil {
		return errors.New("dictionary registry is required")
	}
	if strings.TrimSpace(config.ServiceName) == "" || strings.TrimSpace(config.Target) == "" {
		return errors.New("provider service name and target are required")
	}
	if len(config.Capabilities) == 0 {
		return errors.New("at least one provider capability is required")
	}
	if config.LeaseDuration < 15*time.Second || config.LeaseDuration > 300*time.Second {
		return errors.New("provider lease must be between 15 and 300 seconds")
	}
	if config.LeaderTTL < 3*config.CallTimeout {
		return fmt.Errorf("leader TTL must be at least three call timeouts")
	}
	if config.CacheTTL < 0 || config.CacheTTL > time.Hour {
		return errors.New("provider cache TTL must be between zero and one hour")
	}
	if config.ProviderTimeout < 100*time.Millisecond || config.ProviderTimeout > 30*time.Second {
		return errors.New("provider timeout must be between 100 milliseconds and 30 seconds")
	}
	return nil
}

func durationSeconds(value time.Duration) uint32 { return uint32(value / time.Second) }

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
