// Package serviceregistry provides resilient registration and cached service
// discovery on top of platform.registry.v1.
package serviceregistry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RegistrationClient interface {
	RegisterInstance(context.Context, *registryv1.RegisterInstanceRequest, ...grpc.CallOption) (*registryv1.RegisterInstanceResponse, error)
	RenewLease(context.Context, *registryv1.RenewLeaseRequest, ...grpc.CallOption) (*registryv1.RenewLeaseResponse, error)
	DeregisterInstance(context.Context, *registryv1.DeregisterInstanceRequest, ...grpc.CallOption) (*registryv1.DeregisterInstanceResponse, error)
	SetInstanceStatus(context.Context, *registryv1.SetInstanceStatusRequest, ...grpc.CallOption) (*registryv1.SetInstanceStatusResponse, error)
}

type RegistrantConfig struct {
	Instance          *registryv1.ServiceInstance
	Lease             time.Duration
	HeartbeatInterval time.Duration
	CallTimeout       time.Duration
	RetryMin          time.Duration
	RetryMax          time.Duration
}

type Registrant struct {
	client RegistrationClient
	config RegistrantConfig
	mu     sync.RWMutex
	token  string
}

func NewRegistrant(client RegistrationClient, config RegistrantConfig) (*Registrant, error) {
	if client == nil || config.Instance == nil {
		return nil, errors.New("registry client and instance are required")
	}
	if config.Lease == 0 {
		config.Lease = 30 * time.Second
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = config.Lease / 3
	}
	if config.HeartbeatInterval >= config.Lease {
		return nil, errors.New("heartbeat interval must be shorter than lease")
	}
	if config.CallTimeout == 0 {
		config.CallTimeout = 3 * time.Second
	}
	if config.RetryMin == 0 {
		config.RetryMin = 250 * time.Millisecond
	}
	if config.RetryMax == 0 {
		config.RetryMax = 5 * time.Second
	}
	return &Registrant{client: client, config: config}, nil
}

func (r *Registrant) Run(ctx context.Context) error {
	backoff := r.config.RetryMin
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.currentToken() == "" {
			if err := r.register(ctx); err != nil {
				if !waitJitter(ctx, backoff) {
					return ctx.Err()
				}
				backoff = min(backoff*2, r.config.RetryMax)
				continue
			}
			backoff = r.config.RetryMin
		}
		if !waitJitter(ctx, r.config.HeartbeatInterval) {
			return r.shutdown()
		}
		if err := r.renew(ctx); err != nil {
			if status.Code(err) == codes.NotFound || status.Code(err) == codes.PermissionDenied {
				r.setToken("")
			}
			if !waitJitter(ctx, backoff) {
				return r.shutdown()
			}
			backoff = min(backoff*2, r.config.RetryMax)
			continue
		}
		backoff = r.config.RetryMin
	}
}

func (r *Registrant) register(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, r.config.CallTimeout)
	defer cancel()
	response, err := r.client.RegisterInstance(callCtx, &registryv1.RegisterInstanceRequest{Instance: r.config.Instance, LeaseSeconds: uint32(r.config.Lease / time.Second)})
	if err != nil {
		return fmt.Errorf("register service instance: %w", err)
	}
	r.setToken(response.GetLeaseToken())
	return nil
}
func (r *Registrant) renew(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, r.config.CallTimeout)
	defer cancel()
	_, err := r.client.RenewLease(callCtx, &registryv1.RenewLeaseRequest{ServiceName: r.config.Instance.ServiceName, InstanceId: r.config.Instance.InstanceId, LeaseToken: r.currentToken(), LeaseSeconds: uint32(r.config.Lease / time.Second)})
	if err != nil {
		return fmt.Errorf("renew service instance lease: %w", err)
	}
	return nil
}
func (r *Registrant) shutdown() error {
	token := r.currentToken()
	if token == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.config.CallTimeout)
	defer cancel()
	_, _ = r.client.SetInstanceStatus(ctx, &registryv1.SetInstanceStatusRequest{ServiceName: r.config.Instance.ServiceName, InstanceId: r.config.Instance.InstanceId, LeaseToken: token, Status: registryv1.InstanceStatus_INSTANCE_STATUS_DRAINING})
	_, err := r.client.DeregisterInstance(ctx, &registryv1.DeregisterInstanceRequest{ServiceName: r.config.Instance.ServiceName, InstanceId: r.config.Instance.InstanceId, LeaseToken: token})
	return err
}
func (r *Registrant) currentToken() string  { r.mu.RLock(); defer r.mu.RUnlock(); return r.token }
func (r *Registrant) setToken(value string) { r.mu.Lock(); r.token = value; r.mu.Unlock() }
func waitJitter(ctx context.Context, duration time.Duration) bool {
	delta := duration / 5
	if delta > 0 {
		duration += time.Duration(rand.Int64N(int64(delta*2))) - delta
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
