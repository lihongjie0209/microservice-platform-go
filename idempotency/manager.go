package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrOwnershipExpired = errors.New("idempotency ownership expired")
	ErrResponseTooLarge = errors.New("idempotency response is too large")
)

const defaultMaxResponseBytes = 1 << 20

type State string

const (
	StateAcquired   State = "acquired"
	StateProcessing State = "processing"
	StateCompleted  State = "completed"
	StateFailed     State = "failed"
	StateConflict   State = "conflict"
)

type Config struct {
	Enabled          bool
	Service          string
	ProcessingTTL    time.Duration
	ResultTTL        time.Duration
	FailureTTL       time.Duration
	MaxResponseBytes int
}

type Failure struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status"`
	GRPCCode   int    `json:"grpc_code,omitempty"`
}

type Decision struct {
	State    State
	Response json.RawMessage
	Failure  Failure
	Owner    string
}

type Manager struct {
	client *redis.Client
	cfg    Config
}

func New(client *redis.Client, cfg Config) *Manager {
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = defaultMaxResponseBytes
	}
	return &Manager{client: client, cfg: cfg}
}

func (m *Manager) Enabled() bool { return m != nil && m.cfg.Enabled }

func (m *Manager) storageKey(key string) string {
	return "idempotency:" + m.cfg.Service + ":" + key
}

var beginScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], 'state', 'processing', 'fingerprint', ARGV[1], 'owner', ARGV[3])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {'acquired', '', ''}
end
local fingerprint = redis.call('HGET', KEYS[1], 'fingerprint')
if fingerprint ~= ARGV[1] then return {'conflict', '', ''} end
return {redis.call('HGET', KEYS[1], 'state') or 'processing', redis.call('HGET', KEYS[1], 'response') or '', redis.call('HGET', KEYS[1], 'failure') or ''}
`)

func (m *Manager) Begin(ctx context.Context, key, fingerprint string) (Decision, error) {
	if !m.Enabled() || m.client == nil {
		return Decision{}, errors.New("idempotency is unavailable")
	}
	owner := uuid.NewString()
	value, err := beginScript.Run(ctx, m.client, []string{m.storageKey(key)}, fingerprint, m.cfg.ProcessingTTL.Milliseconds(), owner).Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("begin idempotency request: %w", err)
	}
	if len(value) != 3 {
		return Decision{}, errors.New("invalid idempotency state response")
	}
	decision := Decision{State: State(fmt.Sprint(value[0]))}
	if decision.State == StateAcquired {
		decision.Owner = owner
	}
	if response := fmt.Sprint(value[1]); response != "" {
		decision.Response = json.RawMessage(response)
	}
	if failure := fmt.Sprint(value[2]); failure != "" {
		if err := json.Unmarshal([]byte(failure), &decision.Failure); err != nil {
			return Decision{}, fmt.Errorf("decode idempotency failure: %w", err)
		}
	}
	return decision, nil
}

var renewScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' or redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

// Renew extends a processing record only while owner still holds it.
func (m *Manager) Renew(ctx context.Context, key, owner string) error {
	if !m.Enabled() || m.client == nil || key == "" || owner == "" || m.cfg.ProcessingTTL <= 0 {
		return errors.New("idempotency is unavailable")
	}
	changed, err := renewScript.Run(ctx, m.client, []string{m.storageKey(key)}, owner, m.cfg.ProcessingTTL.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("renew idempotency request: %w", err)
	}
	if changed != 1 {
		return ErrOwnershipExpired
	}
	return nil
}

// StartLease keeps a processing record alive and cancels the returned context
// if Redis becomes unavailable or ownership is lost. The returned stop function
// must be called exactly once after protected work finishes; it waits for the
// renewal goroutine to exit and returns any renewal error.
func (m *Manager) StartLease(ctx context.Context, key, owner string) (context.Context, func() error, error) {
	if !m.Enabled() || m.client == nil || key == "" || owner == "" || m.cfg.ProcessingTTL <= 0 {
		return nil, nil, errors.New("idempotency is unavailable")
	}
	interval := m.cfg.ProcessingTTL / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	leaseCtx, cancel := context.WithCancelCause(ctx)
	stopCh := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				done <- nil
				return
			case <-leaseCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := m.Renew(leaseCtx, key, owner); err != nil {
					cancel(err)
					done <- err
					return
				}
			}
		}
	}()
	var once sync.Once
	var leaseErr error
	stop := func() error {
		once.Do(func() {
			close(stopCh)
			leaseErr = <-done
			cancel(nil)
		})
		return leaseErr
	}
	return leaseCtx, stop, nil
}

var finishScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' or redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'state', ARGV[2], ARGV[3], ARGV[4])
redis.call('HDEL', KEYS[1], 'owner')
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1
`)

func (m *Manager) Complete(ctx context.Context, key, owner string, response any) error {
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("encode idempotency response: %w", err)
	}
	if len(encoded) > m.cfg.MaxResponseBytes {
		abortErr := m.Abort(ctx, key, owner)
		return errors.Join(ErrResponseTooLarge, abortErr)
	}
	changed, err := finishScript.Run(ctx, m.client, []string{m.storageKey(key)}, owner, string(StateCompleted), "response", encoded, m.cfg.ResultTTL.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("complete idempotency request: %w", err)
	}
	if changed != 1 {
		return ErrOwnershipExpired
	}
	return nil
}

var abortScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' or redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
`)

// Abort releases a processing record after a retryable failure. It never
// deletes a completed record or work owned by another request.
func (m *Manager) Abort(ctx context.Context, key, owner string) error {
	if !m.Enabled() || m.client == nil || key == "" || owner == "" {
		return errors.New("idempotency is unavailable")
	}
	changed, err := abortScript.Run(ctx, m.client, []string{m.storageKey(key)}, owner).Int()
	if err != nil {
		return fmt.Errorf("abort idempotency request: %w", err)
	}
	if changed != 1 {
		return ErrOwnershipExpired
	}
	return nil
}

func (m *Manager) Fail(ctx context.Context, key, owner string, failure Failure) error {
	encoded, err := json.Marshal(failure)
	if err != nil {
		return fmt.Errorf("encode idempotency failure: %w", err)
	}
	changed, err := finishScript.Run(ctx, m.client, []string{m.storageKey(key)}, owner, string(StateFailed), "failure", encoded, m.cfg.FailureTTL.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("fail idempotency request: %w", err)
	}
	if changed != 1 {
		return ErrOwnershipExpired
	}
	return nil
}
