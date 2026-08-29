package dynamicconfig

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	configv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/config/v1"
	"google.golang.org/grpc"
)

var ErrNotFound = errors.New("dynamic configuration key not found")

type Resolver interface {
	Resolve(context.Context, *configv1.ResolveRequest, ...grpc.CallOption) (*configv1.ResolveResponse, error)
}

type Options struct {
	Environment string
	TenantID    string
	Service     string
	SubjectID   string
	TTL         time.Duration
}

type Value struct {
	Bytes     []byte
	SecretRef string
	Revision  int64
}

type snapshot struct {
	values    map[string]Value
	etag      string
	expiresAt time.Time
}

type Client struct {
	resolver Resolver
	options  Options
	now      func() time.Time
	value    atomic.Pointer[snapshot]
	refresh  sync.Mutex
}

func New(resolver Resolver, options Options) (*Client, error) {
	if resolver == nil || options.Environment == "" || options.Service == "" {
		return nil, errors.New("resolver, environment, and service are required")
	}
	if options.TTL <= 0 {
		options.TTL = time.Minute
	}
	return &Client{resolver: resolver, options: options, now: time.Now}, nil
}

func (c *Client) Get(ctx context.Context, key string) (Value, error) {
	current := c.value.Load()
	if current == nil || !c.now().Before(current.expiresAt) {
		if err := c.Refresh(ctx, nil); err != nil && current == nil {
			return Value{}, err
		}
		current = c.value.Load()
	}
	value, ok := current.values[key]
	if !ok {
		return Value{}, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	value.Bytes = append([]byte(nil), value.Bytes...)
	return value, nil
}

func (c *Client) Refresh(ctx context.Context, keys []string) error {
	c.refresh.Lock()
	defer c.refresh.Unlock()
	response, err := c.resolver.Resolve(ctx, &configv1.ResolveRequest{Environment: c.options.Environment, TenantId: c.options.TenantID, Service: c.options.Service, Keys: keys, SubjectId: c.options.SubjectID})
	if err != nil {
		return fmt.Errorf("resolve dynamic configuration: %w", err)
	}
	values := make(map[string]Value, len(response.GetEntries()))
	for _, entry := range response.GetEntries() {
		values[entry.GetKey()] = Value{Bytes: append([]byte(nil), entry.GetValue()...), SecretRef: entry.GetSecretRef(), Revision: entry.GetRevision()}
	}
	c.value.Store(&snapshot{values: values, etag: response.GetEtag(), expiresAt: c.now().Add(c.options.TTL)})
	return nil
}

func (c *Client) Invalidate() { c.value.Store(nil) }

func (c *Client) HandleChanged(_ context.Context, envelope *commonv1.EventEnvelope) error {
	event := new(configv1.ConfigChangedEvent)
	if err := eventbus.DecodePayload(envelope, event); err != nil {
		return fmt.Errorf("decode config changed event: %w", err)
	}
	entry := event.GetEntry()
	if entry.GetEnvironment() == c.options.Environment && entry.GetService() == c.options.Service && (entry.GetTenantId() == "" || entry.GetTenantId() == c.options.TenantID) {
		c.Invalidate()
	}
	return nil
}

func (c *Client) ETag() string {
	if current := c.value.Load(); current != nil {
		return current.etag
	}
	return ""
}
