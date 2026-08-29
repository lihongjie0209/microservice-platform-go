package dynamicconfig

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	configv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/config/v1"
	"google.golang.org/grpc"
)

type fakeResolver struct{ calls int }

func (f *fakeResolver) Resolve(context.Context, *configv1.ResolveRequest, ...grpc.CallOption) (*configv1.ResolveResponse, error) {
	f.calls++
	return &configv1.ResolveResponse{Etag: "v1", Entries: []*configv1.ConfigEntry{{Key: "feature.enabled", Value: []byte("true"), Revision: 2}}}, nil
}

func TestClientCachesCopiesAndInvalidates(t *testing.T) {
	resolver := &fakeResolver{}
	client, err := New(resolver, Options{Environment: "testing", Service: "orders", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Get(t.Context(), "feature.enabled")
	if err != nil {
		t.Fatal(err)
	}
	first.Bytes[0] = 'X'
	second, err := client.Get(t.Context(), "feature.enabled")
	if err != nil || string(second.Bytes) != "true" || resolver.calls != 1 || client.ETag() != "v1" {
		t.Fatalf("second=%+v calls=%d etag=%q err=%v", second, resolver.calls, client.ETag(), err)
	}
	client.Invalidate()
	if _, err := client.Get(t.Context(), "feature.enabled"); err != nil || resolver.calls != 2 {
		t.Fatalf("calls after invalidation=%d err=%v", resolver.calls, err)
	}
}

func TestClientReportsMissingKey(t *testing.T) {
	client, _ := New(&fakeResolver{}, Options{Environment: "testing", Service: "orders"})
	_, err := client.Get(t.Context(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get error = %v", err)
	}
}

func TestClientInvalidatesMatchingChangeEvent(t *testing.T) {
	resolver := &fakeResolver{}
	client, _ := New(resolver, Options{Environment: "testing", TenantID: "tenant-1", Service: "orders", TTL: time.Hour})
	if _, err := client.Get(t.Context(), "feature.enabled"); err != nil {
		t.Fatal(err)
	}
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{EventID: "event-1", EventType: "platform.config.v1.ConfigChanged", AggregateID: "config-1", AggregateType: "config_entry", SchemaVersion: 1, OccurredAt: time.Now()}, &configv1.ConfigChangedEvent{Entry: &configv1.ConfigEntry{Environment: "testing", TenantId: "tenant-1", Service: "orders", Key: "feature.enabled"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HandleChanged(t.Context(), envelope); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(t.Context(), "feature.enabled"); err != nil || resolver.calls != 2 {
		t.Fatalf("calls=%d err=%v", resolver.calls, err)
	}
}
