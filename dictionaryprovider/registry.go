// Package dictionaryprovider manages the registration lease of a dynamic
// dictionary provider. Business services still own and implement their data;
// this package only coordinates their presence in dictionary-service.
package dictionaryprovider

import (
	"context"

	dictionaryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/dictionary/v1"
	"google.golang.org/grpc"
)

// Registry is the small control-plane contract used by Registrant. Keeping it
// separate from the generated client makes provider services independently testable.
type Registry interface {
	Register(context.Context, *dictionaryv1.RegisterProviderRequest) (*dictionaryv1.RegisterProviderResponse, error)
	Heartbeat(context.Context, *dictionaryv1.HeartbeatProviderRequest) (*dictionaryv1.HeartbeatProviderResponse, error)
	Unregister(context.Context, *dictionaryv1.UnregisterProviderRequest) error
}

type grpcRegistry struct {
	client dictionaryv1.DictionaryServiceClient
}

// NewGRPCRegistry adapts the shared generated DictionaryService client.
func NewGRPCRegistry(client dictionaryv1.DictionaryServiceClient) Registry {
	return &grpcRegistry{client: client}
}

func (r *grpcRegistry) Register(ctx context.Context, request *dictionaryv1.RegisterProviderRequest) (*dictionaryv1.RegisterProviderResponse, error) {
	return r.client.RegisterProvider(ctx, request)
}

func (r *grpcRegistry) Heartbeat(ctx context.Context, request *dictionaryv1.HeartbeatProviderRequest) (*dictionaryv1.HeartbeatProviderResponse, error) {
	return r.client.HeartbeatProvider(ctx, request)
}

func (r *grpcRegistry) Unregister(ctx context.Context, request *dictionaryv1.UnregisterProviderRequest) error {
	_, err := r.client.UnregisterProvider(ctx, request, grpc.WaitForReady(true))
	return err
}
