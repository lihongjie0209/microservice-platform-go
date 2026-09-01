package appaccess_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	applicationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/application/v1"
	"google.golang.org/grpc"
)

type checker struct {
	request  *applicationv1.BatchCheckTenantApplicationsRequest
	response *applicationv1.BatchCheckTenantApplicationsResponse
	err      error
}

type checkFunc func(context.Context, *applicationv1.BatchCheckTenantApplicationsRequest, ...grpc.CallOption) (*applicationv1.BatchCheckTenantApplicationsResponse, error)

func (f checkFunc) BatchCheckTenantApplications(ctx context.Context, request *applicationv1.BatchCheckTenantApplicationsRequest, options ...grpc.CallOption) (*applicationv1.BatchCheckTenantApplicationsResponse, error) {
	return f(ctx, request, options...)
}

func (c *checker) BatchCheckTenantApplications(_ context.Context, request *applicationv1.BatchCheckTenantApplicationsRequest, _ ...grpc.CallOption) (*applicationv1.BatchCheckTenantApplicationsResponse, error) {
	c.request = request
	return c.response, c.err
}

func TestGRPCVerifierVerify(t *testing.T) {
	tests := []struct {
		name     string
		client   appaccess.BatchCheckClient
		tenantID string
		appID    string
		want     error
	}{
		{
			name:     "granted",
			client:   &checker{response: decisions(decision("app-1", true, "active"))},
			tenantID: "tenant-1",
			appID:    "app-1",
		},
		{
			name:     "not granted",
			client:   &checker{response: decisions(decision("app-1", false, "revoked"))},
			tenantID: "tenant-1",
			appID:    "app-1",
			want:     appaccess.ErrNotGranted,
		},
		{name: "missing client", tenantID: "tenant-1", appID: "app-1", want: appaccess.ErrUnavailable},
		{
			name:     "upstream failure",
			client:   &checker{err: errors.New("unavailable")},
			tenantID: "tenant-1",
			appID:    "app-1",
			want:     appaccess.ErrUnavailable,
		},
		{
			name:     "missing decision",
			client:   &checker{response: decisions()},
			tenantID: "tenant-1",
			appID:    "app-1",
			want:     appaccess.ErrUnavailable,
		},
		{name: "invalid input", client: &checker{}, tenantID: "", appID: "app-1", want: appaccess.ErrInvalidArgument},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verifier := appaccess.NewGRPCVerifier(test.client, time.Second)
			err := verifier.Verify(t.Context(), test.tenantID, test.appID)
			if !errors.Is(err, test.want) {
				t.Fatalf("Verify() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestGRPCVerifierCheckNormalizesBatch(t *testing.T) {
	client := &checker{response: decisions(decision("app-1", true, "active"), decision("app-2", false, "expired"))}
	verifier := appaccess.NewGRPCVerifier(client, time.Second)

	values, err := verifier.Check(t.Context(), " tenant-1 ", []string{" app-1 ", "app-1", "app-2"})
	if err != nil {
		t.Fatal(err)
	}
	if client.request.GetTenantId() != "tenant-1" || len(client.request.GetApplicationIds()) != 2 {
		t.Fatalf("request = %+v", client.request)
	}
	if !values["app-1"].Granted || values["app-2"].Granted || values["app-2"].Reason != "expired" {
		t.Fatalf("decisions = %+v", values)
	}
}

func TestGRPCVerifierPropagatesCancellation(t *testing.T) {
	client := checkFunc(func(ctx context.Context, _ *applicationv1.BatchCheckTenantApplicationsRequest, _ ...grpc.CallOption) (*applicationv1.BatchCheckTenantApplicationsResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	verifier := appaccess.NewGRPCVerifier(client, time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := verifier.Verify(ctx, "tenant-1", "app-1")
	if !errors.Is(err, appaccess.ErrUnavailable) {
		t.Fatalf("Verify() error = %v, want %v", err, appaccess.ErrUnavailable)
	}
}

func decision(id string, granted bool, reason string) *applicationv1.TenantApplicationDecision {
	return &applicationv1.TenantApplicationDecision{ApplicationId: id, Granted: granted, Reason: reason}
}

func decisions(values ...*applicationv1.TenantApplicationDecision) *applicationv1.BatchCheckTenantApplicationsResponse {
	return &applicationv1.BatchCheckTenantApplicationsResponse{Decisions: values}
}
