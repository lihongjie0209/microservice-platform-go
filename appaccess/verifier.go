// Package appaccess verifies that a tenant has an active grant for an application.
package appaccess

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	applicationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/application/v1"
	"google.golang.org/grpc"
)

const maxBatchSize = 100

var (
	ErrInvalidArgument = errors.New("invalid application access request")
	ErrNotGranted      = errors.New("tenant application access is not granted")
	ErrUnavailable     = errors.New("application access decision is unavailable")
)

type BatchCheckClient interface {
	BatchCheckTenantApplications(context.Context, *applicationv1.BatchCheckTenantApplicationsRequest, ...grpc.CallOption) (*applicationv1.BatchCheckTenantApplicationsResponse, error)
}

type Verifier interface {
	Verify(context.Context, string, string) error
}

type Decision struct {
	Granted bool
	Reason  string
}

type GRPCVerifier struct {
	client  BatchCheckClient
	timeout time.Duration
}

func NewGRPCVerifier(client BatchCheckClient, timeout time.Duration) *GRPCVerifier {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &GRPCVerifier{client: client, timeout: timeout}
}

func (v *GRPCVerifier) Verify(ctx context.Context, tenantID, applicationID string) error {
	decisions, err := v.Check(ctx, tenantID, []string{applicationID})
	if err != nil {
		return err
	}
	decision := decisions[strings.TrimSpace(applicationID)]
	if !decision.Granted {
		return fmt.Errorf("%w: tenant=%s application=%s reason=%s", ErrNotGranted, tenantID, applicationID, decision.Reason)
	}
	return nil
}

func (v *GRPCVerifier) Check(ctx context.Context, tenantID string, applicationIDs []string) (map[string]Decision, error) {
	tenantID = strings.TrimSpace(tenantID)
	ids, err := normalizeIDs(applicationIDs)
	if tenantID == "" || err != nil {
		return nil, fmt.Errorf("%w: tenant and 1-%d unique application IDs are required", ErrInvalidArgument, maxBatchSize)
	}
	if v == nil || v.client == nil {
		return nil, fmt.Errorf("%w: upstream is not configured", ErrUnavailable)
	}

	callCtx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	response, err := v.client.BatchCheckTenantApplications(callCtx, &applicationv1.BatchCheckTenantApplicationsRequest{
		TenantId:       tenantID,
		ApplicationIds: ids,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}

	decisions := make(map[string]Decision, len(ids))
	for _, decision := range response.GetDecisions() {
		id := strings.TrimSpace(decision.GetApplicationId())
		if id != "" {
			decisions[id] = Decision{Granted: decision.GetGranted(), Reason: decision.GetReason()}
		}
	}
	for _, id := range ids {
		if _, ok := decisions[id]; !ok {
			return nil, fmt.Errorf("%w: decision missing for application %s", ErrUnavailable, id)
		}
	}
	return decisions, nil
}

func normalizeIDs(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > maxBatchSize {
		return nil, ErrInvalidArgument
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		id := strings.TrimSpace(value)
		if id == "" {
			return nil, ErrInvalidArgument
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result, nil
}
