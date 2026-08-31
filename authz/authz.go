// Package authz provides shared enforcement adapters while leaving policy
// ownership and decision evaluation in authorization-service.
package authz

import (
	"context"
	"errors"

	"github.com/lihongjie0209/microservice-platform-go/principal"
)

var (
	ErrDenied              = errors.New("authorization denied")
	ErrRequirementMissing  = errors.New("authorization requirement is missing")
	ErrDecisionUnavailable = errors.New("authorization decision is unavailable")
)

type Requirement struct {
	Resource   string
	ResourceID string
	Action     string
	Attributes map[string]string
}

type Authorizer interface {
	Authorize(context.Context, principal.Principal, Requirement) error
}

func Enforce(ctx context.Context, authorizer Authorizer, requirement Requirement) error {
	identity, ok := principal.FromContext(ctx)
	if !ok {
		return principal.ErrMissing
	}
	if authorizer == nil || requirement.Resource == "" || requirement.Action == "" {
		return ErrRequirementMissing
	}
	if err := authorizer.Authorize(ctx, identity, requirement); err != nil {
		return err
	}
	return nil
}
