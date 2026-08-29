// Package authz provides shared enforcement adapters while leaving policy
// ownership and decision evaluation in authorization-service.
package authz

import (
	"context"
	"errors"

	"github.com/lihongjie0209/microservice-platform-go/principal"
)

var (
	ErrDenied             = errors.New("authorization denied")
	ErrRequirementMissing = errors.New("authorization requirement is missing")
)

type Requirement struct {
	Resource string
	Action   string
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
		return errors.Join(ErrDenied, err)
	}
	return nil
}
