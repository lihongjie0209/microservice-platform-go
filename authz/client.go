package authz

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/principal"
	authorizationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/authorization/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var ErrInvalidPrincipal = errors.New("principal cannot be mapped to an authorization subject")

type CheckClient interface {
	Check(context.Context, *authorizationv1.CheckRequest, ...grpc.CallOption) (*authorizationv1.CheckResponse, error)
}

type GRPCAuthorizer struct {
	client  CheckClient
	timeout time.Duration
}

func NewGRPCAuthorizer(client CheckClient, timeout time.Duration) *GRPCAuthorizer {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &GRPCAuthorizer{client: client, timeout: timeout}
}

type callerCredentialKey struct{}

func WithCallerCredential(ctx context.Context, value string) context.Context {
	if ctx == nil || strings.TrimSpace(value) == "" {
		return ctx
	}
	return context.WithValue(ctx, callerCredentialKey{}, value)
}

func (a *GRPCAuthorizer) Authorize(ctx context.Context, identity principal.Principal, requirement Requirement) error {
	if a == nil || a.client == nil {
		return errors.New("authorization upstream is not configured")
	}
	subject, err := authorizationSubject(identity)
	if err != nil {
		return err
	}
	if strings.TrimSpace(identity.TenantID) == "" {
		return ErrInvalidPrincipal
	}
	callCtx, cancel := context.WithTimeout(forwardCallerCredential(ctx), a.timeout)
	defer cancel()
	response, err := a.client.Check(callCtx, &authorizationv1.CheckRequest{
		TenantId:     identity.TenantID,
		Subject:      subject,
		ResourceType: requirement.Resource,
		ResourceId:   requirement.ResourceID,
		Action:       requirement.Action,
		Attributes:   requirement.Attributes,
	})
	if err != nil {
		return err
	}
	if !response.GetAllowed() {
		return ErrDenied
	}
	return nil
}

func authorizationSubject(identity principal.Principal) (*authorizationv1.Subject, error) {
	switch identity.Type {
	case principal.TypeUser:
		if strings.TrimSpace(identity.MembershipID) == "" {
			return nil, ErrInvalidPrincipal
		}
		return &authorizationv1.Subject{Id: identity.MembershipID, Type: authorizationv1.SubjectType_SUBJECT_TYPE_MEMBERSHIP}, nil
	case principal.TypeServiceAccount, principal.TypeSystem:
		if strings.TrimSpace(identity.ID) == "" {
			return nil, ErrInvalidPrincipal
		}
		return &authorizationv1.Subject{Id: identity.ID, Type: authorizationv1.SubjectType_SUBJECT_TYPE_SERVICE_ACCOUNT}, nil
	default:
		return nil, ErrInvalidPrincipal
	}
}

func forwardCallerCredential(ctx context.Context) context.Context {
	if outgoing, ok := metadata.FromOutgoingContext(ctx); ok && len(outgoing.Get("authorization")) > 0 {
		return ctx
	}
	if incoming := metadata.ValueFromIncomingContext(ctx, "authorization"); len(incoming) > 0 {
		return metadata.AppendToOutgoingContext(ctx, "authorization", incoming[0])
	}
	if value, ok := ctx.Value(callerCredentialKey{}).(string); ok && value != "" {
		return metadata.AppendToOutgoingContext(ctx, "authorization", value)
	}
	return ctx
}
