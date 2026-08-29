// Package principal carries the authenticated caller across transport and
// application boundaries without coupling domain code to HTTP or gRPC.
package principal

import (
	"context"
	"errors"
	"strings"
)

type Type string

const (
	TypeUser           Type = "user"
	TypeServiceAccount Type = "service_account"
	TypeSystem         Type = "system"
)

var ErrMissing = errors.New("authenticated principal is missing")

type Principal struct {
	ID           string
	Type         Type
	TenantID     string
	MembershipID string
	SessionID    string
}

type contextKey struct{}

func WithContext(ctx context.Context, value Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, value)
}

func FromContext(ctx context.Context) (Principal, bool) {
	if ctx == nil {
		return Principal{}, false
	}
	value, ok := ctx.Value(contextKey{}).(Principal)
	return value, ok && strings.TrimSpace(value.ID) != ""
}

func Require(ctx context.Context) (Principal, error) {
	value, ok := FromContext(ctx)
	if !ok {
		return Principal{}, ErrMissing
	}
	return value, nil
}

func SystemContext(ctx context.Context, actorID string) context.Context {
	return WithContext(ctx, Principal{ID: actorID, Type: TypeSystem})
}
