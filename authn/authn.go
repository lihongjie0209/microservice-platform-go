// Package authn provides transport-independent authentication policy and
// adapters for HTTP and gRPC servers.
package authn

import (
	"context"
	"crypto/subtle"
	"errors"
	"path"
	"strings"

	"github.com/lihongjie0209/microservice-platform-go/principal"
)

var (
	ErrMissingCredential = errors.New("authentication credential is missing")
	ErrInvalidCredential = errors.New("authentication credential is invalid")
)

// BearerVerifier verifies a JWT or another bearer token and returns the
// normalized platform principal. Domain services own the token policy.
type BearerVerifier interface {
	VerifyBearer(context.Context, string) (principal.Principal, error)
}

type PSKPolicy struct {
	Key       string
	Targets   []string
	Principal principal.Principal
}

type Policy struct {
	SkipTargets []string
	PSK         []PSKPolicy
	Bearer      BearerVerifier
}

// Authenticate applies PSK policy before public-route policy. This prevents a
// mistakenly overlapping public wildcard from bypassing a protected PSK route.
func (p Policy) Authenticate(ctx context.Context, target, authorization string) (context.Context, error) {
	for _, candidate := range p.PSK {
		if !MatchesAny(target, candidate.Targets) {
			continue
		}
		if !verifyPSK(authorization, candidate.Key) || strings.TrimSpace(candidate.Principal.ID) == "" {
			return ctx, ErrInvalidCredential
		}
		return principal.WithContext(ctx, candidate.Principal), nil
	}
	if MatchesAny(target, p.SkipTargets) {
		return ctx, nil
	}
	scheme, raw, ok := strings.Cut(authorization, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(raw) == "" {
		return ctx, ErrMissingCredential
	}
	if p.Bearer == nil {
		return ctx, ErrInvalidCredential
	}
	identity, err := p.Bearer.VerifyBearer(ctx, raw)
	if err != nil || strings.TrimSpace(identity.ID) == "" {
		return ctx, ErrInvalidCredential
	}
	return principal.WithContext(ctx, identity), nil
}

func MatchesAny(value string, patterns []string) bool {
	for _, pattern := range patterns {
		if matched, err := path.Match(pattern, value); err == nil && matched {
			return true
		}
	}
	return false
}

func verifyPSK(authorization, expected string) bool {
	scheme, supplied, ok := strings.Cut(authorization, " ")
	return ok && strings.EqualFold(scheme, "PSK") && supplied != "" && len(supplied) == len(expected) &&
		subtle.ConstantTimeCompare([]byte(supplied), []byte(expected)) == 1
}
