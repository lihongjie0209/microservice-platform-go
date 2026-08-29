package authn

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

type JWKSConfig struct {
	URL      string
	Issuer   string
	Audience string
}

type identityClaims struct {
	SubjectType  string `json:"subject_type"`
	SessionID    string `json:"sid,omitempty"`
	TenantID     string `json:"tenant_id,omitempty"`
	MembershipID string `json:"membership_id,omitempty"`
	jwt.RegisteredClaims
}

type JWKSVerifier struct {
	keyfunc  keyfunc.Keyfunc
	issuer   string
	audience string
	cancel   context.CancelFunc
}

func NewJWKSVerifier(ctx context.Context, config JWKSConfig) (*JWKSVerifier, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if strings.TrimSpace(config.URL) == "" || strings.TrimSpace(config.Issuer) == "" || strings.TrimSpace(config.Audience) == "" {
		return nil, errors.New("JWKS URL, issuer, and audience are required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	keys, err := keyfunc.NewDefaultCtx(runCtx, []string{config.URL})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("load JWKS: %w", err)
	}
	return &JWKSVerifier{keyfunc: keys, issuer: config.Issuer, audience: config.Audience, cancel: cancel}, nil
}
func (v *JWKSVerifier) VerifyBearer(ctx context.Context, raw string) (principal.Principal, error) {
	claims := new(identityClaims)
	token, err := jwt.ParseWithClaims(raw, claims, v.keyfunc.KeyfuncCtx(ctx), jwt.WithIssuer(v.issuer), jwt.WithAudience(v.audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithValidMethods([]string{"EdDSA"}))
	if err != nil {
		return principal.Principal{}, fmt.Errorf("verify access token: %w", err)
	}
	if !token.Valid || claims.Subject == "" {
		return principal.Principal{}, errors.New("invalid access token claims")
	}
	subjectType := principal.Type(claims.SubjectType)
	if subjectType != principal.TypeUser && subjectType != principal.TypeServiceAccount {
		return principal.Principal{}, errors.New("invalid subject type")
	}
	return principal.Principal{ID: claims.Subject, Type: subjectType, TenantID: claims.TenantID, MembershipID: claims.MembershipID, SessionID: claims.SessionID}, nil
}
func (v *JWKSVerifier) Close() {
	if v != nil && v.cancel != nil {
		v.cancel()
	}
}
