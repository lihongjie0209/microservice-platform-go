package authn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

type JWKSConfig struct {
	URL      string
	Issuer   string
	Audience string
	// HTTPTimeout bounds the initial key fetch and subsequent refreshes.
	// Zero selects the two-second default.
	HTTPTimeout time.Duration
}

// ErrJWKSUnavailable means key verification was blocked by a failed remote
// refresh, not that a healthy key set rejected the token.
var ErrJWKSUnavailable = errors.New("JWKS unavailable")

type refreshFailureKey struct{}
type refreshFailure struct{ failed bool }

type identityClaims struct {
	SubjectType       string `json:"subject_type,omitempty"`
	PrincipalType     string `json:"principal_type,omitempty"`
	SessionID         string `json:"sid,omitempty"`
	TemplateSessionID string `json:"session_id,omitempty"`
	TenantID          string `json:"tenant_id,omitempty"`
	MembershipID      string `json:"membership_id,omitempty"`
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
	if config.HTTPTimeout == 0 {
		config.HTTPTimeout = 2 * time.Second
	}
	if config.HTTPTimeout < 100*time.Millisecond || config.HTTPTimeout > 10*time.Second {
		return nil, errors.New("JWKS HTTP timeout must be between 100ms and 10s")
	}
	runCtx, cancel := context.WithCancel(ctx)
	returnFirstHTTPError := false
	keys, err := keyfunc.NewDefaultOverrideCtx(runCtx, []string{config.URL}, keyfunc.Override{
		HTTPTimeout:               config.HTTPTimeout,
		NoErrorReturnFirstHTTPReq: &returnFirstHTTPError,
		RefreshErrorHandlerFunc: func(string) func(context.Context, error) {
			return func(refreshCtx context.Context, _ error) {
				if failure, ok := refreshCtx.Value(refreshFailureKey{}).(*refreshFailure); ok {
					failure.failed = true
					return
				}
				slog.Default().WarnContext(refreshCtx, "background JWKS refresh failed")
			}
		},
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("load JWKS: %w", err)
	}
	return &JWKSVerifier{keyfunc: keys, issuer: config.Issuer, audience: config.Audience, cancel: cancel}, nil
}
func (v *JWKSVerifier) VerifyBearer(ctx context.Context, raw string) (principal.Principal, error) {
	claims := new(identityClaims)
	failure := new(refreshFailure)
	keyCtx := context.WithValue(ctx, refreshFailureKey{}, failure)
	token, err := jwt.ParseWithClaims(raw, claims, v.keyfunc.KeyfuncCtx(keyCtx), jwt.WithIssuer(v.issuer), jwt.WithAudience(v.audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithValidMethods([]string{"EdDSA", "RS256", "ES256"}))
	if err != nil {
		if failure.failed {
			return principal.Principal{}, fmt.Errorf("verify access token: %w", ErrJWKSUnavailable)
		}
		return principal.Principal{}, fmt.Errorf("verify access token: %w", err)
	}
	if !token.Valid || strings.TrimSpace(claims.Subject) == "" {
		return principal.Principal{}, errors.New("invalid access token claims")
	}
	if claims.SubjectType != "" && claims.PrincipalType != "" && claims.SubjectType != claims.PrincipalType {
		return principal.Principal{}, errors.New("conflicting access token subject types")
	}
	if claims.SessionID != "" && claims.TemplateSessionID != "" && claims.SessionID != claims.TemplateSessionID {
		return principal.Principal{}, errors.New("conflicting access token session ids")
	}
	subjectTypeValue := claims.SubjectType
	if subjectTypeValue == "" {
		subjectTypeValue = claims.PrincipalType
	}
	sessionID := claims.SessionID
	if sessionID == "" {
		sessionID = claims.TemplateSessionID
	}
	subjectType := principal.Type(subjectTypeValue)
	if subjectType != principal.TypeUser && subjectType != principal.TypeServiceAccount {
		return principal.Principal{}, errors.New("invalid subject type")
	}
	if subjectType == principal.TypeUser && strings.TrimSpace(sessionID) == "" {
		return principal.Principal{}, errors.New("user token requires a session id")
	}
	if subjectType == principal.TypeUser && (strings.TrimSpace(claims.TenantID) == "") != (strings.TrimSpace(claims.MembershipID) == "") {
		return principal.Principal{}, errors.New("user token tenant and membership context must be paired")
	}
	if subjectType == principal.TypeServiceAccount && (sessionID != "" || claims.TenantID != "" || claims.MembershipID != "") {
		return principal.Principal{}, errors.New("service account token cannot carry user context")
	}
	return principal.Principal{ID: claims.Subject, Type: subjectType, TenantID: claims.TenantID, MembershipID: claims.MembershipID, SessionID: sessionID}, nil
}
func (v *JWKSVerifier) Close() {
	if v != nil && v.cancel != nil {
		v.cancel()
	}
}
