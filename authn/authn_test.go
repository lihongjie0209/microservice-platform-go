package authn_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/authn"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

type verifier struct{}

func (verifier) VerifyBearer(_ context.Context, raw string) (principal.Principal, error) {
	if raw != "valid" {
		return principal.Principal{}, errors.New("invalid")
	}
	return principal.Principal{ID: "user-1", Type: principal.TypeUser}, nil
}

func TestPolicy_Authenticate(t *testing.T) {
	key := "01234567890123456789012345678901"
	policy := authn.Policy{
		SkipTargets: []string{"/public/*"},
		PSK:         []authn.PSKPolicy{{Key: key, Targets: []string{"/partner/*"}, Principal: principal.Principal{ID: "partner", Type: principal.TypeServiceAccount}}},
		Bearer:      verifier{},
	}
	tests := []struct {
		name    string
		target  string
		header  string
		wantID  string
		wantErr error
	}{
		{name: "public", target: "/public/status"},
		{name: "bearer", target: "/users/get", header: "Bearer valid", wantID: "user-1"},
		{name: "psk", target: "/partner/sync", header: "PSK " + key, wantID: "partner"},
		{name: "missing", target: "/users/get", wantErr: authn.ErrMissingCredential},
		{name: "invalid psk", target: "/partner/sync", header: "PSK wrong", wantErr: authn.ErrInvalidCredential},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, err := policy.Authenticate(t.Context(), tt.target, tt.header)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Authenticate() error = %v, want %v", err, tt.wantErr)
			}
			identity, _ := principal.FromContext(ctx)
			if identity.ID != tt.wantID {
				t.Fatalf("principal ID = %q, want %q", identity.ID, tt.wantID)
			}
		})
	}
}

func TestHTTPMiddleware_InjectsPrincipal(t *testing.T) {
	policy := authn.Policy{Bearer: verifier{}}
	handler := authn.HTTPMiddleware(policy, nil)(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		identity, _ := principal.FromContext(request.Context())
		_, _ = response.Write([]byte(identity.ID))
	}))
	request := httptest.NewRequest(http.MethodPost, "/users/get", nil)
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "user-1" {
		t.Fatalf("response = (%d, %q)", response.Code, response.Body.String())
	}
}
