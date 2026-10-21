package authn

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWKSVerifier(t *testing.T) {
	t.Parallel()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"keys":[{"kty":"OKP","use":"sig","kid":"key-1","crv":"Ed25519","alg":"EdDSA","x":"%s"}]}`, base64.RawURLEncoding.EncodeToString(publicKey))
	}))
	defer server.Close()
	verifier, err := NewJWKSVerifier(context.Background(), JWKSConfig{URL: server.URL, Issuer: "identity-service", Audience: "tenant-service"})
	if err != nil {
		t.Fatal(err)
	}
	defer verifier.Close()
	now := time.Now()
	claims := identityClaims{SubjectType: "user", SessionID: "session-1", TenantID: "tenant-1", MembershipID: "member-1", RegisteredClaims: jwt.RegisteredClaims{Issuer: "identity-service", Subject: "user-1", Audience: []string{"tenant-service"}, IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = "key-1"
	raw, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := verifier.VerifyBearer(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if actor.ID != "user-1" || actor.TenantID != "tenant-1" {
		t.Fatalf("actor=%#v", actor)
	}
}

func TestJWKSVerifierAcceptsTemplateRSAndESTokens(t *testing.T) {
	for _, test := range []struct {
		name string
		key  func(t *testing.T) (string, jwt.SigningMethod, any)
	}{
		{name: "RS256", key: func(t *testing.T) (string, jwt.SigningMethod, any) {
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			return fmt.Sprintf(`{"keys":[{"kty":"RSA","use":"sig","kid":"key-1","alg":"RS256","n":"%s","e":"%s"}]}`, base64.RawURLEncoding.EncodeToString(key.N.Bytes()), base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1})), jwt.SigningMethodRS256, key
		}},
		{name: "ES256", key: func(t *testing.T) (string, jwt.SigningMethod, any) {
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			return fmt.Sprintf(`{"keys":[{"kty":"EC","use":"sig","kid":"key-1","alg":"ES256","crv":"P-256","x":"%s","y":"%s"}]}`, base64.RawURLEncoding.EncodeToString(key.X.FillBytes(make([]byte, 32))), base64.RawURLEncoding.EncodeToString(key.Y.FillBytes(make([]byte, 32)))), jwt.SigningMethodES256, key
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			jwks, method, key := test.key(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, jwks)
			}))
			defer server.Close()
			verifier, err := NewJWKSVerifier(t.Context(), JWKSConfig{URL: server.URL, Issuer: "identity-service", Audience: "consumer"})
			if err != nil {
				t.Fatal(err)
			}
			defer verifier.Close()
			now := time.Now()
			claims := jwt.MapClaims{"iss": "identity-service", "aud": "consumer", "sub": "user-1", "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(), "principal_type": "user", "session_id": "session-1"}
			token := jwt.NewWithClaims(method, claims)
			token.Header["kid"] = "key-1"
			raw, err := token.SignedString(key)
			if err != nil {
				t.Fatal(err)
			}
			actor, err := verifier.VerifyBearer(t.Context(), raw)
			if err != nil || actor.ID != "user-1" || actor.SessionID != "session-1" {
				t.Fatalf("VerifyBearer() actor=%+v error=%v", actor, err)
			}
		})
	}
}

func TestJWKSVerifierFailsClosedOnInitialFetch(t *testing.T) {
	for _, test := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{name: "server error", handler: func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}},
		{name: "bounded timeout", handler: func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			start := time.Now()
			verifier, err := NewJWKSVerifier(t.Context(), JWKSConfig{URL: server.URL, Issuer: "identity-service", Audience: "consumer", HTTPTimeout: 100 * time.Millisecond})
			if verifier != nil || err == nil {
				t.Fatalf("NewJWKSVerifier() verifier=%v error=%v, want startup failure", verifier, err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("initial JWKS fetch ignored configured timeout")
			}
		})
	}
}

func TestJWKSVerifierRejectsUnboundedHTTPTimeout(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
	}{
		{name: "negative", timeout: -time.Second},
		{name: "below minimum", timeout: 99 * time.Millisecond},
		{name: "above maximum", timeout: 10*time.Second + time.Nanosecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			verifier, err := NewJWKSVerifier(t.Context(), JWKSConfig{URL: "http://127.0.0.1:1/jwks", Issuer: "issuer", Audience: "audience", HTTPTimeout: test.timeout})
			if verifier != nil || err == nil {
				t.Fatalf("NewJWKSVerifier() verifier=%v error=%v, want timeout validation", verifier, err)
			}
		})
	}
}

func TestJWKSVerifierRejectsConflictingOrIncompletePrincipalClaims(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"keys":[{"kty":"OKP","use":"sig","kid":"key-1","crv":"Ed25519","alg":"EdDSA","x":"%s"}]}`, base64.RawURLEncoding.EncodeToString(publicKey))
	}))
	defer server.Close()
	verifier, err := NewJWKSVerifier(t.Context(), JWKSConfig{URL: server.URL, Issuer: "identity-service", Audience: "consumer"})
	if err != nil {
		t.Fatal(err)
	}
	defer verifier.Close()
	for _, test := range []struct {
		name   string
		claims jwt.MapClaims
	}{
		{name: "conflicting subject type", claims: jwt.MapClaims{"subject_type": "user", "principal_type": "service_account", "sid": "session-1"}},
		{name: "conflicting session id", claims: jwt.MapClaims{"subject_type": "user", "sid": "session-1", "session_id": "session-2"}},
		{name: "user without session", claims: jwt.MapClaims{"subject_type": "user"}},
		{name: "unpaired tenant", claims: jwt.MapClaims{"subject_type": "user", "sid": "session-1", "tenant_id": "tenant-1"}},
		{name: "service account with session", claims: jwt.MapClaims{"subject_type": "service_account", "sid": "session-1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			claims := test.claims
			claims["iss"] = "identity-service"
			claims["aud"] = "consumer"
			claims["sub"] = "subject-1"
			claims["iat"] = time.Now().Unix()
			claims["exp"] = time.Now().Add(time.Minute).Unix()
			token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
			token.Header["kid"] = "key-1"
			raw, err := token.SignedString(privateKey)
			if err != nil {
				t.Fatal(err)
			}
			if actor, err := verifier.VerifyBearer(t.Context(), raw); err == nil {
				t.Fatalf("VerifyBearer() accepted malformed claims as %+v", actor)
			}
		})
	}
}

func TestJWKSVerifierDistinguishesUnknownKeyFromRefreshOutage(t *testing.T) {
	for _, test := range []struct {
		name        string
		outage      bool
		unavailable bool
	}{
		{name: "unknown key with healthy JWKS"},
		{name: "unknown key during JWKS outage", outage: true, unavailable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cachedPublic, _, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			_, rotatedPrivate, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			var unavailable atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if unavailable.Load() {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				_, _ = fmt.Fprintf(w, `{"keys":[{"kty":"OKP","use":"sig","kid":"key-1","crv":"Ed25519","alg":"EdDSA","x":"%s"}]}`, base64.RawURLEncoding.EncodeToString(cachedPublic))
			}))
			defer server.Close()
			verifier, err := NewJWKSVerifier(t.Context(), JWKSConfig{URL: server.URL, Issuer: "identity-service", Audience: "consumer"})
			if err != nil {
				t.Fatal(err)
			}
			defer verifier.Close()
			unavailable.Store(test.outage)
			now := time.Now()
			claims := identityClaims{SubjectType: "user", SessionID: "session-1", RegisteredClaims: jwt.RegisteredClaims{
				Issuer: "identity-service", Subject: "user-1", Audience: []string{"consumer"},
				IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
			}}
			token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
			token.Header["kid"] = "key-2"
			raw, err := token.SignedString(rotatedPrivate)
			if err != nil {
				t.Fatal(err)
			}
			_, err = verifier.VerifyBearer(t.Context(), raw)
			if err == nil || errors.Is(err, ErrJWKSUnavailable) != test.unavailable {
				t.Fatalf("VerifyBearer() error=%v, want unavailable=%v", err, test.unavailable)
			}
		})
	}
}
