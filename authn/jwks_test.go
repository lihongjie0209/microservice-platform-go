package authn

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	claims := identityClaims{SubjectType: "user", TenantID: "tenant-1", RegisteredClaims: jwt.RegisteredClaims{Issuer: "identity-service", Subject: "user-1", Audience: []string{"tenant-service"}, IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}}
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
