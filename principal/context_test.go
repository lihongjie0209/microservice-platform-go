package principal_test

import (
	"errors"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/principal"
)

func TestContext(t *testing.T) {
	if _, ok := principal.FromContext(nil); ok {
		t.Fatal("FromContext(nil) found principal")
	}
	if _, err := principal.Require(t.Context()); !errors.Is(err, principal.ErrMissing) {
		t.Fatalf("Require() error = %v, want ErrMissing", err)
	}
	want := principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1"}
	got, err := principal.Require(principal.WithContext(t.Context(), want))
	if err != nil || got != want {
		t.Fatalf("Require() = (%+v, %v), want (%+v, nil)", got, err, want)
	}
}
