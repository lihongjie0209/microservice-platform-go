package authz

import (
	"errors"
	"testing"
)

func TestPolicyRegistryIsFailClosed(t *testing.T) {
	r, err := NewPolicyRegistry(
		Policy{Operation: "/live", Public: true},
		Policy{Operation: "/api/v1/users/page", Requirement: Requirement{Resource: "platform.user", Action: "page", Scope: ScopeTenant}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, public, err := r.Resolve("/live"); err != nil || !public {
		t.Fatalf("public=%v err=%v", public, err)
	}
	requirement, public, err := r.Resolve("/api/v1/users/page")
	if err != nil || public || requirement.Resource != "platform.user" {
		t.Fatalf("requirement=%+v public=%v err=%v", requirement, public, err)
	}
	if _, _, err := r.Resolve("/api/v1/users/new"); !errors.Is(err, ErrPolicyMissing) {
		t.Fatalf("error=%v", err)
	}
}

func TestPolicyRegistryRejectsInvalidDefinitions(t *testing.T) {
	_, err := NewPolicyRegistry(Policy{Operation: "x", Requirement: Requirement{Resource: "Platform User", Action: "read"}})
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("error=%v", err)
	}
}
