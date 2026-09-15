package authz

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
)

var (
	ErrPolicyMissing = errors.New("authorization policy is missing")
	ErrInvalidPolicy = errors.New("authorization policy is invalid")
)

var permissionPart = regexp.MustCompile(`^[a-z][a-z0-9_.:-]{1,127}$`)

type Policy struct {
	Operation   string
	Public      bool
	Requirement Requirement
}

// HTTPResolver adapts the registry to HTTPMiddleware. Unknown routes are
// intentionally returned as protected with an empty requirement (fail closed).
func (r *PolicyRegistry) HTTPResolver(request *http.Request) (Requirement, bool) {
	requirement, public, err := r.Resolve(request.URL.Path)
	if err != nil {
		return Requirement{}, true
	}
	return requirement, !public
}

// GRPCResolver adapts the registry to unary and stream interceptors. Unknown
// methods fail closed through ErrRequirementMissing.
func (r *PolicyRegistry) GRPCResolver(fullMethod string) (Requirement, bool) {
	requirement, public, err := r.Resolve(fullMethod)
	if err != nil {
		return Requirement{}, true
	}
	return requirement, !public
}

// PolicyRegistry is immutable after construction and safe for concurrent use.
type PolicyRegistry struct{ policies map[string]Policy }

func NewPolicyRegistry(policies ...Policy) (*PolicyRegistry, error) {
	registry := &PolicyRegistry{policies: make(map[string]Policy, len(policies))}
	for _, policy := range policies {
		if policy.Operation == "" {
			return nil, fmt.Errorf("%w: operation is empty", ErrInvalidPolicy)
		}
		if _, exists := registry.policies[policy.Operation]; exists {
			return nil, fmt.Errorf("%w: duplicate operation %q", ErrInvalidPolicy, policy.Operation)
		}
		if policy.Public {
			if policy.Requirement.Resource != "" || policy.Requirement.Action != "" {
				return nil, fmt.Errorf("%w: public operation %q has a requirement", ErrInvalidPolicy, policy.Operation)
			}
		} else if !ValidPermission(policy.Requirement.Resource, policy.Requirement.Action) {
			return nil, fmt.Errorf("%w: operation %q has invalid resource/action", ErrInvalidPolicy, policy.Operation)
		}
		registry.policies[policy.Operation] = policy
	}
	return registry, nil
}

func (r *PolicyRegistry) Resolve(operation string) (Requirement, bool, error) {
	if r == nil {
		return Requirement{}, false, ErrPolicyMissing
	}
	policy, exists := r.policies[operation]
	if !exists {
		return Requirement{}, false, fmt.Errorf("%w: %s", ErrPolicyMissing, operation)
	}
	return policy.Requirement, policy.Public, nil
}

func ValidPermission(resource, action string) bool {
	return permissionPart.MatchString(resource) && permissionPart.MatchString(action)
}

func PermissionKey(resource, action string) (string, error) {
	if !ValidPermission(resource, action) {
		return "", ErrInvalidPolicy
	}
	return resource + "." + action, nil
}
