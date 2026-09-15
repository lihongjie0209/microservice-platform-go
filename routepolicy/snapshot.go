package routepolicy

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/lihongjie0209/microservice-platform-go/authz"
)

type Loader interface {
	Load(context.Context) ([]Definition, error)
}
type Snapshot struct {
	compiler *Compiler
	value    atomic.Pointer[map[string]*Policy]
}

func NewSnapshot(compiler *Compiler) *Snapshot {
	snapshot := &Snapshot{compiler: compiler}
	empty := map[string]*Policy{}
	snapshot.value.Store(&empty)
	return snapshot
}
func (s *Snapshot) Replace(definitions []Definition) error {
	next := make(map[string]*Policy, len(definitions))
	for _, definition := range definitions {
		if definition.RouteID == "" {
			return fmt.Errorf("%w: empty route id", ErrInvalid)
		}
		policy, err := s.compiler.Compile(definition)
		if err != nil {
			return fmt.Errorf("compile policy %q: %w", definition.ID, err)
		}
		if _, exists := next[definition.RouteID]; exists {
			return fmt.Errorf("%w: duplicate route %q", ErrInvalid, definition.RouteID)
		}
		next[definition.RouteID] = policy
	}
	s.value.Store(&next)
	return nil
}
func (s *Snapshot) Reload(ctx context.Context, loader Loader) error {
	definitions, err := loader.Load(ctx)
	if err != nil {
		return fmt.Errorf("load route policies: %w", err)
	}
	return s.Replace(definitions)
}
func (s *Snapshot) Evaluate(ctx context.Context, routeID string, authorizer authz.Authorizer) error {
	policy, ok := (*s.value.Load())[routeID]
	if !ok {
		return fmt.Errorf("%w: route %q", ErrMissing, routeID)
	}
	return policy.Evaluate(ctx, authorizer)
}
