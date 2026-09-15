package routepolicy

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/lihongjie0209/microservice-platform-go/authz"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

var (
	ErrMissing       = errors.New("route policy missing")
	ErrInvalid       = errors.New("route policy invalid")
	ErrDenied        = errors.New("route policy denied")
	permissionKeyRef = regexp.MustCompile(`permissions\[\s*"([a-z][a-z0-9_.:-]{2,127})"\s*\]`)
)

const MaxExpressionBytes, MaxPermissionRefs = 4096, 8

type Permission struct {
	ID, Key, Resource, Action string
	Scope                     authz.Scope
}
type Definition struct {
	ID, RouteID, Expression string
	Permissions             map[string]Permission
	Version                 int64
}
type Policy struct {
	definition Definition
	program    cel.Program
}
type Compiler struct{ environment *cel.Env }

func NewCompiler() (*Compiler, error) {
	environment, err := cel.NewEnv(cel.Variable("anonymous", cel.BoolType), cel.Variable("authenticated", cel.BoolType), cel.Variable("principal_type", cel.StringType), cel.Variable("permissions", cel.MapType(cel.StringType, cel.BoolType)))
	if err != nil {
		return nil, fmt.Errorf("create route policy environment: %w", err)
	}
	return &Compiler{environment: environment}, nil
}

func (c *Compiler) Compile(definition Definition) (*Policy, error) {
	expression := strings.TrimSpace(definition.Expression)
	if expression == "" || len(expression) > MaxExpressionBytes {
		return nil, fmt.Errorf("%w: invalid expression length", ErrInvalid)
	}
	keys := PermissionKeys(expression)
	if len(keys) > MaxPermissionRefs {
		return nil, fmt.Errorf("%w: too many permission references", ErrInvalid)
	}
	for _, key := range keys {
		permission, ok := definition.Permissions[key]
		if !ok || permission.Resource == "" || permission.Action == "" {
			return nil, fmt.Errorf("%w: inactive permission %q", ErrInvalid, key)
		}
	}
	if len(definition.Permissions) != len(keys) {
		return nil, fmt.Errorf("%w: unused permission references", ErrInvalid)
	}
	ast, issues := c.environment.Compile(expression)
	if issues.Err() != nil {
		return nil, fmt.Errorf("%w: compile expression: %v", ErrInvalid, issues.Err())
	}
	if ast.OutputType() != cel.BoolType {
		return nil, fmt.Errorf("%w: expression must return bool", ErrInvalid)
	}
	program, err := c.environment.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("create expression program: %w", err)
	}
	return &Policy{definition: definition, program: program}, nil
}

func PermissionKeys(expression string) []string {
	matches, seen := permissionKeyRef.FindAllStringSubmatch(expression, -1), map[string]struct{}{}
	keys := make([]string, 0, len(matches))
	for _, match := range matches {
		if _, ok := seen[match[1]]; !ok {
			seen[match[1]] = struct{}{}
			keys = append(keys, match[1])
		}
	}
	return keys
}

func (p *Policy) Evaluate(ctx context.Context, authorizer authz.Authorizer) error {
	identity, authenticated := principal.FromContext(ctx)
	if authenticated && len(p.definition.Permissions) > 0 && authorizer == nil {
		return authz.ErrDecisionUnavailable
	}
	decisions := make(map[string]bool, len(p.definition.Permissions))
	for key, permission := range p.definition.Permissions {
		if !authenticated {
			decisions[key] = false
			continue
		}
		err := authorizer.Authorize(ctx, identity, authz.Requirement{Resource: permission.Resource, Action: permission.Action, Scope: permission.Scope})
		if err != nil && !errors.Is(err, authz.ErrDenied) {
			return fmt.Errorf("authorize permission %q: %w", key, err)
		}
		decisions[key] = err == nil
	}
	principalType := ""
	if authenticated {
		principalType = string(identity.Type)
	}
	value, _, err := p.program.ContextEval(ctx, map[string]any{"anonymous": !authenticated, "authenticated": authenticated, "principal_type": principalType, "permissions": decisions})
	if err != nil {
		return fmt.Errorf("evaluate route policy: %w", err)
	}
	if value != types.True {
		return ErrDenied
	}
	return nil
}
