// Package stableid generates deterministic UUID v5 identifiers for initialized
// reference data. It must not be used for transactional records or security.
package stableid

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

var (
	ErrInvalidNamespace = errors.New("invalid stable ID namespace")
	ErrInvalidKey       = errors.New("invalid canonical business key")
	canonicalKey        = regexp.MustCompile(`^[a-z][a-z0-9_.-]*(?::[a-z0-9][a-z0-9_.-]*)+$`)
)

// Generator owns one immutable UUID namespace.
type Generator struct{ namespace uuid.UUID }

func New(namespace string) (Generator, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(namespace))
	if err != nil || parsed == uuid.Nil {
		return Generator{}, fmt.Errorf("%w: %q", ErrInvalidNamespace, namespace)
	}
	return Generator{namespace: parsed}, nil
}

// UUID returns UUID v5(namespace, key). Key normalization is deliberately
// rejected rather than applied silently because the key is a durable contract.
func (g Generator) UUID(key string) (uuid.UUID, error) {
	if g.namespace == uuid.Nil {
		return uuid.Nil, ErrInvalidNamespace
	}
	if key != strings.TrimSpace(key) || key != strings.ToLower(key) || !canonicalKey.MatchString(key) {
		return uuid.Nil, fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	return uuid.NewSHA1(g.namespace, []byte(key)), nil
}

func (g Generator) String(key string) (string, error) {
	id, err := g.UUID(key)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
