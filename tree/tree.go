// Package tree provides deterministic, validated tree construction for flat records.
package tree

import "errors"

var (
	ErrDuplicateID   = errors.New("tree contains duplicate id")
	ErrMissingParent = errors.New("tree contains missing parent")
	ErrCycle         = errors.New("tree contains cycle")
)

// Node keeps the original value and its ordered children.
type Node[T any] struct {
	Value    T          `json:"value"`
	Children []*Node[T] `json:"children"`
}

// Build converts an already ordered flat slice into a forest. A false parent
// result marks a root. Missing parents, duplicate IDs, and cycles are rejected.
func Build[T any, K comparable](items []T, id func(T) K, parent func(T) (K, bool)) ([]*Node[T], error) {
	nodes := make(map[K]*Node[T], len(items))
	for _, item := range items {
		key := id(item)
		if _, exists := nodes[key]; exists {
			return nil, ErrDuplicateID
		}
		nodes[key] = &Node[T]{Value: item, Children: make([]*Node[T], 0)}
	}
	roots := make([]*Node[T], 0)
	for _, item := range items {
		key := id(item)
		parentKey, hasParent := parent(item)
		if !hasParent {
			roots = append(roots, nodes[key])
			continue
		}
		parentNode, exists := nodes[parentKey]
		if !exists {
			return nil, ErrMissingParent
		}
		parentNode.Children = append(parentNode.Children, nodes[key])
	}
	if hasCycle(items, id, parent) {
		return nil, ErrCycle
	}
	return roots, nil
}

func hasCycle[T any, K comparable](items []T, id func(T) K, parent func(T) (K, bool)) bool {
	parents := make(map[K]K, len(items))
	for _, item := range items {
		if parentID, ok := parent(item); ok {
			parents[id(item)] = parentID
		}
	}
	for _, item := range items {
		seen := make(map[K]struct{})
		current := id(item)
		for {
			if _, exists := seen[current]; exists {
				return true
			}
			seen[current] = struct{}{}
			next, exists := parents[current]
			if !exists {
				break
			}
			current = next
		}
	}
	return false
}

// Walk visits nodes depth-first in stable input/sibling order.
func Walk[T any](forest []*Node[T], visit func(T) bool) {
	for _, node := range forest {
		if !visit(node.Value) {
			return
		}
		Walk(node.Children, visit)
	}
}

// Flatten returns values in depth-first order.
func Flatten[T any](forest []*Node[T]) []T {
	values := make([]T, 0)
	Walk(forest, func(value T) bool {
		values = append(values, value)
		return true
	})
	return values
}
