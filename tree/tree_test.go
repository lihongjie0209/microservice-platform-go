package tree

import (
	"errors"
	"testing"
)

type item struct{ id, parent string }

func TestBuildAndFlatten(t *testing.T) {
	items := []item{{id: "root"}, {id: "a", parent: "root"}, {id: "b", parent: "root"}}
	forest, err := Build(items, func(v item) string { return v.id }, func(v item) (string, bool) { return v.parent, v.parent != "" })
	if err != nil {
		t.Fatal(err)
	}
	got := Flatten(forest)
	if len(forest) != 1 || len(forest[0].Children) != 2 || len(got) != 3 {
		t.Fatalf("forest=%+v flat=%+v", forest, got)
	}
}

func TestBuildRejectsInvalidTrees(t *testing.T) {
	tests := []struct {
		items []item
		want  error
	}{
		{[]item{{id: "a"}, {id: "a"}}, ErrDuplicateID},
		{[]item{{id: "a", parent: "missing"}}, ErrMissingParent},
		{[]item{{id: "a", parent: "b"}, {id: "b", parent: "a"}}, ErrCycle},
	}
	for _, tt := range tests {
		_, err := Build(tt.items, func(v item) string { return v.id }, func(v item) (string, bool) { return v.parent, v.parent != "" })
		if !errors.Is(err, tt.want) {
			t.Fatalf("Build() error=%v want=%v", err, tt.want)
		}
	}
}
