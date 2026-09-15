package stableid

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const testNamespace = "91d574b2-277f-4bc7-9eec-2bd14b633f21"

func TestGeneratorIsStable(t *testing.T) {
	generator, err := New(testNamespace)
	require.NoError(t, err)
	first, err := generator.String("permission:platform.user.update")
	require.NoError(t, err)
	second, err := generator.String("permission:platform.user.update")
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, uuid.Version(5), uuid.MustParse(first).Version())
}

func TestGeneratorRejectsUnstableKeys(t *testing.T) {
	generator, err := New(testNamespace)
	require.NoError(t, err)
	for _, key := range []string{"Permission:platform.user.update", " permission:platform.user.update", "display-name", "menu:系统"} {
		t.Run(key, func(t *testing.T) { _, err := generator.String(key); require.ErrorIs(t, err, ErrInvalidKey) })
	}
}

func TestGeneratorGoldenMapping(t *testing.T) {
	generator, err := New(testNamespace)
	require.NoError(t, err)
	id, err := generator.String("menu:system:user-management")
	require.NoError(t, err)
	require.Equal(t, "f434e0f7-62b6-5ba0-9dde-1fd8db71943b", id)
}
