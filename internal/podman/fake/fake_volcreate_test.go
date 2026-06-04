package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFake_VolumeCreate(t *testing.T) {
	f := New()
	ctx := context.Background()

	require.NoError(t, f.VolumeCreate(ctx, "h1", "vol"))
	v, err := f.VolumeInspect(ctx, "h1", "vol")
	require.NoError(t, err)
	assert.Equal(t, "vol", v.Name)

	// Idempotent: creating an existing volume is not an error.
	require.NoError(t, f.VolumeCreate(ctx, "h1", "vol"))
}
