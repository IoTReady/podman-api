//go:build integration

package podman

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func TestReal_Secrets_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx := context.Background()
	const name = "podman-api-itest-secret"

	t.Cleanup(func() { _ = c.SecretRemove(context.Background(), "local", name) })

	require.NoError(t, c.SecretCreate(ctx, "local", name, []byte("v1")))
	s, err := c.SecretInspect(ctx, "local", name)
	require.NoError(t, err)
	assert.Equal(t, name, s.Name)

	require.NoError(t, c.SecretRemove(ctx, "local", name))
	_, err = c.SecretInspect(ctx, "local", name)
	require.ErrorIs(t, err, ErrNotFound)
}
