//go:build integration

package podman

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

const podsTestYAML = `
apiVersion: v1
kind: Pod
metadata:
  name: podman-api-itest
  labels:
    podman-api/itest: "true"
spec:
  containers:
    - name: c
      image: docker.io/library/alpine:latest
      command: ["sleep", "60"]
`

func TestReal_PlayKubeAndLifecycle_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		_ = c.PodRemove(context.Background(), "local", "podman-api-itest", true)
	})

	require.NoError(t, c.PlayKube(ctx, "local", podsTestYAML, true))

	p, err := c.PodInspect(ctx, "local", "podman-api-itest")
	require.NoError(t, err)
	assert.Equal(t, "podman-api-itest", p.Name)

	require.NoError(t, c.PodStop(ctx, "local", "podman-api-itest"))
	require.NoError(t, c.PodStart(ctx, "local", "podman-api-itest"))

	require.NoError(t, c.PodRemove(ctx, "local", "podman-api-itest", true))
	_, err = c.PodInspect(ctx, "local", "podman-api-itest")
	require.ErrorIs(t, err, ErrNotFound)
}
