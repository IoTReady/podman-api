package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestFake_PlayKubeAndInspect(t *testing.T) {
	f := New()
	ctx := context.Background()

	yaml := `
apiVersion: v1
kind: Pod
metadata:
  name: example-x
  labels:
    podman-api/template: example
spec:
  containers:
    - name: app
      image: x:latest
`
	require.NoError(t, f.PlayKube(ctx, "h1", yaml, false))

	p, err := f.PodInspect(ctx, "h1", "example-x")
	require.NoError(t, err)
	assert.Equal(t, "example-x", p.Name)
	assert.Equal(t, "Running", p.Status)
	require.Len(t, p.Containers, 1)
	assert.Equal(t, "app", p.Containers[0].Name)
}

func TestFake_PodNotFound(t *testing.T) {
	f := New()
	_, err := f.PodInspect(context.Background(), "h1", "nope")
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_PodLifecycle(t *testing.T) {
	f := New()
	ctx := context.Background()
	yaml := `
apiVersion: v1
kind: Pod
metadata: {name: p1}
spec:
  containers: [{name: c1, image: x}]
`
	require.NoError(t, f.PlayKube(ctx, "h", yaml, false))

	require.NoError(t, f.PodStop(ctx, "h", "p1"))
	p, _ := f.PodInspect(ctx, "h", "p1")
	assert.Equal(t, "Exited", p.Status)

	require.NoError(t, f.PodStart(ctx, "h", "p1"))
	p, _ = f.PodInspect(ctx, "h", "p1")
	assert.Equal(t, "Running", p.Status)

	require.NoError(t, f.PodRemove(ctx, "h", "p1", false))
	_, err := f.PodInspect(ctx, "h", "p1")
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_Secrets(t *testing.T) {
	f := New()
	ctx := context.Background()
	require.NoError(t, f.SecretCreate(ctx, "h", "s", []byte("v")))
	s, err := f.SecretInspect(ctx, "h", "s")
	require.NoError(t, err)
	assert.Equal(t, "s", s.Name)

	_, err = f.SecretInspect(ctx, "h", "missing")
	require.ErrorIs(t, err, podman.ErrNotFound)
}
