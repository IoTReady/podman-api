package ingress

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman/fake"
)

func TestEnsureProxyCreatesWhenAbsent(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, &memStore{}, nil, Config{
		Network: "podman-api-ingress", CaddyImage: "docker.io/library/caddy:2", ACMEEmail: "ops@example.com",
	})
	created, err := c.ensureProxy(context.Background(), "h1", "{\n\temail ops@example.com\n}\n\n")
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, []string{"podman-api-ingress"}, f.NetworkEnsureCalls["h1"])
	require.Len(t, f.PlayCalls, 1)
	require.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
	require.Contains(t, string(f.VolumeData("h1", caddyConfigVolume)), "email ops@example.com")
}

func TestEnsureProxyNoopWhenPresent(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, &memStore{}, nil, Config{Network: "n", CaddyImage: "img"})
	_, err := c.ensureProxy(context.Background(), "h1", "{\n}\n\n")
	require.NoError(t, err)
	plays := len(f.PlayCalls)
	created, err := c.ensureProxy(context.Background(), "h1", "{\n}\n\n")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, plays, len(f.PlayCalls), "no second play")
}

func TestCaddyPodYAMLShape(t *testing.T) {
	y := caddyPodYAML("docker.io/library/caddy:2")
	require.Contains(t, y, "name: "+caddyPodName)
	require.Contains(t, y, "image: docker.io/library/caddy:2")
	require.Contains(t, y, "hostPort: 80")
	require.Contains(t, y, "hostPort: 443")
	require.Contains(t, y, "claimName: "+caddyConfigVolume)
	require.Contains(t, y, "claimName: "+caddyDataVolume)
	require.True(t, strings.Contains(y, "kind: Pod"))
}
