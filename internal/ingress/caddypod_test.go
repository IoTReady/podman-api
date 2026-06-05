package ingress

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func TestEnsureProxyCreatesWhenAbsent(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, store.NewMemory(), Config{
		Network: "podman-api-ingress", CaddyImage: "docker.io/library/caddy:2", ACMEEmail: "ops@example.com",
	})
	created, err := c.ensureProxy(context.Background(), "h1", "{\n\temail ops@example.com\n}\n\n")
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, []string{"podman-api-ingress"}, f.NetworkEnsureCalls["h1"])
	require.Len(t, f.PlayCalls, 1)
	require.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
	// The initial Caddyfile is seeded through the pod's boot env (CADDY_SEED),
	// not the volume-import API, so it rides in the played manifest.
	require.Contains(t, f.PlayCalls[0].YAML, "CADDY_SEED")
	require.Contains(t, f.PlayCalls[0].YAML, "email ops@example.com")
}

func TestEnsureProxyNoopWhenPresent(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, store.NewMemory(), Config{Network: "n", CaddyImage: "img"})
	_, err := c.ensureProxy(context.Background(), "h1", "{\n}\n\n")
	require.NoError(t, err)
	plays := len(f.PlayCalls)
	created, err := c.ensureProxy(context.Background(), "h1", "{\n}\n\n")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, plays, len(f.PlayCalls), "no second play")
}

func TestCaddyPodYAMLShape(t *testing.T) {
	y := caddyPodYAML("docker.io/library/caddy:2", "demo.example.com {\n\treverse_proxy web-x:8080\n}\n")
	require.Contains(t, y, "name: "+caddyPodName)
	require.Contains(t, y, "image: docker.io/library/caddy:2")
	require.Contains(t, y, "hostPort: 80")
	require.Contains(t, y, "hostPort: 443")
	require.Contains(t, y, "claimName: "+caddyConfigVolume)
	require.Contains(t, y, "claimName: "+caddyDataVolume)
	require.True(t, strings.Contains(y, "kind: Pod"))
	// Boots via a seeding sh wrapper + the caddyfile in CADDY_SEED (no volume
	// import), and only writes the seed when the file is absent so a reload survives.
	require.Contains(t, y, `command: ["sh", "-c",`)
	require.Contains(t, y, "name: CADDY_SEED")
	require.Contains(t, y, "demo.example.com")
	require.Contains(t, y, "exec caddy run --config "+caddyConfigDir+"/"+caddyConfigFile)
}
