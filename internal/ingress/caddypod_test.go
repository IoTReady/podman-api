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
	created, err := c.ensureProxy(context.Background(), "h1")
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, []string{"podman-api-ingress"}, f.NetworkEnsureCalls["h1"])
	require.Len(t, f.PlayCalls, 1)
	require.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
	// Pod YAML must NOT contain route content (routes go via admin API).
	require.NotContains(t, f.PlayCalls[0].YAML, "reverse_proxy")
	// Pod YAML must enable admin on 0.0.0.0:2019.
	require.Contains(t, f.PlayCalls[0].YAML, "admin 0.0.0.0:2019")
	// Pod must expose the admin hostPort.
	require.Contains(t, f.PlayCalls[0].YAML, "hostPort: 2019")
}

func TestEnsureProxyNoopWhenPresent(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, store.NewMemory(), Config{Network: "n", CaddyImage: "img"})
	_, err := c.ensureProxy(context.Background(), "h1")
	require.NoError(t, err)
	plays := len(f.PlayCalls)
	created, err := c.ensureProxy(context.Background(), "h1")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, plays, len(f.PlayCalls), "no second play")
}

func TestCaddyPodYAMLShape(t *testing.T) {
	y := caddyPodYAML("docker.io/library/caddy:2", "ops@example.com")
	require.Contains(t, y, "name: "+caddyPodName)
	require.Contains(t, y, "image: docker.io/library/caddy:2")
	require.Contains(t, y, "hostPort: 80")
	require.Contains(t, y, "hostPort: 443")
	require.Contains(t, y, "hostPort: 2019")
	require.Contains(t, y, "containerPort: 2019")
	require.Contains(t, y, "claimName: "+caddyDataVolume)
	require.True(t, strings.Contains(y, "kind: Pod"))
	// Boot via seeding sh wrapper that writes seed only when absent.
	require.Contains(t, y, `command: ["sh", "-c",`)
	require.Contains(t, y, "name: CADDY_SEED")
	// Seed carries admin config and ACME email.
	require.Contains(t, y, "admin 0.0.0.0:2019")
	require.Contains(t, y, "ops@example.com")
	// Routes must NOT appear in the pod YAML.
	require.NotContains(t, y, "reverse_proxy")
}

func TestCaddySeedCaddyfileNoEmail(t *testing.T) {
	seed := caddySeedCaddyfile("")
	require.Contains(t, seed, "admin 0.0.0.0:2019")
	require.NotContains(t, seed, "email")
}

func TestCaddySeedCaddyfileWithEmail(t *testing.T) {
	seed := caddySeedCaddyfile("ops@example.com")
	require.Contains(t, seed, "admin 0.0.0.0:2019")
	require.Contains(t, seed, "email ops@example.com")
}
