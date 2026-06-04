package ingress

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func webSpecStore() *memStore {
	return &memStore{specs: []store.Spec{
		{Host: "h1", Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}},
	}}
}

func TestReconcileFreshHostSeedsAndPlays(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, webSpecStore(), map[string]TemplateIngress{"web": {Port: 8080}},
		Config{Network: "podman-api-ingress", CaddyImage: "docker.io/library/caddy:2", ACMEEmail: "ops@example.com"})

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	require.Contains(t, string(f.VolumeData("h1", caddyConfigVolume)), "blog.example.com")
	require.Contains(t, string(f.VolumeData("h1", caddyConfigVolume)), "reverse_proxy web-blog:8080")
	require.Empty(t, f.ExecCalls, "fresh pod boots from the seeded volume; no reload")
}

func TestReconcileExistingHostCopiesAndReloads(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, webSpecStore(), map[string]TemplateIngress{"web": {Port: 8080}},
		Config{Network: "n", CaddyImage: "img", ACMEEmail: "ops@example.com"})
	require.NoError(t, c.Reconcile(context.Background(), "h1")) // creates the pod

	require.NoError(t, c.Reconcile(context.Background(), "h1")) // pod exists -> cp + reload

	require.NotEmpty(t, f.CopyCalls)
	last := f.CopyCalls[len(f.CopyCalls)-1]
	require.Equal(t, caddyConfigDir, last.DestDir)
	require.Equal(t, caddyConfigFile, last.Name)
	require.Contains(t, string(last.Content), "reverse_proxy web-blog:8080")
	require.GreaterOrEqual(t, len(f.ExecCalls), 2)
	require.Contains(t, f.ExecCalls[len(f.ExecCalls)-2].Cmd, "validate")
	require.Contains(t, f.ExecCalls[len(f.ExecCalls)-1].Cmd, "reload")
}

func TestReconcileFailsWhenValidateFails(t *testing.T) {
	f := fake.New()
	f.ExecFunc = func(_, _ string, cmd []string) (podman.ExecResult, error) {
		for _, a := range cmd {
			if a == "validate" {
				return podman.ExecResult{ExitCode: 1, Output: "adapt: bad config"}, nil
			}
		}
		return podman.ExecResult{}, nil
	}
	c := NewCaddyController(f, webSpecStore(), map[string]TemplateIngress{"web": {Port: 8080}},
		Config{Network: "n", CaddyImage: "img"})
	require.NoError(t, c.Reconcile(context.Background(), "h1")) // create
	err := c.Reconcile(context.Background(), "h1")              // cp + failing validate
	require.Error(t, err)
	require.Contains(t, err.Error(), "validate")
}
