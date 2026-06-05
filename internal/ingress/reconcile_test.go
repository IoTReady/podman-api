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

	// The fresh Caddy pod is seeded with the rendered route via its boot env, so
	// the route rides in the played manifest (not the volume-import API).
	require.NotEmpty(t, f.PlayCalls)
	caddyYAML := f.PlayCalls[len(f.PlayCalls)-1].YAML
	require.Contains(t, caddyYAML, "blog.example.com")
	require.Contains(t, caddyYAML, "reverse_proxy web-blog:8080")
	require.Empty(t, f.ExecCalls, "fresh pod boots from the seeded config; no reload")
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
	// cp + exec must target the container name `podman kube play` actually
	// assigns ("<pod>-caddy"), not the bare spec name, or every steady-state
	// reconcile would hit a non-existent container.
	require.Equal(t, caddyContainer, last.Container)
	require.Contains(t, string(last.Content), "reverse_proxy web-blog:8080")
	require.GreaterOrEqual(t, len(f.ExecCalls), 2)
	require.Equal(t, caddyContainer, f.ExecCalls[len(f.ExecCalls)-1].Container)
	require.Contains(t, f.ExecCalls[len(f.ExecCalls)-2].Cmd, "validate")
	require.Contains(t, f.ExecCalls[len(f.ExecCalls)-1].Cmd, "reload")
}

func TestReconcileNoRoutesNoPodIsNoop(t *testing.T) {
	f := fake.New()
	// empty memStore -> zero routes; no Caddy pod exists yet
	c := NewCaddyController(f, &memStore{}, map[string]TemplateIngress{"web": {Port: 8080}},
		Config{Network: "n", CaddyImage: "img", ACMEEmail: "ops@example.com"})

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	require.Empty(t, f.PlayCalls, "no Caddy pod should be created when there are no routes")
	require.Empty(t, f.NetworkEnsureCalls["h1"], "no network should be ensured when there are no routes")
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
