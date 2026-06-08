package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// specReconcileSvc builds a Service with one host (h1) and a "web" template
// that declares one per-instance secret, backed by a fake client and memory
// store. The template body includes a {{.slug}} parameter so we can verify
// the rendered YAML is correct.
func specReconcileSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	webTmpl := store.Template{
		Meta: render.Meta{
			ID:         "web",
			Parameters: requiredParams("slug"),
			Secrets:    render.Secrets{PerInstance: []string{"password"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: web-{{.slug}}
  labels:
    podman-api/template: web
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: app
      image: nginx:latest
`,
		Origin: "seed",
	}
	svc, st := newSvcWith(t, fc, hosts, webTmpl)
	return svc, fc, st
}

// seedBootSpec writes a stored spec for (host, tmpl, slug) into the memory store
// with the given optional secrets and a slug parameter matching the name.
func seedBootSpec(t *testing.T, st *store.Memory, host, tmpl, slug string, secrets map[string]string) {
	t.Helper()
	var sec map[string]string
	if secrets != nil {
		sec = secrets
	}
	err := st.PutSpec(context.Background(), store.Spec{
		Host:       host,
		Template:   tmpl,
		Slug:       slug,
		Parameters: map[string]any{"slug": slug},
		Secrets:    sec,
	})
	require.NoError(t, err)
}

// fakeIngressController is a test double for ingress.Controller that
// records every Reconcile call.
type fakeIngressController struct {
	reconcileCalls int
	lastHost       string
}

func (f *fakeIngressController) Reconcile(_ context.Context, host string) error {
	f.reconcileCalls++
	f.lastHost = host
	return nil
}

func TestReconcileSpecsOnHost_AlreadyRunning(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed a spec and pre-create the pod.
	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	fc.AddPod("h1", podman.Pod{Name: "web-my-app", Status: "Running",
		Labels: map[string]string{"podman-api/template": "web", "podman-api/slug": "my-app"}})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — the pod was already running.
	assert.Empty(t, fc.PlayCalls, "should not re-play an already-running pod")
}

func TestReconcileSpecsOnHost_MissingPod(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should have been called once with the correct pod name.
	require.Len(t, fc.PlayCalls, 1, "should re-play a missing pod")
	assert.Equal(t, "h1", fc.PlayCalls[0].Host)
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-my-app")
	assert.Contains(t, fc.PlayCalls[0].YAML, "podman-api/slug: my-app")
	assert.False(t, fc.PlayCalls[0].Replace, "replace should be false for boot converge")
}

func TestReconcileSpecsOnHost_WithSecrets(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", map[string]string{"password": "s3cret"})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should have been called.
	require.Len(t, fc.PlayCalls, 1, "should re-play a missing pod with secrets")

	// The secret should have been created on the host.
	secrets, err := fc.SecretList(ctx, "h1")
	require.NoError(t, err)
	found := false
	for _, s := range secrets {
		if s.Name == "web-my-app-password" {
			found = true
			break
		}
	}
	assert.True(t, found, "per-instance secret should exist on host after boot converge")
}

func TestReconcileSpecsOnHost_TemplateDeleted(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed a spec, then delete the template from the store.
	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	require.NoError(t, st.DeleteTemplate(ctx, "web"))

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — template is gone.
	assert.Empty(t, fc.PlayCalls, "should not re-play when template is deleted")
}

func TestReconcileSpecsOnHost_HostUnreachable(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	// Make PodInspect return an error (simulates unreachable host).
	fc.PodInspectErr = assert.AnError

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — host was unreachable.
	assert.Empty(t, fc.PlayCalls, "should not re-play when host is unreachable")
}

func TestReconcileSpecsOnHost_NoSpecs(t *testing.T) {
	svc, fc, _ := specReconcileSvc(t)
	ctx := context.Background()

	// No specs seeded — nothing to reconcile.
	svc.ReconcileSpecsOnHost(ctx, "h1")

	assert.Empty(t, fc.PlayCalls, "should not play when there are no specs")
}

func TestReconcileSpecsOnHost_MultipleInstances(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed two specs: one already running, one missing.
	seedBootSpec(t, st, "h1", "web", "app-a", nil)
	seedBootSpec(t, st, "h1", "web", "app-b", nil)
	fc.AddPod("h1", podman.Pod{Name: "web-app-a", Status: "Running",
		Labels: map[string]string{"podman-api/template": "web", "podman-api/slug": "app-a"}})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// Only the missing pod should be re-played.
	require.Len(t, fc.PlayCalls, 1, "should re-play only the missing pod")
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-app-b")
}

func TestReconcileSpecsOnHost_NonRunningPod(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed a spec where the pod exists but is not running (e.g. Exited).
	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	fc.AddPod("h1", podman.Pod{Name: "web-my-app", Status: "Exited",
		Labels: map[string]string{"podman-api/template": "web", "podman-api/slug": "my-app"}})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// Should re-play because the pod is not running, with replace=true so
	// podman replaces the stale pod rather than failing with "pod exists".
	require.Len(t, fc.PlayCalls, 1, "should re-play a non-running pod")
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-my-app")
	assert.True(t, fc.PlayCalls[0].Replace, "should use replace=true for a non-running pod")
}

// errStore wraps a store.Memory and injects errors on GetSpec for a specific
// (template, slug) pair.
type errStore struct {
	*store.Memory
	corruptTmpl string
	corruptSlug string
}

func (e *errStore) GetSpec(ctx context.Context, host, template, slug string) (store.Spec, error) {
	if template == e.corruptTmpl && slug == e.corruptSlug {
		return store.Spec{}, store.ErrSpecCorrupt
	}
	return e.Memory.GetSpec(ctx, host, template, slug)
}

// listErrStore wraps a store.Memory and fails ListSpecKeys.
type listErrStore struct {
	*store.Memory
}

func (l *listErrStore) ListSpecKeys(_ context.Context, _ string) ([]store.SpecKey, error) {
	return nil, assert.AnError
}

func TestReconcileSpecsOnHost_SpecCorrupt(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Replace the store with one that returns ErrSpecCorrupt for "web/my-app".
	es := &errStore{Memory: st, corruptTmpl: "web", corruptSlug: "my-app"}
	svc.SetStore(es)

	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — spec was corrupt.
	assert.Empty(t, fc.PlayCalls, "should not re-play when spec is corrupt")
}

func TestReconcileSpecsOnHost_StoreListError(t *testing.T) {
	fc := fake.New()
	ctx := context.Background()

	// Service with no templates — ListSpecKeys on the error store returns error
	// before any template lookup is needed.
	svc := NewService(fc, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	svc.SetStore(&listErrStore{Memory: store.NewMemory()})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// Should not error or panic; just log and return.
	assert.Empty(t, fc.PlayCalls, "should not play when store list fails")
}

func TestReconcileSpecsOnHost_IngressEnabled(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Enable ingress on the service.
	ingCtl := &fakeIngressController{}
	svc.SetIngress(ingCtl, "test-net")

	// Seed a spec with a domain so ingress would have routes to manage.
	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should have been called.
	require.Len(t, fc.PlayCalls, 1)
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-my-app")

	// Ingress should have been reconciled exactly once.
	assert.Equal(t, 1, ingCtl.reconcileCalls)
	assert.Equal(t, "h1", ingCtl.lastHost)
}
