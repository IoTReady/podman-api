package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// noIngressTemplate is a fixture that declares NO ingress.
func noIngressTemplate() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "db",
			Parameters: render.Parameters{Required: []string{"slug", "image"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: db-{{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
`,
		Source: "db.yaml",
	}
}

// recordingCtl is a test double Controller that records the hosts it reconciled.
type recordingCtl struct{ hosts []string }

func (r *recordingCtl) Reconcile(_ context.Context, host string) error {
	r.hosts = append(r.hosts, host)
	return nil
}

// webTemplate is a web-shaped fixture that declares ingress.
func webTemplate() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID: "web",
			Parameters: render.Parameters{
				Required: []string{"slug", "image"},
			},
			Ingress: &render.Ingress{Container: "web", Port: 8080},
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
    - name: web
      image: {{.image}}
`,
		Source: "web.yaml",
	}
}

func newWebSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc := NewService(f, hosts, []config.Template{webTemplate()})
	return svc, f
}

func webApply(slug string) ApplyRequest {
	return ApplyRequest{
		Template:   "web",
		Slug:       slug,
		Parameters: map[string]any{"slug": slug, "image": "docker.io/library/nginx:1"},
		Domains:    []string{"app.example.com"},
	}
}

func TestApplyRejectsDomainsWhenIngressDisabled(t *testing.T) {
	svc, _ := newWebSvc(t) // default Disabled controller, ingress not enabled
	err := svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ingress is disabled")
}

func TestApplyAttachesNetworkAndReconcilesWhenEnabled(t *testing.T) {
	svc, f := newWebSvc(t)
	rec := &recordingCtl{}
	svc.SetIngress(rec, "podman-api-ingress")

	require.NoError(t, svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true}))

	require.Len(t, f.PlayCalls, 1)
	assert.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
	// The ingress network must be ensured BEFORE the app pod joins it, or the
	// first deploy on a host fails ("network not found"). The fake now rejects a
	// play onto an un-ensured network, so a missing ensure would fail Apply above.
	assert.Contains(t, f.NetworkEnsureCalls["h1"], "podman-api-ingress")
	assert.Equal(t, []string{"h1"}, rec.hosts)
}

// A domain on a template that declares no ingress: must be rejected BEFORE the
// pod is played or the spec persisted — otherwise the spec poisons every later
// reconcile on the host.
func TestApplyRejectsDomainsOnNonIngressTemplate(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc := NewService(f, hosts, []config.Template{noIngressTemplate()})
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")

	req := ApplyRequest{
		Template:   "db",
		Slug:       "main",
		Parameters: map[string]any{"slug": "main", "image": "docker.io/library/postgres:16"},
		Domains:    []string{"db.example.com"},
	}
	err := svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no ingress")
	assert.Empty(t, f.PlayCalls, "no pod should be played for a rejected request")
}

// A domain already claimed by another instance on the host must be rejected
// pre-mutation, not discovered only at reconcile time.
func TestApplyRejectsDuplicateDomainAcrossInstances(t *testing.T) {
	svc, f := newWebSvc(t)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	st := store.NewMemory()
	svc.SetStore(st)
	require.NoError(t, st.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "web", Slug: "other", Domains: []string{"app.example.com"},
	}))

	// webApply("demo") also claims app.example.com -> conflicts with web/other.
	err := svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already claimed")
	assert.Empty(t, f.PlayCalls, "no pod should be played for a rejected request")
}
