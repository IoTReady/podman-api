package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
)

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
	assert.Equal(t, []string{"h1"}, rec.hosts)
}
