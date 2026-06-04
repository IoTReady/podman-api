package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// secretAndPortTemplate declares both a per-host secret and a fixed hostPort, so
// a single destination can surface two distinct preflight issues at once.
func secretAndPortTemplate() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "needs-both",
			Parameters: render.Parameters{Required: []string{"slug", "image"}},
			Secrets:    render.Secrets{PerHostReferenced: []string{"shared-token"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: needs-both-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
      ports:
        - hostPort: 9090
          containerPort: 80
`,
		Source: "needs-both.yaml",
	}
}

func TestPreflightIssues_CollectsAll(t *testing.T) {
	ctx := context.Background()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	f := fake.New()
	mem := store.NewMemory()
	svc := NewService(f, hosts, []config.Template{secretAndPortTemplate()})
	svc.SetStore(mem)
	// Occupy port 9090 on the destination so the port check fails; leave the
	// per-host secret "shared-token" absent so the secret check also fails.
	f.AddPod("h2", podman.Pod{Name: "occupier", Status: "Running",
		Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 9090}}}}})

	tmpl := secretAndPortTemplate()
	eff := map[string]any{"slug": "x", "image": "img"}
	errs := svc.preflightIssues(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-both", Slug: "x"}, tmpl, eff)

	require.Len(t, errs, 2, "expected both the missing-secret and port-conflict issues")
	var sawSecret, sawPort bool
	for _, e := range errs {
		if errors.Is(e, ErrHostSecretMissing) {
			sawSecret = true
		}
		if errors.Is(e, ErrPortConflict) {
			sawPort = true
		}
	}
	assert.True(t, sawSecret, "missing per-host secret not reported")
	assert.True(t, sawPort, "port conflict not reported")
}
