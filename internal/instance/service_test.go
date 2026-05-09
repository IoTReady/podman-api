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
)

func newSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	tmpl := config.Template{
		Meta: render.Meta{
			ID: "lite-engine",
			Parameters: render.Parameters{
				Required: []string{"slug", "image", "port", "base_url", "app_template", "s3_bucket", "s3_endpoint"},
			},
			Secrets: render.Secrets{
				PerInstance:       []string{"auth_secret"},
				PerHostReferenced: []string{"s3-access-key-id", "s3-secret-access-key"},
			},
			Volumes: []render.Volume{{Name: "data", Backup: "litestream"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: lite-engine-{{.slug}}
  labels:
    podman-api/template: lite-engine
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
`,
		Source: "lite-engine.yaml",
	}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	require.NoError(t, f.SecretCreate(context.Background(), "h1", "s3-access-key-id", []byte("k")))
	require.NoError(t, f.SecretCreate(context.Background(), "h1", "s3-secret-access-key", []byte("s")))
	svc := NewService(f, hosts, []config.Template{tmpl})
	return svc, f
}

func TestService_Apply_Then_Get(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	req := ApplyRequest{
		Template: "lite-engine",
		Slug:     "iotready",
		Parameters: map[string]any{
			"slug": "iotready", "image": "x:1", "port": 31001,
			"base_url": "https://x", "app_template": "crm",
			"s3_bucket": "b", "s3_endpoint": "https://s3",
		},
		Secrets: map[string]string{"auth_secret": "v"},
	}
	require.NoError(t, svc.Apply(ctx, "h1", req, true))

	obs, err := svc.Get(ctx, "h1", "lite-engine", "iotready")
	require.NoError(t, err)
	assert.Equal(t, "lite-engine", obs.Template)
	assert.Equal(t, "iotready", obs.Slug)
	assert.Equal(t, "Running", obs.Pod.Status)
}

func TestService_Apply_RequiresHostSecret(t *testing.T) {
	svc, f := newSvc(t)
	require.NoError(t, f.SecretRemove(context.Background(), "h1", "s3-access-key-id"))

	err := svc.Apply(context.Background(), "h1", ApplyRequest{
		Template: "lite-engine",
		Slug:     "x",
		Parameters: map[string]any{
			"slug": "x", "image": "x:1", "port": 1,
			"base_url": "x", "app_template": "crm", "s3_bucket": "b", "s3_endpoint": "e",
		},
		Secrets: map[string]string{"auth_secret": "v"},
	}, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrHostSecretMissing)
}

func TestService_UnknownTemplate(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "h1", ApplyRequest{Template: "nope", Slug: "x"}, false)
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestService_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "nope", ApplyRequest{Template: "lite-engine", Slug: "x"}, false)
	require.ErrorIs(t, err, ErrUnknownHost)
}

func TestService_Lifecycle(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	req := ApplyRequest{
		Template: "lite-engine", Slug: "iotready",
		Parameters: map[string]any{
			"slug": "iotready", "image": "x:1", "port": 1,
			"base_url": "x", "app_template": "crm", "s3_bucket": "b", "s3_endpoint": "e",
		},
		Secrets: map[string]string{"auth_secret": "v"},
	}
	require.NoError(t, svc.Apply(ctx, "h1", req, true))

	require.NoError(t, svc.Stop(ctx, "h1", "lite-engine", "iotready"))
	obs, _ := svc.Get(ctx, "h1", "lite-engine", "iotready")
	assert.Equal(t, "Exited", obs.Pod.Status)

	require.NoError(t, svc.Start(ctx, "h1", "lite-engine", "iotready"))
	obs, _ = svc.Get(ctx, "h1", "lite-engine", "iotready")
	assert.Equal(t, "Running", obs.Pod.Status)

	require.NoError(t, svc.Delete(ctx, "h1", "lite-engine", "iotready", DeleteOptions{}))
	_, err := svc.Get(ctx, "h1", "lite-engine", "iotready")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

// podman is imported but used only to satisfy reference in tests above.
var _ = podman.ErrNotFound
