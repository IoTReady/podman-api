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

// portTemplate is a fixture whose rendered Pod declares a fixed hostPort, used
// to exercise the dest port-conflict preflight.
func portTemplate() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "web",
			Parameters: render.Parameters{Required: []string{"slug", "image"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: web-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
      ports:
        - hostPort: 8080
          containerPort: 80
`,
		Source: "web.yaml",
	}
}

func newMigrateSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x"},
		{ID: "h2", Addr: "unix", Socket: "/y"},
		{ID: "draining", Addr: "unix", Socket: "/z", Drain: true},
	}
	f := fake.New()
	mem := store.NewMemory()
	svc := NewService(f, hosts, []config.Template{pgTemplate(), portTemplate()})
	svc.SetStore(mem)
	return svc, f, mem
}

func seedSpec(t *testing.T, mem *store.Memory, host, tmpl, slug string, params map[string]any) {
	t.Helper()
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: host, Template: tmpl, Slug: slug, Parameters: params,
	}))
}

func TestCheckMigratable_Errors(t *testing.T) {
	svc, _, mem := newMigrateSvc(t)
	ctx := context.Background()
	seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})

	err := svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h1", Template: "postgres", Slug: "db1"})
	require.ErrorIs(t, err, ErrSameHost)
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "nope", ToHost: "h2", Template: "postgres", Slug: "db1"})
	require.ErrorIs(t, err, ErrUnknownHost)
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "nope", Template: "postgres", Slug: "db1"})
	require.ErrorIs(t, err, ErrUnknownHost)
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "ghost", Slug: "db1"})
	require.ErrorIs(t, err, ErrUnknownTemplate)
	err = svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "absent"})
	require.ErrorIs(t, err, store.ErrNotFound)
	require.NoError(t, svc.CheckMigratable(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}))
}

func TestMigrate_PreflightFailFast_SourceUntouched(t *testing.T) {
	ctx := context.Background()

	t.Run("dest draining", func(t *testing.T) {
		svc, f, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
		f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "draining", Template: "postgres", Slug: "db1"}, nil)
		require.ErrorIs(t, err, ErrHostDraining)
		p, _ := f.PodInspect(ctx, "h1", "postgres-db1")
		assert.Equal(t, "Running", p.Status)
	})

	t.Run("dest already has instance", func(t *testing.T) {
		svc, f, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
		f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
		f.AddPod("h2", podman.Pod{Name: "postgres-db1", Status: "Running"})
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
		require.ErrorIs(t, err, ErrInstanceExists)
		p, _ := f.PodInspect(ctx, "h1", "postgres-db1")
		assert.Equal(t, "Running", p.Status)
	})

	t.Run("port conflict on dest", func(t *testing.T) {
		svc, f, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "web", "w1", map[string]any{"slug": "w1", "image": "x"})
		f.AddPod("h1", podman.Pod{Name: "web-w1", Status: "Running"})
		f.AddPod("h2", podman.Pod{Name: "other", Status: "Running",
			Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 8080}}}}})
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "web", Slug: "w1"}, nil)
		require.ErrorIs(t, err, ErrPortConflict)
		p, _ := f.PodInspect(ctx, "h1", "web-w1")
		assert.Equal(t, "Running", p.Status)
	})

	t.Run("missing per-host secret on dest", func(t *testing.T) {
		hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
		f := fake.New()
		mem := store.NewMemory()
		svc := NewService(f, hosts, []config.Template{templateWithHostSecret()})
		svc.SetStore(mem)
		seedSpec(t, mem, "h1", "needs-host-secret", "s1", map[string]any{"slug": "s1", "image": "x"})
		f.AddPod("h1", podman.Pod{Name: "needs-host-secret-s1", Status: "Running"})
		err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}, nil)
		require.ErrorIs(t, err, ErrHostSecretMissing)
		p, _ := f.PodInspect(ctx, "h1", "needs-host-secret-s1")
		assert.Equal(t, "Running", p.Status)
	})
}

func TestMigrate_SelfValidates(t *testing.T) {
	svc, _, mem := newMigrateSvc(t)
	ctx := context.Background()
	seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
	err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h1", Template: "postgres", Slug: "db1"}, nil)
	require.ErrorIs(t, err, ErrSameHost)
	err = svc.Migrate(ctx, MigrateRequest{FromHost: "nope", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
	require.ErrorIs(t, err, ErrUnknownHost)
}
