package instance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

var migrateFailErr = errors.New("injected failure")

// setVerifyKnobs temporarily shrinks the verify-poll timing for tests.
func setVerifyKnobs(timeout, interval time.Duration) func() {
	ot, oi := verifyTimeout, verifyInterval
	verifyTimeout, verifyInterval = timeout, interval
	return func() { verifyTimeout, verifyInterval = ot, oi }
}

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

func TestPodReady(t *testing.T) {
	tests := []struct {
		name string
		pod  podman.Pod
		want bool
	}{
		{"pod not running", podman.Pod{Status: "Exited", Containers: []podman.Container{{Status: "Running"}}}, false},
		{"container not running", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Exited"}}}, false},
		{"no healthcheck, all running", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running"}}}, true},
		{"healthcheck healthy", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "healthy"}}}, true},
		{"healthcheck unhealthy", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "unhealthy"}}}, false},
		{"healthcheck still starting", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "starting"}}}, false},
		{"mixed declared and undeclared", podman.Pod{Status: "Running", Containers: []podman.Container{{Status: "Running", Health: "healthy"}, {Status: "Running"}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podReady(tt.pod); got != tt.want {
				t.Fatalf("podReady = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWaitRunning_HealthGate(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	svc, f, _ := newMigrateSvc(t)
	ctx := context.Background()

	t.Run("ready when declared healthcheck is healthy", func(t *testing.T) {
		f.AddPod("h2", podman.Pod{Name: "web-ok", Status: "Running",
			Containers: []podman.Container{{Status: "Running", Health: "healthy"}}})
		require.NoError(t, svc.waitRunning(ctx, "h2", "web", "ok"))
	})

	t.Run("times out while a healthcheck stays unhealthy", func(t *testing.T) {
		f.AddPod("h2", podman.Pod{Name: "web-bad", Status: "Running",
			Containers: []podman.Container{{Status: "Running", Health: "unhealthy"}}})
		err := svc.waitRunning(ctx, "h2", "web", "bad")
		require.Error(t, err)
		assert.ErrorContains(t, err, "not running")
	})
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

func TestMigrate_HappyPath(t *testing.T) {
	svc, f, mem := newMigrateSvc(t)
	ctx := context.Background()
	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}

	require.NoError(t, mem.PutSpec(ctx, store.Spec{
		Host: "h1", Template: "postgres", Slug: "db1",
		Parameters: params, Secrets: map[string]string{"password": "p"},
	}))
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
	srcTar := tarBytes(t, map[string]string{"PG_VERSION": "16"})
	f.SetVolumeData("h1", "postgres-db1-data", srcTar)

	var steps []string
	err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"},
		func(s, _ string) { steps = append(steps, s) })
	require.NoError(t, err)

	// Source gone.
	_, err = f.PodInspect(ctx, "h1", "postgres-db1")
	require.ErrorIs(t, err, podman.ErrNotFound)
	_, err = mem.GetSpec(ctx, "h1", "postgres", "db1")
	require.ErrorIs(t, err, store.ErrNotFound)
	assert.Nil(t, f.VolumeData("h1", "postgres-db1-data"))

	// Dest running with copied volume bytes + stored spec.
	p, err := f.PodInspect(ctx, "h2", "postgres-db1")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)
	assert.Equal(t, srcTar, f.VolumeData("h2", "postgres-db1-data"))
	_, err = mem.GetSpec(ctx, "h2", "postgres", "db1")
	require.NoError(t, err)

	assert.Equal(t, []string{"load", "preflight", "stop-source", "copy-volume", "verify-volume", "apply-dest", "verify", "commit"}, steps)
}

func TestMigrate_Rollback(t *testing.T) {
	ctx := context.Background()
	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}

	// seed: a running source instance with spec(+secret), pod, and a data volume.
	seed := func(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
		svc, f, mem := newMigrateSvc(t)
		require.NoError(t, mem.PutSpec(ctx, store.Spec{
			Host: "h1", Template: "postgres", Slug: "db1",
			Parameters: params, Secrets: map[string]string{"password": "p"},
		}))
		f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
		f.SetVolumeData("h1", "postgres-db1-data", tarBytes(t, map[string]string{"PG_VERSION": "16"}))
		return svc, f, mem
	}

	// assertRolledBack: source restored & intact, dest fully reaped.
	assertRolledBack := func(t *testing.T, f *fake.Fake, mem *store.Memory) {
		t.Helper()
		p, err := f.PodInspect(ctx, "h1", "postgres-db1")
		require.NoError(t, err)
		assert.Equal(t, "Running", p.Status) // source restarted
		_, err = mem.GetSpec(ctx, "h1", "postgres", "db1")
		require.NoError(t, err) // source spec intact
		_, err = f.PodInspect(ctx, "h2", "postgres-db1")
		require.ErrorIs(t, err, podman.ErrNotFound) // dest pod reaped
		_, err = mem.GetSpec(ctx, "h2", "postgres", "db1")
		require.ErrorIs(t, err, store.ErrNotFound)             // dest spec reaped
		assert.Nil(t, f.VolumeData("h2", "postgres-db1-data")) // dest volume reaped
	}

	req := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}

	t.Run("copy fails", func(t *testing.T) {
		svc, f, mem := seed(t)
		f.ImportErr = migrateFailErr
		err := svc.Migrate(ctx, req, nil)
		require.Error(t, err)
		assertRolledBack(t, f, mem)
	})

	t.Run("apply fails", func(t *testing.T) {
		svc, f, mem := seed(t)
		f.PlayKubeErr = migrateFailErr
		err := svc.Migrate(ctx, req, nil)
		require.Error(t, err)
		assertRolledBack(t, f, mem)
	})

	t.Run("verify fails", func(t *testing.T) {
		svc, f, mem := seed(t)
		f.PlayKubePodStatus = "Exited" // dest pod never reaches Running
		restore := setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)
		defer restore()
		err := svc.Migrate(ctx, req, nil)
		require.Error(t, err)
		assertRolledBack(t, f, mem)
	})

	t.Run("verify fails when a container is not running", func(t *testing.T) {
		svc, f, mem := seed(t)
		f.PlayKubeContainerStatus = "Exited" // pod will be Running but container not
		restore := setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)
		defer restore()
		err := svc.Migrate(ctx, req, nil)
		require.Error(t, err)
		assertRolledBack(t, f, mem)
	})

	t.Run("context cancelled during verify still rolls back", func(t *testing.T) {
		svc, f, mem := seed(t)
		f.PlayKubePodStatus = "Exited" // never Running, so verify waits and hits ctx.Done
		cctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled: waitRunning returns ctx.Err() on first select
		err := svc.Migrate(cctx, req, nil)
		require.Error(t, err)
		// Rollback runs on a detached context, so it completes despite the dead ctx.
		assertRolledBack(t, f, mem)
	})
}

func TestMigrate_VolumeIntegrityMismatch_RollsBack(t *testing.T) {
	ctx := context.Background()
	svc, f, mem := newMigrateSvc(t)
	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}
	require.NoError(t, mem.PutSpec(ctx, store.Spec{
		Host: "h1", Template: "postgres", Slug: "db1",
		Parameters: params, Secrets: map[string]string{"password": "p"},
	}))
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})
	f.SetVolumeData("h1", "postgres-db1-data", tarBytes(t, map[string]string{"PG_VERSION": "16"}))
	// Destination receives different content than the source -> manifests differ.
	f.ImportTransform = func(_, _ string, _ []byte) []byte {
		return tarBytes(t, map[string]string{"PG_VERSION": "CORRUPT"})
	}

	err := svc.Migrate(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, nil)
	require.ErrorIs(t, err, ErrVolumeIntegrity)

	// Rolled back: source restarted & intact, dest reaped (pod was never even applied).
	p, perr := f.PodInspect(ctx, "h1", "postgres-db1")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
	_, gerr := mem.GetSpec(ctx, "h1", "postgres", "db1")
	require.NoError(t, gerr)
	_, derr := f.PodInspect(ctx, "h2", "postgres-db1")
	require.ErrorIs(t, derr, podman.ErrNotFound)
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
