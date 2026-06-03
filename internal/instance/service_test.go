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

// pgTemplate is the postgres-shaped fixture used across these tests. It mirrors
// the bundled templates/postgres.yaml but is inlined so the test doesn't depend
// on the embedded FS.
func pgTemplate() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID: "postgres",
			Parameters: render.Parameters{
				Required: []string{"slug", "image", "port", "db", "user"},
			},
			Secrets: render.Secrets{
				PerInstance: []string{"password"},
			},
			Volumes: []render.Volume{{Name: "data", Backup: "none"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: postgres-{{.slug}}
  labels:
    podman-api/template: postgres
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
`,
		Source: "postgres.yaml",
	}
}

// templateWithHostSecret returns a synthetic template that declares a per-host
// secret reference, used to exercise the ErrHostSecretMissing path.
func templateWithHostSecret() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID: "needs-host-secret",
			Parameters: render.Parameters{
				Required: []string{"slug", "image"},
			},
			Secrets: render.Secrets{
				PerHostReferenced: []string{"shared-pull-token"},
			},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: needs-host-secret-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
`,
		Source: "needs-host-secret.yaml",
	}
}

func newSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc := NewService(f, hosts, []config.Template{pgTemplate(), templateWithHostSecret()})
	return svc, f
}

func pgApply(slug string) ApplyRequest {
	return ApplyRequest{
		Template: "postgres",
		Slug:     slug,
		Parameters: map[string]any{
			"slug": slug, "image": "docker.io/library/postgres:16",
			"port": 5432, "db": "app", "user": "app",
		},
		Secrets: map[string]string{"password": "p"},
	}
}

func TestService_Apply_Then_Get(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	obs, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "postgres", obs.Template)
	assert.Equal(t, "demo", obs.Slug)
	assert.Equal(t, "Running", obs.Pod.Status)
}

func TestService_Apply_RequiresHostSecret(t *testing.T) {
	svc, _ := newSvc(t)
	// shared-pull-token is intentionally not seeded on the fake host.
	err := svc.Apply(context.Background(), "h1", ApplyRequest{
		Template:   "needs-host-secret",
		Slug:       "x",
		Parameters: map[string]any{"slug": "x", "image": "x:1"},
	}, ApplyOptions{Replace: false})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrHostSecretMissing)
}

func TestService_UnknownTemplate(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "h1", ApplyRequest{Template: "nope", Slug: "x"}, ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestService_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "nope", ApplyRequest{Template: "postgres", Slug: "x"}, ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrUnknownHost)
}

func TestService_Lifecycle(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.Stop(ctx, "h1", "postgres", "demo"))
	obs, _ := svc.Get(ctx, "h1", "postgres", "demo")
	assert.Equal(t, "Exited", obs.Pod.Status)

	require.NoError(t, svc.Start(ctx, "h1", "postgres", "demo"))
	obs, _ = svc.Get(ctx, "h1", "postgres", "demo")
	assert.Equal(t, "Running", obs.Pod.Status)

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))
	_, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestService_Delete_PrunesOrphanSecretWhenPodAlreadyGone(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("orphan"), ApplyOptions{Replace: true}))

	// Applying created the per-instance secret.
	secs, err := f.SecretList(ctx, "h1")
	require.NoError(t, err)
	require.Len(t, secs, 1)

	// A prune-less delete removes the pod but leaves the secret orphaned.
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "orphan", DeleteOptions{}))
	secs, _ = f.SecretList(ctx, "h1")
	require.Len(t, secs, 1, "secret should be orphaned after a prune-less delete")

	// Deleting again WITH prune must reap the orphan and succeed even though the
	// pod is already gone — delete is an idempotent reconcile, not a 404.
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "orphan",
		DeleteOptions{PruneSecrets: true, PruneVolumes: true}))
	secs, _ = f.SecretList(ctx, "h1")
	require.Empty(t, secs, "orphaned secret should be pruned")
}

func TestService_Delete_AbsentInstanceWithoutPruneIsNotFound(t *testing.T) {
	svc, _ := newSvc(t)
	// Nothing applied; deleting a non-existent instance without prune flags
	// still reports not-found (the idempotent-prune path must not mask this).
	err := svc.Delete(context.Background(), "h1", "postgres", "ghost", DeleteOptions{})
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestService_Apply_ConflictWhenExists(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	req := pgApply("dup")
	// First apply with replace=true succeeds.
	require.NoError(t, svc.Apply(ctx, "h1", req, ApplyOptions{Replace: true}))

	// Second apply with replace=false must return ErrInstanceExists.
	err := svc.Apply(ctx, "h1", req, ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrInstanceExists)
}

func TestService_Upgrade_DoesNotMutateCallerMap(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	apply := pgApply("up")
	require.NoError(t, svc.Apply(ctx, "h1", apply, ApplyOptions{Replace: true}))

	upgradeReq := pgApply("up")
	originalImage := upgradeReq.Parameters["image"]
	require.NoError(t, svc.Upgrade(ctx, "h1", upgradeReq, "docker.io/library/postgres:17"))

	// Caller's map untouched.
	assert.Equal(t, originalImage, upgradeReq.Parameters["image"], "Upgrade must not mutate caller's Parameters map")

	// Pod actually has the new image (via observed).
	obs, err := svc.Get(ctx, "h1", "postgres", "up")
	require.NoError(t, err)
	require.Len(t, obs.Containers, 1)
	assert.Equal(t, "docker.io/library/postgres:17", obs.Containers[0].Image)
}

func TestService_Apply_PrePullsImages(t *testing.T) {
	svc, f := newSvc(t)
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.Len(t, f.PullCalls, 1, "Apply must pre-pull every container image")
	assert.Equal(t, "h1", f.PullCalls[0].Host)
	assert.Equal(t, "docker.io/library/postgres:16", f.PullCalls[0].Image)
}

func TestService_Apply_PullFailureMapsToErrImagePull(t *testing.T) {
	svc, f := newSvc(t)
	f.PullErr = map[string]error{"": errors.New("manifest unknown")}
	err := svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrImagePull)
	assert.Contains(t, err.Error(), "manifest unknown")
	// Pull failure must abort BEFORE any secret is written so we don't leak orphans.
	_, secretErr := f.SecretInspect(context.Background(), "h1", "postgres-demo-password")
	assert.ErrorIs(t, secretErr, podman.ErrNotFound, "no per-instance secret should be created when pull fails")
}

func TestService_Apply_SkipPull(t *testing.T) {
	svc, f := newSvc(t)
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true, SkipPull: true}))
	assert.Empty(t, f.PullCalls, "SkipPull must suppress all ImagePull calls")
}

// podman is imported but used only to satisfy reference in tests above.
var _ = podman.ErrNotFound

func TestService_Apply_PersistsSpec(t *testing.T) {
	svc, _ := newSvc(t)
	mem := store.NewMemory()
	svc.SetStore(mem)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "p", sp.Secrets["password"])
	assert.Equal(t, "docker.io/library/postgres:16", sp.Parameters["image"])
}

func TestService_Apply_PlayKubeFail_NoSpec(t *testing.T) {
	svc, f := newSvc(t)
	mem := store.NewMemory()
	svc.SetStore(mem)
	f.PlayKubeErr = errors.New("boom")
	ctx := context.Background()

	require.Error(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestService_Apply_StorePutError_Fatal(t *testing.T) {
	svc, _ := newSvc(t)
	mem := store.NewMemory()
	mem.PutErr = errors.New("db down")
	svc.SetStore(mem)

	err := svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist spec")
}

func TestService_Delete_RemovesSpec(t *testing.T) {
	svc, _ := newSvc(t)
	mem := store.NewMemory()
	svc.SetStore(mem)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestService_Delete_NilStore_OK(t *testing.T) {
	svc, _ := newSvc(t) // no SetStore
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))
}

func TestService_Delete_StoreDeleteError_Fatal(t *testing.T) {
	svc, _ := newSvc(t)
	mem := store.NewMemory()
	svc.SetStore(mem)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	mem.DeleteErr = errors.New("db down")
	err := svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete spec")
}
