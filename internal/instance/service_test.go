package instance

import (
	"context"
	"errors"
	"sync/atomic"
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

// requiredParams turns a list of names into required ParamDefs of type string —
// the typed equivalent of the old render.Parameters{Required: ...} fixtures.
func requiredParams(names ...string) []render.ParamDef {
	out := make([]render.ParamDef, 0, len(names))
	for _, n := range names {
		out = append(out, render.ParamDef{Name: n, Type: "string", Required: true})
	}
	return out
}

// seedStore returns a Memory store pre-loaded with the given templates, ready to
// hand to svc.SetStore. The instance Service always resolves templates from its
// store, so every test seeds the catalog this way.
func seedStore(t *testing.T, tmpls ...store.Template) *store.Memory {
	t.Helper()
	mem := store.NewMemory()
	for _, tm := range tmpls {
		require.NoError(t, mem.PutTemplate(context.Background(), tm))
	}
	return mem
}

// newSvcWith builds a Service whose catalog is seeded with tmpls and returns it
// alongside the backing Memory store (for tests that also assert on specs).
func newSvcWith(t *testing.T, client podman.Client, hosts []config.Host, tmpls ...store.Template) (*Service, *store.Memory) {
	t.Helper()
	mem := seedStore(t, tmpls...)
	svc := NewService(client, hosts)
	svc.SetStore(mem)
	return svc, mem
}

// pgTemplate is the postgres-shaped fixture used across these tests. It mirrors
// the bundled templates/postgres.yaml but is inlined so the test doesn't depend
// on the embedded FS.
func pgTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "postgres",
			Parameters: requiredParams("slug", "image", "port", "db", "user"),
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
		Origin: "seed",
	}
}

// templateWithHostSecret returns a synthetic template that declares a per-host
// secret reference, used to exercise the ErrHostSecretMissing path.
func templateWithHostSecret() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "needs-host-secret",
			Parameters: requiredParams("slug", "image"),
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
		Origin: "seed",
	}
}

func newSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	svc, f, _ := newSvcMem(t)
	return svc, f
}

// newSvcMem is newSvc but also returns the backing Memory store, already seeded
// with the pg + host-secret templates. Tests that assert on persisted specs use
// this rather than wiring a fresh (template-less) store.
func newSvcMem(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc, mem := newSvcWith(t, f, hosts, pgTemplate(), templateWithHostSecret())
	return svc, f, mem
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

	_, startErr := svc.Start(ctx, "h1", "postgres", "demo")
	require.NoError(t, startErr)
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
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "p", sp.Secrets["password"])
	assert.Equal(t, "docker.io/library/postgres:16", sp.Parameters["image"])
}

func TestService_Apply_PlayKubeFail_NoSpec(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	f.PlayKubeErr = errors.New("boom")
	ctx := context.Background()

	require.Error(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

// keylessStore wraps a Memory store but reports SecretsEnabled()==false, the way
// a SQLite store opened without -spec-key-file behaves.
type keylessStore struct{ *store.Memory }

func (keylessStore) SecretsEnabled() bool { return false }

// On a key-less store, a secret-bearing Apply must be rejected with
// ErrSecretsNeedKey BEFORE any host mutation — no pod played, no secret created —
// so the host is never left with an orphaned pod/secrets the missing spec can't
// account for. (#61)
func TestService_Apply_SecretBearing_KeylessStore_NoMutation(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	mem := seedStore(t, pgTemplate())
	svc := NewService(f, hosts)
	svc.SetStore(keylessStore{mem})
	ctx := context.Background()

	err := svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.ErrorIs(t, err, store.ErrSecretsNeedKey)

	// No host mutation happened: no pod played, no secret created.
	assert.Empty(t, f.PlayCalls, "PlayKube must not be called")
	secs, lerr := f.SecretList(ctx, "h1")
	require.NoError(t, lerr)
	assert.Empty(t, secs, "no secrets must be created")
	// And of course no spec row.
	_, gerr := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, gerr, store.ErrNotFound)
}

func TestService_Apply_StorePutError_Fatal(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	mem.PutErr = errors.New("db down")

	err := svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist spec")
}

func TestService_Delete_RemovesSpec(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestService_Delete_StoreDeleteError_Fatal(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	mem.DeleteErr = errors.New("db down")
	err := svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete spec")
}

func TestService_UpgradeImage_ReusesStoredSecrets(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()

	// Initial deploy persists params + the per-instance secret.
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	// Image-only upgrade: the operator supplies just the new image.
	require.NoError(t, svc.UpgradeImage(ctx, "h1", "postgres", "demo", "docker.io/library/postgres:17"))

	// Stored spec now carries the new image; the secret is reused unchanged.
	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "docker.io/library/postgres:17", sp.Parameters["image"])
	assert.Equal(t, "p", sp.Secrets["password"], "secret should be reused, not wiped")
	// Non-image params are preserved too.
	assert.Equal(t, "app", sp.Parameters["db"])
}

func TestService_UpgradeImage_MissingSpecIsNotFound(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.UpgradeImage(context.Background(), "h1", "postgres", "ghost", "x:1")
	assert.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestService_UpgradeImage_EmptyImageRejected(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.UpgradeImage(context.Background(), "h1", "postgres", "demo", "")
	require.Error(t, err)
}

// twoSecretTemplate declares two per-instance secrets so a rotation of one can
// prove the other (a field left blank) is preserved. render.Validate treats the
// PerInstance list as the complete allow-list AND requires every declared name,
// so an undeclared secret can't be seeded — both must be declared and supplied.
func twoSecretTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "twosec",
			Parameters: requiredParams("slug", "image"),
			Secrets:    render.Secrets{PerInstance: []string{"password", "token"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: twosec-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
`,
		Origin: "seed",
	}
}

func TestRotateInstanceSecrets_OverlaysAndReapplies(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, twoSecretTemplate())
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "twosec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "keep"},
	}, ApplyOptions{Replace: true}))

	// Rotate only "password"; "token" is left out (the write-only "blank keeps
	// existing value" path).
	require.NoError(t, svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo",
		map[string]string{"password": "rotated"}))

	got, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	assert.Equal(t, "rotated", got.Secrets["password"]) // overlaid
	assert.Equal(t, "keep", got.Secrets["token"])       // absent name preserved
	assert.Equal(t, "img:1", got.Parameters["image"])   // params preserved
}

// TestUpgradeImage_AllowsMissingRequiredSecret guards the Upgrade/Rotate
// consistency decision: like rotation, an image-only upgrade must not be blocked
// because the stored spec lacks a per-instance secret the template now requires
// (a secret added after the instance was deployed).
func TestUpgradeImage_AllowsMissingRequiredSecret(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, twoSecretTemplate())
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "twosec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "t"},
	}, ApplyOptions{Replace: true}))
	// Simulate a template that gained "token" after deploy: drop it from the spec.
	sp, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	delete(sp.Secrets, "token")
	require.NoError(t, mem.PutSpec(ctx, sp))

	require.NoError(t, svc.UpgradeImage(ctx, "h1", "twosec", "demo", "img:2"))

	got, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	assert.Equal(t, "img:2", got.Parameters["image"])
}

// gatedClient wraps a podman.Client and, once armed, blocks every PlayKube until
// the test closes release — signalling each entry on reached (buffered so a second
// concurrent entry never blocks). It lets the test interleave two rotations of one
// instance deterministically.
type gatedClient struct {
	podman.Client
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *gatedClient) PlayKube(ctx context.Context, host, yaml string, replace bool, networks ...string) error {
	if g.armed.Load() {
		g.reached <- struct{}{}
		<-g.release
	}
	return g.Client.PlayKube(ctx, host, yaml, replace, networks...)
}

// TestRotateInstanceSecrets_ConcurrentRotationsDoNotLoseUpdates proves load+apply
// is atomic under one lock: two concurrent rotations of *different* secrets on the
// same instance must both survive. On the pre-fix code (GetSpec outside Apply's
// lock) the second rotation reads the spec before the first commits and re-applies
// a stale value, dropping one update.
func TestRotateInstanceSecrets_ConcurrentRotationsDoNotLoseUpdates(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	g := &gatedClient{Client: fake.New(), reached: make(chan struct{}, 2), release: make(chan struct{})}
	svc, mem := newSvcWith(t, g, hosts, twoSecretTemplate())
	ctx := context.Background()

	// Deploy with the gate disarmed.
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "twosec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "t"},
	}, ApplyOptions{Replace: true}))

	g.armed.Store(true)

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	// A rotates password, parks in PlayKube holding the instance lock.
	go func() {
		errA <- svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo", map[string]string{"password": "A"})
	}()
	<-g.reached
	// B rotates token. In BOTH worlds B blocks on Apply's instance lock (held by
	// A, parked in PlayKube) and never reaches its own gate — the difference is
	// only WHERE B's GetSpec sits relative to that lock:
	//   pre-fix:  B reads the spec OUTSIDE the lock first (the stale pre-commit
	//             read is what bakes in the lost update), then blocks on the lock;
	//   post-fix: B blocks on the lock BEFORE any read, so it can't read stale.
	go func() {
		errB <- svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo", map[string]string{"token": "B"})
	}()
	// Give B time to reach that blocking point before releasing A. A fixed wait
	// (not a gate signal) is correct here precisely because B cannot reach the
	// PlayKube gate while A holds the lock — so there is no signal to wait on;
	// longer only slows the test, shorter risks nothing.
	time.Sleep(300 * time.Millisecond)
	close(g.release)
	require.NoError(t, <-errA)
	require.NoError(t, <-errB)

	got, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	assert.Equal(t, "A", got.Secrets["password"], "password rotation lost (RMW race)")
	assert.Equal(t, "B", got.Secrets["token"], "token rotation lost (RMW race)")
}

func TestRotateInstanceSecrets_EmptyIsRejected(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.Error(t, svc.RotateInstanceSecrets(ctx, "h1", "postgres", "demo", map[string]string{}))
}

func TestRotateInstanceSecrets_NoSpecIsNotFound(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	err := svc.RotateInstanceSecrets(context.Background(), "h1", "postgres", "ghost",
		map[string]string{"password": "x"})
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestRotateInstanceSecrets_CorruptSpecPropagates(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	svc.SetStore(&getSpecErrStore{Memory: mem, err: store.ErrSpecCorrupt})
	err := svc.RotateInstanceSecrets(context.Background(), "h1", "postgres", "demo",
		map[string]string{"password": "x"})
	require.ErrorIs(t, err, store.ErrSpecCorrupt)
}

func TestInstanceSecretState_ReportsPresenceNotValues(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	set, err := svc.InstanceSecretState(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.True(t, set["password"]) // stored
	assert.False(t, set["token"])   // never set → absent → false
}

func TestInstanceSecretState_NoSpecIsNotFound(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	_, err := svc.InstanceSecretState(context.Background(), "h1", "postgres", "ghost")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestApplyAndObserve_ReadyOnSuccess(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.PlayKubeContainerHealth = "healthy"
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.ApplyAndObserve(context.Background(), "h1", ApplyRequest{
		Template:   "web",
		Slug:       "s1",
		Parameters: map[string]any{"slug": "s1", "image": "nginx"},
	}, ApplyOptions{})
	require.NoError(t, err)
	assert.True(t, obs.Ready)
	assert.Empty(t, obs.Warnings)
}

func TestApplyAndObserve_WarningOnTimeout(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.PlayKubeContainerHealth = "starting"
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.ApplyAndObserve(context.Background(), "h1", ApplyRequest{
		Template:   "web",
		Slug:       "s1",
		Parameters: map[string]any{"slug": "s1", "image": "nginx"},
	}, ApplyOptions{})
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "readiness timeout")
}

func TestStart_ReadyOnSuccess(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-s1", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "healthy"}}})
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.Start(context.Background(), "h1", "web", "s1")
	require.NoError(t, err)
	assert.True(t, obs.Ready)
	assert.Empty(t, obs.Warnings)
}

func TestStart_WarningOnTimeout(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-s1", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.Start(context.Background(), "h1", "web", "s1")
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "readiness timeout")
}
