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
	"github.com/iotready/podman-api/internal/store"
)

// failingStore fails every PutHostSecret. It embeds *store.Memory so it still
// satisfies the full Store + template-catalog surface (and resolves templates),
// overriding only the one method we care about.
type failingStore struct {
	*store.Memory
}

// getErrStore makes GetHostSecret fail with a non-NotFound (infra) error, to
// exercise the store-lookup-error path. Embeds a real Memory so the other
// store methods (and the template catalog) still work.
type getErrStore struct {
	*store.Memory
	err error
}

func (g getErrStore) GetHostSecret(ctx context.Context, host, name string) ([]byte, error) {
	return nil, g.err
}

func (failingStore) PutHostSecret(_ context.Context, _, _ string, _ []byte) error {
	return errors.New("boom")
}

func newHostSecretSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/a"},
		{ID: "h2", Addr: "unix", Socket: "/b"},
	}
	f := fake.New()
	svc, mem := newSvcWith(t, f, hosts, templateWithHostSecret())
	return svc, f, mem
}

func TestPutHostSecret_PersistsByDefault(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true))

	_, err := f.SecretInspect(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
	got, err := mem.GetHostSecret(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), got)
}

func TestPutHostSecret_PersistFalseSkipsStore(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), false))

	_, err := f.SecretInspect(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
	_, err = mem.GetHostSecret(ctx, "h1", "shared-pull-token")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestDeleteHostSecret_RemovesFromStore(t *testing.T) {
	svc, _, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true))
	require.NoError(t, svc.DeleteHostSecret(ctx, "h1", "shared-pull-token"))
	_, err := mem.GetHostSecret(ctx, "h1", "shared-pull-token")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestHostSecretProvisionable(t *testing.T) {
	svc, _, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	ok, err := svc.hostSecretProvisionable(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = svc.hostSecretProvisionable(ctx, "h1", "absent")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestPutHostSecret_PersistError_HostStillUpdated(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/a"}}
	f := fake.New()
	svc := NewService(f, hosts)
	svc.SetStore(failingStore{Memory: store.NewMemory()})
	ctx := context.Background()

	err := svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist host secret")

	// Push-before-persist: the host already holds the secret even though persist failed.
	_, ierr := f.SecretInspect(ctx, "h1", "shared-pull-token")
	assert.NoError(t, ierr)
}

func TestPreflightIssues_ProvisionableNotBlocking(t *testing.T) {
	svc, _, mem := newHostSecretSvc(t)
	ctx := context.Background()
	// shared-pull-token absent on dest h2 BUT persisted for source h1.
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	tmpl := templateWithHostSecret()
	eff := map[string]any{"slug": "s1", "image": "img"}
	req := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}
	issues, provisionable := svc.preflightIssues(ctx, req, tmpl, eff, false)

	assert.Empty(t, issues, "provisionable secret must not be a blocking issue")
	assert.Equal(t, []string{"shared-pull-token"}, provisionable)
}

func TestPreflightIssues_NotPersistedStillBlocks(t *testing.T) {
	svc, _, _ := newHostSecretSvc(t)
	ctx := context.Background()
	tmpl := templateWithHostSecret()
	eff := map[string]any{"slug": "s1", "image": "img"}
	req := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}
	issues, provisionable := svc.preflightIssues(ctx, req, tmpl, eff, false)

	require.Len(t, issues, 1)
	assert.ErrorIs(t, issues[0], ErrHostSecretMissing)
	assert.Empty(t, provisionable)
}

func TestPreflightIssues_PresentOnDestNotProvisioned(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	// Already present on the destination AND persisted: present wins, no provision.
	require.NoError(t, f.SecretCreate(ctx, "h2", "shared-pull-token", []byte("x")))
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	tmpl := templateWithHostSecret()
	eff := map[string]any{"slug": "s1", "image": "img"}
	req := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1"}
	issues, provisionable := svc.preflightIssues(ctx, req, tmpl, eff, false)

	assert.Empty(t, issues)
	assert.Empty(t, provisionable, "present-on-dest secret is not in the provision list")
}

// seedHostSecretInstance puts a running instance of needs-host-secret on h1 with
// a stored spec, so Migrate can load + move it. Mirrors TestMigrate_HappyPath:
// a Status:"Running" source pod is all the fake needs (this template has no
// volumes and no healthcheck, so waitRunning is liveness-gated).
func seedHostSecretInstance(t *testing.T, f *fake.Fake, mem *store.Memory, slug string) {
	t.Helper()
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "needs-host-secret", Slug: slug,
		Parameters: map[string]any{"slug": slug, "image": "img"},
		Secrets:    map[string]string{},
	}))
	f.AddPod("h1", podman.Pod{Name: "needs-host-secret-" + slug, Status: "Running"})
}

func TestMigrate_ProvisionsPersistedHostSecret(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	seedHostSecretInstance(t, f, mem, "s1")
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("topsecret")))

	err := svc.Migrate(ctx, MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1",
	}, nil)
	require.NoError(t, err)

	_, err = f.SecretInspect(ctx, "h2", "shared-pull-token")
	assert.NoError(t, err, "host secret must be provisioned on the destination")
}

func TestMigrate_MissingUnpersistedHostSecretFails(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	seedHostSecretInstance(t, f, mem, "s1")

	err := svc.Migrate(ctx, MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1",
	}, nil)
	assert.ErrorIs(t, err, ErrHostSecretMissing)

	// Source instance untouched (preflight failed before Stop).
	p, ierr := f.PodInspect(ctx, "h1", "needs-host-secret-s1")
	require.NoError(t, ierr)
	assert.Equal(t, "Running", p.Status)
}

func TestMigrate_ProvisionFails_RollsBack(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	seedHostSecretInstance(t, f, mem, "s1")
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))
	f.SecretCreateErr = errors.New("disk full") // provisioning the dest secret fails

	err := svc.Migrate(ctx, MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1",
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provision host secret")

	// Rolled back: source pod restored and still Running.
	p, ierr := f.PodInspect(ctx, "h1", "needs-host-secret-s1")
	require.NoError(t, ierr)
	assert.Equal(t, "Running", p.Status)
}

// raceCreateClient simulates a concurrent migrate creating the dest host secret
// in the window between this migrate's inspect and create: SecretCreate records
// the secret (as the racing sibling would) but still returns an error.
type raceCreateClient struct {
	*fake.Fake
}

func (c *raceCreateClient) SecretCreate(ctx context.Context, h, name string, val []byte) error {
	_ = c.Fake.SecretCreate(ctx, h, name, val) // the "other" migrate creates it
	return errors.New("name already in use")
}

func TestMigrate_ProvisionRace_Benign(t *testing.T) {
	ctx := context.Background()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/a"},
		{ID: "h2", Addr: "unix", Socket: "/b"},
	}
	f := fake.New()
	client := &raceCreateClient{Fake: f}
	svc, mem := newSvcWith(t, client, hosts, templateWithHostSecret())

	seedHostSecretInstance(t, f, mem, "s1")
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	err := svc.Migrate(ctx, MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1",
	}, nil)
	require.NoError(t, err, "a benign create race must not fail the migrate")

	_, ierr := f.SecretInspect(ctx, "h2", "shared-pull-token")
	assert.NoError(t, ierr, "the racing create left the secret present on the dest")
}

func TestPreflightIssues_StoreErrorCollectsAndContinues(t *testing.T) {
	ctx := context.Background()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	f := fake.New()
	svc := NewService(f, hosts)
	svc.SetStore(getErrStore{Memory: seedStore(t, secretAndPortTemplate()), err: errors.New("store boom")})
	// Occupy port 9090 on the dest so the port check ALSO fails — proving the
	// scan continued past the store-lookup error rather than early-returning.
	f.AddPod("h2", podman.Pod{Name: "occupier", Status: "Running",
		Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 9090}}}}})

	eff := map[string]any{"slug": "x", "image": "img"}
	issues, prov := svc.preflightIssues(ctx,
		MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-both", Slug: "x"},
		secretAndPortTemplate(), eff, false)

	assert.Empty(t, prov)
	require.Len(t, issues, 2, "store-lookup error must not short-circuit the port scan")
	var sawPort, sawStoreErr bool
	for _, e := range issues {
		if errors.Is(e, ErrPortConflict) {
			sawPort = true
		} else if !errors.Is(e, ErrHostSecretMissing) {
			sawStoreErr = true
		}
	}
	assert.True(t, sawPort, "port conflict still reported")
	assert.True(t, sawStoreErr, "store lookup error reported as its own issue")
}

func TestMigrate_RecordsDestForChaining(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	seedHostSecretInstance(t, f, mem, "s1")
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("topsecret")))

	require.NoError(t, svc.Migrate(ctx, MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "needs-host-secret", Slug: "s1",
	}, nil))

	// The destination is now a valid future provisioning source: a later h2->h3
	// hop must not re-block on ErrHostSecretMissing.
	got, err := mem.GetHostSecret(ctx, "h2", "shared-pull-token")
	require.NoError(t, err)
	assert.Equal(t, []byte("topsecret"), got)
}

func TestDeleteHostSecret_StoreDeleteFails(t *testing.T) {
	svc, f, mem := newHostSecretSvc(t)
	ctx := context.Background()
	require.NoError(t, f.SecretCreate(ctx, "h1", "shared-pull-token", []byte("v")))
	mem.DeleteErr = errors.New("store down")

	err := svc.DeleteHostSecret(ctx, "h1", "shared-pull-token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete persisted host secret")
}
