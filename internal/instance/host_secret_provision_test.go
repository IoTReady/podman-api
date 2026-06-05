package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// failingStore satisfies store.Store but fails every PutHostSecret. Embedding
// the interface means we only override the one method we care about (the rest
// panic with nil-pointer if called, which is fine — this test never calls them).
type failingStore struct {
	store.Store
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
	mem := store.NewMemory()
	svc := NewService(f, hosts, []config.Template{templateWithHostSecret()})
	svc.SetStore(mem)
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

func TestPutHostSecret_NoStoreIsNoOp(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/a"}}
	f := fake.New()
	svc := NewService(f, hosts, []config.Template{templateWithHostSecret()})
	ctx := context.Background()
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true))
	_, err := f.SecretInspect(ctx, "h1", "shared-pull-token")
	require.NoError(t, err)
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
	svc := NewService(f, hosts, []config.Template{templateWithHostSecret()})
	svc.SetStore(failingStore{})
	ctx := context.Background()

	err := svc.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v"), true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist host secret")

	// Push-before-persist: the host already holds the secret even though persist failed.
	_, ierr := f.SecretInspect(ctx, "h1", "shared-pull-token")
	assert.NoError(t, ierr)
}
