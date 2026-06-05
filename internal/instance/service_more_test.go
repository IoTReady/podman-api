package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

// --- Ping / Version ---------------------------------------------------------

func TestService_PingAndVersion(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	require.NoError(t, svc.Ping(ctx, "h1"))
	assert.ErrorIs(t, svc.Ping(ctx, "nope"), ErrUnknownHost)

	v, err := svc.Version(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, "fake-1.0", v)

	_, err = svc.Version(ctx, "nope")
	assert.ErrorIs(t, err, ErrUnknownHost)
}

// --- ListAllInstances / InstanceCount: backend error ------------------------

func TestService_ListAll_BackendError(t *testing.T) {
	svc, f := newSvc(t)
	f.PodListErr = errors.New("backend boom")
	ctx := context.Background()

	_, err := svc.ListAllInstances(ctx, "h1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backend boom")

	_, err = svc.InstanceCount(ctx, "h1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backend boom")
}

// --- Apply: play-kube failure and pod-inspect failure -----------------------

func TestService_Apply_PlayKubeError(t *testing.T) {
	svc, f := newSvc(t)
	f.PlayKubeErr = errors.New("play exploded")

	err := svc.Apply(context.Background(), "h1", pgApply("pk"), ApplyOptions{Replace: true, SkipPull: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "play kube")
}

func TestService_Apply_PodInspectError(t *testing.T) {
	svc, f := newSvc(t)
	f.PodInspectErr = errors.New("inspect boom")

	err := svc.Apply(context.Background(), "h1", pgApply("pi"), ApplyOptions{Replace: true, SkipPull: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inspect pod")
}

// Re-applying an existing instance rotates its per-instance secret (inspect ->
// remove -> recreate), exercising the rotation branch in Apply.
func TestService_Apply_RotatesInstanceSecret(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("rot"), ApplyOptions{Replace: true, SkipPull: true}))
	secret := "postgres-rot-password"
	_, err := f.SecretInspect(ctx, "h1", secret)
	require.NoError(t, err, "first apply must create the per-instance secret")

	// Second apply (replace) must succeed and leave the secret present.
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("rot"), ApplyOptions{Replace: true, SkipPull: true}))
	_, err = f.SecretInspect(ctx, "h1", secret)
	require.NoError(t, err, "rotated secret must still exist after re-apply")
}

// --- Get: pod-inspect error and volume inclusion ----------------------------

func TestService_Get_PodInspectError(t *testing.T) {
	svc, f := newSvc(t)
	f.PodInspectErr = errors.New("inspect boom")

	_, err := svc.Get(context.Background(), "h1", "postgres", "x")
	require.Error(t, err)
	// A non-NotFound backend error is surfaced verbatim, not mapped to
	// ErrInstanceNotFound.
	assert.NotErrorIs(t, err, ErrInstanceNotFound)
	assert.Contains(t, err.Error(), "inspect boom")
}

func TestService_Get_IncludesSeededVolume(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("vol"), ApplyOptions{Replace: true, SkipPull: true}))

	// pgTemplate declares a "data" volume → name postgres-vol-data.
	f.AddVolume("h1", podman.Volume{Name: "postgres-vol-data", SizeBytes: 4096})

	obs, err := svc.Get(ctx, "h1", "postgres", "vol")
	require.NoError(t, err)
	require.Len(t, obs.Volumes, 1)
	assert.Equal(t, "postgres-vol-data", obs.Volumes[0].Name)
	assert.Equal(t, int64(4096), obs.Volumes[0].SizeBytes)
}

// --- InstanceVolumes: seeded volume is returned -----------------------------

func TestService_InstanceVolumes_IncludesSeeded(t *testing.T) {
	svc, f := newSvc(t)
	f.AddVolume("h1", podman.Volume{Name: "postgres-iv-data", SizeBytes: 128})

	vols, err := svc.InstanceVolumes(context.Background(), "h1", "postgres", "iv")
	require.NoError(t, err)
	require.Len(t, vols, 1)
	assert.Equal(t, "postgres-iv-data", vols[0].Name)
}

// --- Logs: pod-inspect error ------------------------------------------------

func TestService_Logs_PodInspectError(t *testing.T) {
	svc, f := newSvc(t)
	f.PodInspectErr = errors.New("inspect boom")

	_, err := svc.Logs(context.Background(), "h1", "postgres", "x", "db", podman.LogOptions{})
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInstanceNotFound)
	assert.Contains(t, err.Error(), "inspect boom")
}

// --- PutHostSecret: rotation path -------------------------------------------

func TestService_PutHostSecret_Rotates(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()

	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared", []byte("v1"), true))
	// Second put must find the existing secret and rotate it (remove + create).
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared", []byte("v2"), true))

	_, err := f.SecretInspect(ctx, "h1", "shared")
	require.NoError(t, err)
}

// --- HostLoad ---------------------------------------------------------------

func TestService_HostLoad_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	_, err := svc.HostLoad(context.Background(), "nope")
	assert.ErrorIs(t, err, ErrUnknownHost)
}

func TestService_HostLoad_PassesThrough(t *testing.T) {
	svc, f := newSvc(t)
	f.HostInfoVal = podman.HostInfo{CPUs: 4, MemTotal: 200, MemFree: 50, MemUsedPct: 75}
	got, err := svc.HostLoad(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 4, got.CPUs)
	assert.Equal(t, 75.0, got.MemUsedPct)
}

// --- HostCounts -------------------------------------------------------------

func TestService_HostCounts(t *testing.T) {
	svc, _ := newSvc(t)
	// Apply two instances of the postgres template (each pod has 1 container).
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("a"), ApplyOptions{}))
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("b"), ApplyOptions{}))

	instances, containers, err := svc.HostCounts(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 2, instances)
	assert.Equal(t, instances, containers) // postgres template = 1 container per pod
}

func TestService_HostCounts_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	_, _, err := svc.HostCounts(context.Background(), "nope")
	assert.ErrorIs(t, err, ErrUnknownHost)
}

// --- hostsSnap: nil guard (white-box) ---------------------------------------

func TestService_HostsSnap_NilBeforeStore(t *testing.T) {
	// A zero-value Service has never stored a host map; the snapshot is nil and
	// host lookups miss cleanly rather than panicking.
	var s Service
	assert.Nil(t, s.hostsSnap())
	_, ok := s.host("anything")
	assert.False(t, ok)
}
