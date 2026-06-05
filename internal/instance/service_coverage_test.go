package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestService_Restart(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("r"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Restart(ctx, "h1", "postgres", "r"))
}

func TestService_Lifecycle_ErrorPaths(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	// Unknown template flows through lifecycle's lookup.
	require.ErrorIs(t, svc.Start(ctx, "h1", "nope", "x"), ErrUnknownTemplate)
	// Unknown host likewise.
	require.ErrorIs(t, svc.Stop(ctx, "nope", "postgres", "x"), ErrUnknownHost)
	// A known template+host but never-applied slug maps podman's not-found to
	// the instance-not-found sentinel.
	require.ErrorIs(t, svc.Restart(ctx, "h1", "postgres", "ghost"), ErrInstanceNotFound)
}

func TestService_List(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("a"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("b"), ApplyOptions{Replace: true}))

	got, err := svc.List(ctx, "h1", "postgres")
	require.NoError(t, err)
	assert.Len(t, got, 2)

	_, err = svc.List(ctx, "h1", "nope")
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestService_HostsAndTemplates(t *testing.T) {
	svc, _ := newSvc(t)

	hosts := svc.Hosts()
	require.Len(t, hosts, 1)
	assert.Equal(t, "h1", hosts[0].ID)

	tmpls, err := svc.Templates(context.Background())
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, tm := range tmpls {
		ids[tm.Meta.ID] = true
	}
	assert.True(t, ids["postgres"])
	assert.True(t, ids["needs-host-secret"])
}

func TestService_PortsInUse(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	_, err := svc.PortsInUse(ctx, "nope")
	require.ErrorIs(t, err, ErrUnknownHost)

	ports, err := svc.PortsInUse(ctx, "h1")
	require.NoError(t, err)
	assert.Empty(t, ports)
}

func TestService_HostSecrets_RoundTrip(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	// Unknown-host guards.
	_, err := svc.HostSecrets(ctx, "nope")
	require.ErrorIs(t, err, ErrUnknownHost)
	require.ErrorIs(t, svc.PutHostSecret(ctx, "nope", "s", []byte("v"), true), ErrUnknownHost)
	require.ErrorIs(t, svc.DeleteHostSecret(ctx, "nope", "s"), ErrUnknownHost)

	// Create.
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared", []byte("v1"), true))
	secs, err := svc.HostSecrets(ctx, "h1")
	require.NoError(t, err)
	require.Len(t, secs, 1)

	// Rotate: existing secret is removed then recreated (still one entry).
	require.NoError(t, svc.PutHostSecret(ctx, "h1", "shared", []byte("v2"), true))
	secs, _ = svc.HostSecrets(ctx, "h1")
	require.Len(t, secs, 1)

	// Delete, then delete again is idempotent (not-found swallowed).
	require.NoError(t, svc.DeleteHostSecret(ctx, "h1", "shared"))
	require.NoError(t, svc.DeleteHostSecret(ctx, "h1", "shared"))
	secs, _ = svc.HostSecrets(ctx, "h1")
	assert.Empty(t, secs)
}

func TestService_DeleteVolume(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	require.ErrorIs(t, svc.DeleteVolume(ctx, "nope", "v", false), ErrUnknownHost)
	// Removing a volume that doesn't exist is idempotent.
	require.NoError(t, svc.DeleteVolume(ctx, "h1", "absent", true))
}

func TestService_InstanceVolumes(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	_, err := svc.InstanceVolumes(ctx, "h1", "nope", "x")
	require.ErrorIs(t, err, ErrUnknownTemplate)

	// The template declares a "data" volume, but the fake host has none, so
	// inspect misses are silently omitted and the result is empty.
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("v"), ApplyOptions{Replace: true}))
	vols, err := svc.InstanceVolumes(ctx, "h1", "postgres", "v")
	require.NoError(t, err)
	assert.Empty(t, vols)
}

func TestService_Logs(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	// Unknown template.
	_, err := svc.Logs(ctx, "h1", "nope", "x", "db", podman.LogOptions{})
	require.ErrorIs(t, err, ErrUnknownTemplate)

	// Known template but no pod -> instance not found.
	_, err = svc.Logs(ctx, "h1", "postgres", "ghost", "db", podman.LogOptions{})
	require.ErrorIs(t, err, ErrInstanceNotFound)

	// Applied instance returns a (closed, from the fake) log channel.
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("l"), ApplyOptions{Replace: true}))
	ch, err := svc.Logs(ctx, "h1", "postgres", "l", "db", podman.LogOptions{})
	require.NoError(t, err)
	require.NotNil(t, ch)
	_, open := <-ch
	assert.False(t, open, "fake returns an already-closed channel")
}

func TestService_Upgrade(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	// Upgrade requires a non-empty image.
	require.Error(t, svc.Upgrade(ctx, "h1", pgApply("u"), ""))

	// Happy path: replace-in-place with a new image.
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("u"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Upgrade(ctx, "h1", pgApply("u"), "docker.io/library/postgres:17"))
}
