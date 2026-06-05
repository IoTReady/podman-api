package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func newSvcDrainable(t *testing.T) (*Service, *fake.Fake, []config.Host) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc, _ := newSvcWith(t, f, hosts, pgTemplate(), templateWithHostSecret())
	return svc, f, hosts
}

func TestService_Drain_BlocksCreate(t *testing.T) {
	svc, _, hosts := newSvcDrainable(t)
	hosts[0].Drain = true
	svc.SetHosts(hosts)

	err := svc.Apply(context.Background(), "h1", pgApply("new"), ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrHostDraining, "create-shaped Apply must be blocked")
}

func TestService_Drain_BlocksReplaceWhenPodMissing(t *testing.T) {
	svc, _, hosts := newSvcDrainable(t)
	hosts[0].Drain = true
	svc.SetHosts(hosts)

	// PUT against a slug that doesn't exist on the host is effectively a
	// create — the drain gate must still apply.
	err := svc.Apply(context.Background(), "h1", pgApply("never-existed"), ApplyOptions{Replace: true})
	require.ErrorIs(t, err, ErrHostDraining)
}

func TestService_Drain_AllowsReplaceWhenPodExists(t *testing.T) {
	svc, _, hosts := newSvcDrainable(t)
	ctx := context.Background()

	// Establish a pod while the host is undrained.
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("existing"), ApplyOptions{Replace: true}))

	// Now drain the host. Replace-shaped writes against the existing pod
	// must still succeed — this is the upgrade / config-rotation flow.
	hosts[0].Drain = true
	svc.SetHosts(hosts)
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("existing"), ApplyOptions{Replace: true}),
		"replace against existing pod must succeed even while draining")
}

func TestService_Drain_AllowsLifecycleAndDelete(t *testing.T) {
	svc, _, hosts := newSvcDrainable(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("lifeok"), ApplyOptions{Replace: true}))

	hosts[0].Drain = true
	svc.SetHosts(hosts)

	// Stop / Start / Restart / Delete are operationally what drain is FOR
	// (so operators can wind a host down). They must not be blocked.
	require.NoError(t, svc.Stop(ctx, "h1", "postgres", "lifeok"))
	require.NoError(t, svc.Start(ctx, "h1", "postgres", "lifeok"))
	require.NoError(t, svc.Restart(ctx, "h1", "postgres", "lifeok"))
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "lifeok", DeleteOptions{}))
}

func TestService_InstanceCount(t *testing.T) {
	svc, _, _ := newSvcDrainable(t)
	ctx := context.Background()

	n, err := svc.InstanceCount(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("aa"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("bb"), ApplyOptions{Replace: true}))

	n, err = svc.InstanceCount(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestService_SetHosts_AtomicSwap(t *testing.T) {
	svc, _, _ := newSvcDrainable(t)
	// Initial: undrained.
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("ok"), ApplyOptions{Replace: false}))

	// Swap to a drained version.
	svc.SetHosts([]config.Host{{ID: "h1", Addr: "unix", Socket: "/x", Drain: true}})
	err := svc.Apply(context.Background(), "h1", pgApply("after"), ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrHostDraining)

	// Swap back; new creates work again.
	svc.SetHosts([]config.Host{{ID: "h1", Addr: "unix", Socket: "/x", Drain: false}})
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("again"), ApplyOptions{Replace: false}))
}
