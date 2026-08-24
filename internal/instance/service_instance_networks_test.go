package instance

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// #270: the whole point — one template, two isolated groups. An instance joins
// a network its template never names, without a template edit that would move
// every other instance of that template too.
func TestApplyAttachesPerInstanceNetworks(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate())

	req := sharedNetApply("vedanta")
	req.Networks = []render.Network{{Name: "customer-a"}}
	require.NoError(t, svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"customer-a"}, fc.PlayCalls[0].Networks)
	assert.Equal(t, []string{"customer-a"}, fc.NetworkEnsureCalls["h1"])
}

// Union, not override: the template's declarations are guaranteed connectivity
// an instance cannot drop, so both sets are joined, template's first.
func TestApplyUnionsPerInstanceNetworksWithTemplate(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))

	req := sharedNetApply("vedanta")
	req.Networks = []render.Network{{Name: "customer-a"}}
	require.NoError(t, svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared", "customer-a"}, fc.PlayCalls[0].Networks)
}

// A per-instance entry naming a network the template already declares must add
// its aliases to that ONE join, not produce a second --network podman rejects.
func TestApplyMergesPerInstanceAliasesOntoTemplateNetwork(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	))

	req := sharedNetApply("vedanta")
	req.Networks = []render.Network{{Name: "frappe-shared", Aliases: []string{"db"}}}
	require.NoError(t, svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared:alias=mariadb,alias=db"}, fc.PlayCalls[0].Networks)
	assert.Equal(t, []string{"frappe-shared"}, fc.NetworkEnsureCalls["h1"])
}

// The request's networks are policed by the same validator as a template's own,
// before anything is mutated — a malformed name must not reach NetworkEnsure.
func TestApplyRejectsInvalidPerInstanceNetwork(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate())

	req := sharedNetApply("vedanta")
	req.Networks = []render.Network{{Name: "not a network"}}
	err := svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid network name")
	assert.Empty(t, fc.PlayCalls)
	assert.Empty(t, fc.NetworkEnsureCalls["h1"])
}

func TestApplyRejectsInvalidPerInstanceAlias(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate())

	req := sharedNetApply("vedanta")
	req.Networks = []render.Network{{Name: "customer-a", Aliases: []string{"bad alias"}}}
	err := svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid alias")
	assert.Empty(t, fc.PlayCalls)
}

// The DNS uniqueness check must see a PEER's per-instance networks, which live
// on no template. Without the spec-side projection this apply would be accepted
// and both pods would answer to "db" on customer-a — #269's failure, reached by
// the door #270 opens.
func TestApplyRejectsAliasConflictOnPerInstanceNetwork(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate())

	first := sharedNetApply("vedanta")
	first.Networks = []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}
	require.NoError(t, svc.Apply(context.Background(), "h1", first, ApplyOptions{Replace: true}))

	second := sharedNetApply("swelect")
	second.Networks = []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}
	err := svc.Apply(context.Background(), "h1", second, ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `DNS name "db"`)
	assert.Contains(t, err.Error(), "db/vedanta")
	assert.Len(t, fc.PlayCalls, 1, "the conflicting pod must not be played")
}

// The mirror image: two instances of one template on DIFFERENT per-instance
// networks share no namespace, so the same alias on each is legal. This is the
// isolated-groups case the feature exists for — it must not be blocked by the
// subject's own override leaking onto its peers' claims.
func TestApplyAllowsSameAliasOnDifferentPerInstanceNetworks(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate())

	first := sharedNetApply("vedanta")
	first.Networks = []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}
	require.NoError(t, svc.Apply(context.Background(), "h1", first, ApplyOptions{Replace: true}))

	second := sharedNetApply("swelect")
	second.Networks = []render.Network{{Name: "customer-b", Aliases: []string{"db"}}}
	require.NoError(t, svc.Apply(context.Background(), "h1", second, ApplyOptions{Replace: true}))

	assert.Len(t, fc.PlayCalls, 2)
}

// An instance's own networks are its own: an apply of a SECOND instance of the
// same template must not inherit the first's memberships through the shared
// template-meta cache.
func TestApplyDoesNotLeakPerInstanceNetworksBetweenInstances(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate())

	first := sharedNetApply("vedanta")
	first.Networks = []render.Network{{Name: "customer-a"}}
	require.NoError(t, svc.Apply(context.Background(), "h1", first, ApplyOptions{Replace: true}))

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("swelect"), ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 2)
	assert.Empty(t, fc.PlayCalls[1].Networks)
}

// Persisted on the spec, so every later read (converge, migrate, rename) sees it.
func TestApplyPersistsPerInstanceNetworks(t *testing.T) {
	svc, _, st := sharedNetSvc(t, sharedNetTemplate())

	req := sharedNetApply("vedanta")
	req.Networks = []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}
	require.NoError(t, svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true}))

	sp, err := st.GetSpec(context.Background(), "h1", "db", "vedanta")
	require.NoError(t, err)
	assert.Equal(t, []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}, sp.AppliedNetworks)
}

// Boot converge replays the instance's OWN set: the template names none of
// these networks, so re-deriving the join list from the template alone would
// silently detach the pod after a reboot.
func TestReconcileSpecsOnHost_ReplaysPerInstanceNetworks(t *testing.T) {
	svc, fc, st := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	require.NoError(t, st.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "db", Slug: "vedanta",
		Parameters:      map[string]any{"slug": "vedanta"},
		AppliedNetworks: []render.Network{{Name: "customer-a", Aliases: []string{"db"}}},
	}))

	svc.ReconcileSpecsOnHost(context.Background(), "h1")

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared", "customer-a:alias=db"}, fc.PlayCalls[0].Networks)
	assert.Equal(t, []string{"frappe-shared", "customer-a"}, fc.NetworkEnsureCalls["h1"])

	// ...and the converge does not drop the field on its way back to the store,
	// or the NEXT converge would be the one that detaches the pod.
	sp, err := st.GetSpec(context.Background(), "h1", "db", "vedanta")
	require.NoError(t, err)
	assert.Equal(t, []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}, sp.AppliedNetworks)
}

// Every re-apply built from a stored spec must carry the field forward. These
// paths reconstruct an ApplyRequest field by field, so an omission here is
// silent: the instance keeps running and loses a network on its next restart.
func TestReapplyPathsCarryPerInstanceNetworksForward(t *testing.T) {
	nets := []render.Network{{Name: "customer-a"}}
	seed := func(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
		t.Helper()
		// An image parameter, so the image-only upgrade path has something to
		// override — the shared fixture declares only slug.
		tmpl := sharedNetTemplate()
		tmpl.Meta.Parameters = requiredParams("slug", "image")
		tmpl.Body = strings.Replace(tmpl.Body, "mariadb:latest", "{{.image}}", 1)
		svc, fc, st := sharedNetSvc(t, tmpl)
		req := sharedNetApply("vedanta")
		req.Parameters["image"] = "mariadb:10"
		req.Networks = nets
		require.NoError(t, svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true}))
		return svc, fc, st
	}

	t.Run("UpgradeImage", func(t *testing.T) {
		svc, _, st := seed(t)
		require.NoError(t, svc.UpgradeImage(context.Background(), "h1", "db", "vedanta", "mariadb:11"))
		sp, err := st.GetSpec(context.Background(), "h1", "db", "vedanta")
		require.NoError(t, err)
		assert.Equal(t, nets, sp.AppliedNetworks)
	})

	t.Run("UpdateInstanceParameters", func(t *testing.T) {
		svc, _, st := seed(t)
		require.NoError(t, svc.UpdateInstanceParameters(context.Background(), "h1", "db", "vedanta",
			map[string]any{"image": "mariadb:11"}))
		sp, err := st.GetSpec(context.Background(), "h1", "db", "vedanta")
		require.NoError(t, err)
		assert.Equal(t, nets, sp.AppliedNetworks)
	})

	t.Run("Rename", func(t *testing.T) {
		svc, _, st := seed(t)
		require.NoError(t, svc.Rename(context.Background(), "h1", "db", "vedanta",
			RenameRequest{NewSlug: "vedanta2"}, func(string, string) {}))
		sp, err := st.GetSpec(context.Background(), "h1", "db", "vedanta2")
		require.NoError(t, err)
		assert.Equal(t, nets, sp.AppliedNetworks)
	})
}

// A per-instance network is ensured on the DESTINATION host by the apply there,
// so a migrated instance keeps its connectivity even on a host that has never
// seen that network.
func TestMigrateCarriesPerInstanceNetworksToDestination(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x"},
		{ID: "h2", Addr: "unix", Socket: "/y"},
	}
	svc, st := newSvcWith(t, fc, hosts, sharedNetTemplate())

	req := sharedNetApply("vedanta")
	req.Networks = []render.Network{{Name: "customer-a"}}
	require.NoError(t, svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true}))

	require.NoError(t, svc.Migrate(context.Background(), MigrateRequest{
		FromHost: "h1", ToHost: "h2", Template: "db", Slug: "vedanta",
	}, func(string, string) {}))

	sp, err := st.GetSpec(context.Background(), "h2", "db", "vedanta")
	require.NoError(t, err)
	assert.Equal(t, []render.Network{{Name: "customer-a"}}, sp.AppliedNetworks)
	assert.Equal(t, []string{"customer-a"}, fc.NetworkEnsureCalls["h2"])
}
