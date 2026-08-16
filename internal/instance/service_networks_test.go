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

// sharedNetTemplate is a fixture declaring shared networks and NO ingress —
// the #243 case: a database an app pod reaches by pod DNS name, with no host
// port published at all.
func sharedNetTemplate(names ...string) store.Template {
	nets := make([]render.Network, 0, len(names))
	for _, n := range names {
		nets = append(nets, render.Network{Name: n})
	}
	return sharedNetTemplateWith(nets...)
}

// sharedNetTemplateWith is the same fixture with full control over each
// membership, for the alias cases (#269).
func sharedNetTemplateWith(networks ...render.Network) store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "db",
			Parameters: requiredParams("slug"),
			Networks:   networks,
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: db-{{.slug}}
  labels:
    podman-api/template: db
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: db
      image: mariadb:latest
`,
		Origin: "seed",
	}
}

func sharedNetSvc(t *testing.T, tmpl store.Template) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, st := newSvcWith(t, fc, hosts, tmpl)
	return svc, fc, st
}

func sharedNetApply(slug string) ApplyRequest {
	return ApplyRequest{
		Template:   "db",
		Slug:       slug,
		Parameters: map[string]any{"slug": slug},
	}
}

// The whole point of #243: a template with no ingress declaration still gets
// its pod attached to the networks it names. The fake rejects a play onto an
// un-ensured network, so this also pins the ensure-before-play ordering.
func TestApplyAttachesDeclaredNetworksWithoutIngress(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared", "metrics"))

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("mariadb"), ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared", "metrics"}, fc.PlayCalls[0].Networks)
	assert.Equal(t, []string{"frappe-shared", "metrics"}, fc.NetworkEnsureCalls["h1"])
}

// Ingress and declared networks compose: the ingress network leads, the
// declared ones follow.
func TestApplyCombinesIngressAndDeclaredNetworks(t *testing.T) {
	tmpl := sharedNetTemplate("frappe-shared")
	tmpl.Meta.Ingress = &render.Ingress{Container: "db", Port: 3306}
	svc, fc, _ := sharedNetSvc(t, tmpl)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("mariadb"), ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"podman-api-ingress", "frappe-shared"}, fc.PlayCalls[0].Networks)
}

// A template that names the ingress network explicitly must not have the pod
// attached to it twice — podman rejects a duplicate --network.
func TestApplyDeduplicatesNetworkNamedTwice(t *testing.T) {
	tmpl := sharedNetTemplate("podman-api-ingress", "frappe-shared")
	tmpl.Meta.Ingress = &render.Ingress{Container: "db", Port: 3306}
	svc, fc, _ := sharedNetSvc(t, tmpl)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("mariadb"), ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"podman-api-ingress", "frappe-shared"}, fc.PlayCalls[0].Networks)
}

// Declared networks are independent of the ingress FEATURE: an instance on a
// server started without -ingress still joins its shared networks. Without
// this, a pro deployment that never enables ingress silently gets no networking.
func TestApplyAttachesDeclaredNetworksWhenIngressDisabled(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	// no SetIngress call: ingressEnabled() is false

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("mariadb"), ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared"}, fc.PlayCalls[0].Networks)
}

// #269: declared aliases ride along with the join as podman's
// "name:alias=..." spelling, so a peer resolves the instance by a
// slug-independent name. The network is still ensured under its bare name.
func TestApplyAttachesNetworkAliases(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb", "db"}},
		render.Network{Name: "metrics"},
	))

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared:alias=mariadb,alias=db", "metrics"}, fc.PlayCalls[0].Networks)
	assert.Equal(t, []string{"frappe-shared", "metrics"}, fc.NetworkEnsureCalls["h1"])
}

// De-duplicating the ingress network must MERGE aliases, not drop them: the
// bare ingress entry leads the list, so a naive keep-first would silently
// discard the aliases the template declared for that same network.
func TestApplyMergesAliasesOntoDeduplicatedIngressNetwork(t *testing.T) {
	tmpl := sharedNetTemplateWith(render.Network{Name: "podman-api-ingress", Aliases: []string{"db"}})
	tmpl.Meta.Ingress = &render.Ingress{Container: "db", Port: 3306}
	svc, fc, _ := sharedNetSvc(t, tmpl)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"podman-api-ingress:alias=db"}, fc.PlayCalls[0].Networks)
}

// The reconcile path plays the same alias arguments as apply — an instance that
// comes back after a reboot must answer to the same names, or its peers break.
func TestReconcileSpecsOnHost_AttachesNetworkAliases(t *testing.T) {
	svc, fc, st := sharedNetSvc(t, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	))
	seedBootSpec(t, st, "h1", "db", "vedanta", nil)

	svc.ReconcileSpecsOnHost(context.Background(), "h1")

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared:alias=mariadb"}, fc.PlayCalls[0].Networks)
}

// Aliases come from the template, so a second instance of an aliased template on
// the same host+network claims the same DNS name. Podman would accept both and
// resolve one arbitrarily; apply refuses instead.
func TestApplyRejectsDuplicateAliasOnSameNetwork(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	))
	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	err := svc.Apply(context.Background(), "h1", sharedNetApply("second"), ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mariadb")
	assert.Contains(t, err.Error(), "db/vedanta")
	assert.Len(t, fc.PlayCalls, 1, "the conflicting pod must not be played")
}

// Re-applying the SAME instance is not a self-conflict — it holds its own names.
func TestApplyAllowsReapplyOfAliasedInstance(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	))
	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))
	assert.Len(t, fc.PlayCalls, 2)
}

// The same alias on DIFFERENT networks is not a conflict: podman resolves
// per-network, so two instances that never share a network never collide.
func TestApplyAllowsSameAliasOnDifferentNetworks(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	one := sharedNetTemplateWith(render.Network{Name: "net-a", Aliases: []string{"mariadb"}})
	two := sharedNetTemplateWith(render.Network{Name: "net-b", Aliases: []string{"mariadb"}})
	two.Meta.ID = "db2"
	two.Body = strings.ReplaceAll(one.Body, "db-{{.slug}}", "db2-{{.slug}}")
	svc, _ := newSvcWith(t, fc, hosts, one, two)

	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))
	err := svc.Apply(context.Background(), "h1",
		ApplyRequest{Template: "db2", Slug: "other", Parameters: map[string]any{"slug": "other"}},
		ApplyOptions{Replace: true})

	require.NoError(t, err)
	assert.Len(t, fc.PlayCalls, 2)
}

// An alias must not shadow another instance's pod DNS name on the same network:
// "db-vedanta" resolving to a different pod than db/vedanta is precisely the
// silent misrouting aliases are supposed to avoid.
func TestApplyRejectsAliasCollidingWithAnotherPodName(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	plain := sharedNetTemplate("frappe-shared")
	shadow := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"db-vedanta"}})
	shadow.Meta.ID = "db2"
	shadow.Body = strings.ReplaceAll(plain.Body, "db-{{.slug}}", "db2-{{.slug}}")
	svc, _ := newSvcWith(t, fc, hosts, plain, shadow)
	require.NoError(t, svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	err := svc.Apply(context.Background(), "h1",
		ApplyRequest{Template: "db2", Slug: "other", Parameters: map[string]any{"slug": "other"}},
		ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db-vedanta")
	assert.Len(t, fc.PlayCalls, 1)
}

// The reverse direction: a NEW instance whose pod name is already claimed as an
// existing instance's alias would itself be shadowed, so it is refused too.
func TestApplyRejectsPodNameAlreadyClaimedAsAlias(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	claimer := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"db-vedanta"}})
	claimer.Meta.ID = "db2"
	plain := sharedNetTemplate("frappe-shared")
	claimer.Body = strings.ReplaceAll(plain.Body, "db-{{.slug}}", "db2-{{.slug}}")
	svc, _ := newSvcWith(t, fc, hosts, plain, claimer)
	require.NoError(t, svc.Apply(context.Background(), "h1",
		ApplyRequest{Template: "db2", Slug: "other", Parameters: map[string]any{"slug": "other"}},
		ApplyOptions{Replace: true}))

	err := svc.Apply(context.Background(), "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db-vedanta")
	assert.Len(t, fc.PlayCalls, 1)
}

// A network that cannot be ensured must fail the apply rather than play a pod
// that silently cannot reach its peers.
func TestApplyFailsWhenDeclaredNetworkCannotBeEnsured(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	fc.NetworkEnsureErr = assert.AnError

	err := svc.Apply(context.Background(), "h1", sharedNetApply("mariadb"), ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "frappe-shared")
	assert.Empty(t, fc.PlayCalls, "must not play a pod whose network is missing")
}

// The boot-converge path must attach the same networks as the apply path. This
// is the site most likely to drift: an instance that reconciles after a reboot
// would otherwise come back with no shared networking.
func TestReconcileSpecsOnHost_AttachesDeclaredNetworks(t *testing.T) {
	svc, fc, st := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	seedBootSpec(t, st, "h1", "db", "mariadb", nil)

	svc.ReconcileSpecsOnHost(context.Background(), "h1")

	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, []string{"frappe-shared"}, fc.PlayCalls[0].Networks)
	assert.Equal(t, []string{"frappe-shared"}, fc.NetworkEnsureCalls["h1"])
}

// Rename applies the new slug while the old spec is still in the store (it is
// deleted only after the new pod verifies), so without ApplyOptions.
// SupersededSlug the instance would collide with itself and every aliased
// template would be unrenameable — the workflow aliases exist to protect
// (#269 review finding 1).
func TestRenameAliasedInstance(t *testing.T) {
	svc, _, _ := sharedNetSvc(t, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	))
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	err := svc.Rename(ctx, "h1", "db", "vedanta", RenameRequest{NewSlug: "vedanta2"}, func(string, string) {})
	require.NoError(t, err)
}

// The same seam fixes the pre-existing version of this bug on the ingress side:
// renaming an instance that carries a domain hit "domain already claimed" by its
// own old spec, on main, before aliases existed.
func TestRenameInstanceWithDomains(t *testing.T) {
	tmpl := sharedNetTemplate()
	tmpl.Meta.Ingress = &render.Ingress{Container: "db", Port: 3306}
	svc, _, _ := sharedNetSvc(t, tmpl)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()
	req := sharedNetApply("vedanta")
	req.Domains = []string{"a.example.com"}
	require.NoError(t, svc.Apply(ctx, "h1", req, ApplyOptions{Replace: true}))

	err := svc.Rename(ctx, "h1", "db", "vedanta", RenameRequest{NewSlug: "vedanta2"}, func(string, string) {})
	require.NoError(t, err)
}

// A superseded slug is only excused for the template being applied — it must not
// let an apply walk over an unrelated instance's claim.
func TestSupersededSlugDoesNotExcuseAnotherTemplate(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	one := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}})
	two := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}})
	two.Meta.ID = "db2"
	two.Body = strings.ReplaceAll(one.Body, "db-{{.slug}}", "db2-{{.slug}}")
	svc, _ := newSvcWith(t, fc, hosts, one, two)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	err := svc.Apply(ctx, "h1",
		ApplyRequest{Template: "db2", Slug: "other", Parameters: map[string]any{"slug": "other"}},
		ApplyOptions{Replace: true, SupersededSlug: "vedanta"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mariadb")
}

// An edit that would make two live instances claim one name is refused, not
// warned about: applyLocked is also the upgrade / rotate / parameter-update
// path, so accepting it would wedge both instances against all three with no
// in-product way out (#269 review finding 2).
func TestUpdateTemplateRejectsAliasThatWedgesLiveInstances(t *testing.T) {
	svc, _, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("a"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("b"), ApplyOptions{Replace: true}))

	err := svc.UpdateTemplate(ctx, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	))

	require.ErrorIs(t, err, ErrInvalidTemplate)
	assert.Contains(t, err.Error(), "mariadb")
	assert.Contains(t, err.Error(), "db/a")
	assert.Contains(t, err.Error(), "db/b")

	// And the instances stay servable: the rejected edit was not persisted, so a
	// re-apply (the upgrade/rotate path) still works.
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("a"), ApplyOptions{Replace: true}))
}

// The same edit against a SINGLE instance is fine — that is the ordinary way an
// alias gets adopted.
func TestUpdateTemplateAllowsAliasWithOneInstance(t *testing.T) {
	svc, _, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("a"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.UpdateTemplate(ctx, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	)))
}

// Boot converge cannot refuse a duplicate claim — keeping an instance down after
// a reboot is worse than a contended alias, and by then the state already exists
// — but it must not play one silently either. Both instances are named in the
// log (#269 review finding 2, second half).
func TestReconcileSpecsOnHost_WarnsOnDuplicateAliasClaim(t *testing.T) {
	svc, fc, st := sharedNetSvc(t, sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}},
	))
	seedBootSpec(t, st, "h1", "db", "a", nil)
	seedBootSpec(t, st, "h1", "db", "b", nil)

	out := captureLog(t, func() { svc.ReconcileSpecsOnHost(context.Background(), "h1") })

	assert.Len(t, fc.PlayCalls, 2, "both instances still come back")
	assert.Contains(t, out, "mariadb")
	assert.Contains(t, out, "db/a")
	assert.Contains(t, out, "db/b")
}

// A conflict between two OTHER instances must not fail an unrelated apply. The
// state is reachable and transient by design — rename holds the old and new spec
// at once for up to verifyTimeout — so a host-wide reading would refuse every
// deploy on the host for minutes, and would contradict converge, which tolerates
// the same state (#269 re-review finding 1).
func TestApplyIgnoresConflictBetweenOtherInstances(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	aliased := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}})
	other := sharedNetTemplateWith(render.Network{Name: "frappe-shared"})
	other.Meta.ID = "app"
	other.Body = strings.ReplaceAll(aliased.Body, "db-{{.slug}}", "app-{{.slug}}")
	svc, st := newSvcWith(t, fc, hosts, aliased, other)
	ctx := context.Background()

	// Two instances of the aliased template both claiming "mariadb" — the state
	// a rename passes through, seeded directly here.
	seedBootSpec(t, st, "h1", "db", "a", nil)
	seedBootSpec(t, st, "h1", "db", "b", nil)

	err := svc.Apply(ctx, "h1",
		ApplyRequest{Template: "app", Slug: "web", Parameters: map[string]any{"slug": "web"}},
		ApplyOptions{Replace: true})

	require.NoError(t, err, "an unrelated instance must not be blocked by someone else's conflict")
	require.Len(t, fc.PlayCalls, 1)
}

// ...and the same for a template edit: a pre-existing clash between two
// instances of a DIFFERENT template must not make this template un-editable.
func TestUpdateTemplateIgnoresConflictInAnotherTemplate(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	aliased := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}})
	other := sharedNetTemplateWith(render.Network{Name: "frappe-shared"})
	other.Meta.ID = "app"
	other.Body = strings.ReplaceAll(aliased.Body, "db-{{.slug}}", "app-{{.slug}}")
	svc, st := newSvcWith(t, fc, hosts, aliased, other)
	ctx := context.Background()
	seedBootSpec(t, st, "h1", "db", "a", nil)
	seedBootSpec(t, st, "h1", "db", "b", nil)
	seedBootSpec(t, st, "h1", "app", "web", nil)

	edited := other
	edited.Meta.Networks = []render.Network{{Name: "frappe-shared", Aliases: []string{"web-front"}}}

	require.NoError(t, svc.UpdateTemplate(ctx, edited))
}

// The subject scoping must not blunt the check itself: a conflict the subject is
// party to on a network it joins is still refused, and one on a network it does
// not join is not its business.
func TestApplyConflictScopedToJoinedNetworks(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	onShared := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"mariadb"}})
	elsewhere := sharedNetTemplateWith(render.Network{Name: "other-net", Aliases: []string{"mariadb"}})
	elsewhere.Meta.ID = "app"
	elsewhere.Body = strings.ReplaceAll(onShared.Body, "db-{{.slug}}", "app-{{.slug}}")
	svc, _ := newSvcWith(t, fc, hosts, onShared, elsewhere)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("vedanta"), ApplyOptions{Replace: true}))

	// Same alias, different network: allowed.
	require.NoError(t, svc.Apply(ctx, "h1",
		ApplyRequest{Template: "app", Slug: "web", Parameters: map[string]any{"slug": "web"}},
		ApplyOptions{Replace: true}))

	// Same alias, same network: still refused.
	err := svc.Apply(ctx, "h1", sharedNetApply("second"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mariadb")
}
