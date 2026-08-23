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
)

// #272: kube play registers every container name in the played YAML as an alias
// on every network the pod joins, so two instances of a template whose
// container is literally named "db" both answer to "db" on a shared network —
// silently, and with podman resolving one of them arbitrarily. The apply-time
// uniqueness check must see those names, not just the declared aliases.
func TestApplyRejectsSecondInstanceSharingLiteralContainerName(t *testing.T) {
	svc, fc, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("first"), ApplyOptions{Replace: true}))

	err := svc.Apply(ctx, "h1", sharedNetApply("second"), ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"db"`)
	assert.Contains(t, err.Error(), "container name")
	assert.Contains(t, err.Error(), "db/first")
	assert.Len(t, fc.PlayCalls, 1, "the colliding pod must not be played")
}

// The stored fixture above carries no ContainerNames (it is written straight to
// the store, exactly like a row registered before the field existed), so the
// test above already covers the back-compat path. This one pins the other half:
// a row that DOES carry the field is read from it, and the field is what the
// registration path writes.
func TestCreateTemplateStoresDerivedContainerNames(t *testing.T) {
	svc, _, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	ctx := context.Background()
	fresh := sharedNetTemplate("frappe-shared")
	fresh.Meta.ID = "cache"
	fresh.Body = strings.ReplaceAll(fresh.Body, "db-{{.slug}}", "cache-{{.slug}}")
	fresh.Body = strings.ReplaceAll(fresh.Body, "name: db\n", "name: redis\n")
	fresh.Origin = ""

	require.NoError(t, svc.CreateTemplate(ctx, fresh))

	got, err := svc.GetTemplate(ctx, "cache")
	require.NoError(t, err)
	assert.Equal(t, []string{"redis"}, got.Meta.ContainerNames)
}

// The escape hatch, and the policy for templated names: a container name that
// varies per instance is not a shared claim, so two instances coexist. Such a
// name is never banked as a literal claim (it is not the same name twice), and
// registration only warns.
func TestApplyAllowsTemplatedContainerNameOnSharedNetwork(t *testing.T) {
	tmpl := sharedNetTemplate("frappe-shared")
	tmpl.Body = strings.ReplaceAll(tmpl.Body, "name: db\n", "name: db-{{.slug}}\n")
	svc, fc, _ := sharedNetSvc(t, tmpl)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("first"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("second"), ApplyOptions{Replace: true}))
	assert.Len(t, fc.PlayCalls, 2)
}

// A template that declares networks AND a templated container name gets a
// warning at registration: the name is unenforceable, so the operator is told
// rather than left to assume the check covers it.
func TestCreateTemplateWarnsOnTemplatedContainerName(t *testing.T) {
	svc, _, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	fresh := sharedNetTemplate("frappe-shared")
	fresh.Meta.ID = "cache"
	fresh.Body = strings.ReplaceAll(fresh.Body, "db-{{.slug}}", "cache-{{.slug}}")
	fresh.Body = strings.ReplaceAll(fresh.Body, "name: db\n", "name: redis-{{.slug}}\n")
	fresh.Origin = ""

	out := captureLog(t, func() {
		require.NoError(t, svc.CreateTemplate(context.Background(), fresh))
	})
	assert.Contains(t, out, "redis-")
	assert.Contains(t, out, "frappe-shared")
}

// A container name of one template shadows a DECLARED ALIAS of another just as
// surely as two aliases would — both are the same DNS name on one network.
func TestApplyRejectsAliasCollidingWithAnotherTemplatesContainerName(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	plain := sharedNetTemplate("frappe-shared") // container "db"
	aliased := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"db"}})
	aliased.Meta.ID = "app"
	aliased.Body = strings.ReplaceAll(plain.Body, "db-{{.slug}}", "app-{{.slug}}")
	aliased.Body = strings.ReplaceAll(aliased.Body, "name: db\n", "name: web\n")
	svc, _ := newSvcWith(t, fc, hosts, plain, aliased)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("first"), ApplyOptions{Replace: true}))

	err := svc.Apply(ctx, "h1",
		ApplyRequest{Template: "app", Slug: "web", Parameters: map[string]any{"slug": "web"}},
		ApplyOptions{Replace: true})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"db"`)
	assert.Len(t, fc.PlayCalls, 1)
}

// A template must not declare an alias it already carries as a container name:
// the pod answers to it either way, so the declaration reads as a name the
// operator could rename away when it is not.
func TestCreateTemplateRejectsAliasEqualToOwnContainerName(t *testing.T) {
	svc, _, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	bad := sharedNetTemplateWith(render.Network{Name: "frappe-shared", Aliases: []string{"db"}})
	bad.Meta.ID = "cache"
	bad.Body = strings.ReplaceAll(bad.Body, "db-{{.slug}}", "cache-{{.slug}}")
	bad.Origin = ""

	err := svc.CreateTemplate(context.Background(), bad)

	require.ErrorIs(t, err, ErrInvalidTemplate)
	assert.Contains(t, err.Error(), "container name")
}

// The same rule on the edit path, which is the one that can wedge live
// instances.
func TestUpdateTemplateRejectsAliasEqualToOwnContainerName(t *testing.T) {
	svc, _, _ := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))

	err := svc.UpdateTemplate(context.Background(), sharedNetTemplateWith(
		render.Network{Name: "frappe-shared", Aliases: []string{"db"}},
	))

	require.ErrorIs(t, err, ErrInvalidTemplate)
	assert.Contains(t, err.Error(), "container name")
}

// The implicit ingress network is exempt. Every ingress template joins it, it is
// not an opt-in namespace claim, and the ingress controller addresses pods by
// pod DNS name — enforcing container names there would refuse the second
// instance of every web template.
func TestApplyAllowsSharedContainerNameOnIngressNetwork(t *testing.T) {
	tmpl := sharedNetTemplate() // no declared networks
	tmpl.Meta.Ingress = &render.Ingress{Container: "db", Port: 3306}
	svc, fc, _ := sharedNetSvc(t, tmpl)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("first"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("second"), ApplyOptions{Replace: true}))
	assert.Len(t, fc.PlayCalls, 2)
}

// Boot converge cannot refuse — an instance that stays down after a reboot is
// worse than a contended name — but it must name both instances, container-name
// collisions included.
func TestReconcileSpecsOnHost_WarnsOnContainerNameCollision(t *testing.T) {
	svc, fc, st := sharedNetSvc(t, sharedNetTemplate("frappe-shared"))
	seedBootSpec(t, st, "h1", "db", "a", nil)
	seedBootSpec(t, st, "h1", "db", "b", nil)

	out := captureLog(t, func() { svc.ReconcileSpecsOnHost(context.Background(), "h1") })

	assert.Len(t, fc.PlayCalls, 2, "both instances still come back")
	assert.Contains(t, out, "container name")
	assert.Contains(t, out, "db/a")
	assert.Contains(t, out, "db/b")
}

// A template edit that ADDS a shared network to a template with two live
// instances now collides on the container name, not only on an alias — and,
// like the alias case, refusing the edit is what keeps both instances
// upgradeable.
func TestUpdateTemplateRejectsNetworkThatWedgesOnContainerName(t *testing.T) {
	svc, _, st := sharedNetSvc(t, sharedNetTemplate())
	ctx := context.Background()
	seedBootSpec(t, st, "h1", "db", "a", nil)
	seedBootSpec(t, st, "h1", "db", "b", nil)

	err := svc.UpdateTemplate(ctx, sharedNetTemplate("frappe-shared"))

	require.ErrorIs(t, err, ErrInvalidTemplate)
	assert.Contains(t, err.Error(), "container name")
	assert.Contains(t, err.Error(), "db/a")
}

// A BODY edit wedges live instances just as an alias edit does: renaming a
// container from a per-instance name to a literal one hands both instances the
// same podman-registered alias, and `networks:` never changed — so the edit
// gate cannot be networks-only (#272).
func TestUpdateTemplateRejectsBodyEditThatWedgesOnContainerName(t *testing.T) {
	tmpl := sharedNetTemplate("frappe-shared")
	tmpl.Body = strings.ReplaceAll(tmpl.Body, "name: db\n", "name: db-{{.slug}}\n")
	svc, _, _ := sharedNetSvc(t, tmpl)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("a"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("b"), ApplyOptions{Replace: true}))

	err := svc.UpdateTemplate(ctx, sharedNetTemplate("frappe-shared")) // literal "db" again

	require.ErrorIs(t, err, ErrInvalidTemplate)
	assert.Contains(t, err.Error(), "container name")
	assert.Contains(t, err.Error(), "db/a")

	// ...and the rejected edit left both instances upgradeable.
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("a"), ApplyOptions{Replace: true}))
}

// ...and the exemption survives a template that names the ingress network in
// its own `networks:` block, which is the supported way to declare an alias on
// it (joinNetworks merges rather than drops). Keying the exemption on how the
// membership was declared instead of on the network's identity would refuse the
// second instance of exactly this legal web template, on its container name —
// the failure the exemption exists to prevent (#285 review item 1).
func TestApplyAllowsSharedContainerNameOnExplicitlyDeclaredIngressNetwork(t *testing.T) {
	tmpl := sharedNetTemplateWith(render.Network{Name: "podman-api-ingress", Aliases: []string{"front"}})
	tmpl.Meta.Ingress = &render.Ingress{Container: "db", Port: 3306}
	svc, fc, _ := sharedNetSvc(t, tmpl)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("first"), ApplyOptions{Replace: true}))
	err := svc.Apply(ctx, "h1", sharedNetApply("second"), ApplyOptions{Replace: true})

	// The declared ALIAS still collides — that rule is unchanged — so assert on
	// the container name specifically: it must not be what refuses this apply.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "front")
	assert.NotContains(t, err.Error(), "container name")
	assert.Len(t, fc.PlayCalls, 1)
}

// The same template without the alias: two instances of an ingress web template
// that names the ingress network explicitly must both deploy.
func TestApplyAllowsTwoInstancesOnExplicitlyDeclaredIngressNetwork(t *testing.T) {
	tmpl := sharedNetTemplate("podman-api-ingress")
	tmpl.Meta.Ingress = &render.Ingress{Container: "db", Port: 3306}
	svc, fc, _ := sharedNetSvc(t, tmpl)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("first"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", sharedNetApply("second"), ApplyOptions{Replace: true}))
	assert.Len(t, fc.PlayCalls, 2)
}
