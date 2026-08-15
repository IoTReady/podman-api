package instance

import (
	"context"
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
func sharedNetTemplate(networks ...string) store.Template {
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
