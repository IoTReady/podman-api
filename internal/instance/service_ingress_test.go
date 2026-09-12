package instance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// noIngressTemplate is a fixture that declares NO ingress.
func noIngressTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "db",
			Parameters: requiredParams("slug", "image"),
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: db-{{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
`,
		Origin: "seed",
	}
}

// recordingCtl is a test double Controller that records the hosts it reconciled.
type recordingCtl struct{ hosts []string }

func (r *recordingCtl) Reconcile(_ context.Context, host string) error {
	r.hosts = append(r.hosts, host)
	return nil
}

// webTemplate is a web-shaped fixture that declares ingress.
func webTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "web",
			Parameters: requiredParams("slug", "image"),
			Ingress:    &render.Ingress{Container: "web", Port: 8080},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: web-{{.slug}}
  labels:
    podman-api/template: web
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: web
      image: {{.image}}
`,
		Origin: "seed",
	}
}

func newWebSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	return svc, f
}

func webApply(slug string) ApplyRequest {
	return ApplyRequest{
		Template:   "web",
		Slug:       slug,
		Parameters: map[string]any{"slug": slug, "image": "docker.io/library/nginx:1"},
		Domains:    []string{"app.example.com"},
	}
}

// An apply carrying domains against a host with ingress_managed: false must
// be rejected up front, not silently accepted and persisted. Without this,
// GET .../instances/... shows the domain as configured while
// CaddyController.Reconcile permanently no-ops for that host (#301) --
// nothing anywhere says the domain is never actually routed. Found in review
// of #301's own PR.
func TestApplyRejectsDomainsOnUnmanagedHost(t *testing.T) {
	managed := false
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x", IngressManaged: &managed}}
	f := fake.New()
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")

	err := svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not ingress-managed")
	assert.Empty(t, f.PlayCalls, "no pod should be played for a rejected request")
}

// A nil IngressManaged (the default, "unset means managed") and an explicit
// true must both still allow a domain-carrying apply -- only an explicit
// false rejects.
func TestApplyAllowsDomainsWhenIngressManagedUnsetOrTrue(t *testing.T) {
	managedTrue := true
	for _, hosts := range [][]config.Host{
		{{ID: "h1", Addr: "unix", Socket: "/x"}},                               // unset
		{{ID: "h1", Addr: "unix", Socket: "/x", IngressManaged: &managedTrue}}, // explicit true
	} {
		svc, _ := newSvcWith(t, fake.New(), hosts, webTemplate())
		svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
		err := svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true})
		require.NoError(t, err)
	}
}

func TestApplyRejectsDomainsWhenIngressDisabled(t *testing.T) {
	svc, _ := newWebSvc(t) // default Disabled controller, ingress not enabled
	err := svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ingress is disabled")
}

func TestApplyAttachesNetworkAndReconcilesWhenEnabled(t *testing.T) {
	svc, f := newWebSvc(t)
	rec := &recordingCtl{}
	svc.SetIngress(rec, "podman-api-ingress")

	require.NoError(t, svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true}))

	require.Len(t, f.PlayCalls, 1)
	assert.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
	// The ingress network must be ensured BEFORE the app pod joins it, or the
	// first deploy on a host fails ("network not found"). The fake now rejects a
	// play onto an un-ensured network, so a missing ensure would fail Apply above.
	assert.Contains(t, f.NetworkEnsureCalls["h1"], "podman-api-ingress")
	assert.Equal(t, []string{"h1"}, rec.hosts)
}

// A domain on a template that declares no ingress: must be rejected BEFORE the
// pod is played or the spec persisted — otherwise the spec poisons every later
// reconcile on the host.
func TestApplyRejectsDomainsOnNonIngressTemplate(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc, _ := newSvcWith(t, f, hosts, noIngressTemplate())
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")

	req := ApplyRequest{
		Template:   "db",
		Slug:       "main",
		Parameters: map[string]any{"slug": "main", "image": "docker.io/library/postgres:16"},
		Domains:    []string{"db.example.com"},
	}
	err := svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no ingress")
	assert.Empty(t, f.PlayCalls, "no pod should be played for a rejected request")
}

// slowReadStore widens the validateIngress check window: every host-wide
// uniqueness read (ListSpecKeys) sleeps before returning. This makes the #82
// TOCTOU race deterministic — concurrent Applies for different instances all
// observe the pre-claim state unless the service serializes domain claims per
// host. It embeds *store.Memory so the template catalog and remaining store
// methods carry through unchanged.
type slowReadStore struct {
	*store.Memory
	delay time.Duration
}

func (s slowReadStore) ListSpecKeys(ctx context.Context, host string) ([]store.SpecKey, error) {
	time.Sleep(s.delay)
	return s.Memory.ListSpecKeys(ctx, host)
}

// Two different instances racing to claim the SAME host-wide-unique domain must
// not both succeed: the host-wide uniqueness check and the spec persist have to
// be atomic across instances. The per-instance lock alone does not serialize
// distinct instances, so without a per-host guard both observe an unclaimed
// domain and both persist it. (#82)
func TestApplyDomainUniquenessIsHostSerialized(t *testing.T) {
	svc, _ := newWebSvc(t)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	svc.SetStore(slowReadStore{Memory: seedStore(t, webTemplate()), delay: 50 * time.Millisecond})

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Distinct slugs -> distinct instance locks (all run concurrently),
			// but every request claims the same domain app.example.com.
			req := webApply(fmt.Sprintf("inst%d", i))
			errs[i] = svc.Apply(context.Background(), "h1", req, ApplyOptions{Replace: true})
		}(i)
	}
	wg.Wait()

	success := 0
	for _, e := range errs {
		if e == nil {
			success++
			continue
		}
		assert.Contains(t, e.Error(), "already claimed")
	}
	assert.Equal(t, 1, success, "exactly one instance may claim a host-wide-unique domain")
}

// A domain already claimed by another instance on the host must be rejected
// pre-mutation, not discovered only at reconcile time.
func TestApplyRejectsDuplicateDomainAcrossInstances(t *testing.T) {
	svc, f := newWebSvc(t)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	st := seedStore(t, webTemplate())
	svc.SetStore(st)
	require.NoError(t, st.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "web", Slug: "other", Domains: []string{"app.example.com"},
	}))

	// webApply("demo") also claims app.example.com -> conflicts with web/other.
	err := svc.Apply(context.Background(), "h1", webApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already claimed")
	assert.Empty(t, f.PlayCalls, "no pod should be played for a rejected request")
}

// webWithSecretsTemplate declares BOTH ingress and two per-instance secrets, so
// a domains-only update can be proven not to touch either.
func webWithSecretsTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "websec",
			Parameters: requiredParams("slug", "image"),
			Ingress:    &render.Ingress{Container: "web", Port: 8080},
			Secrets:    render.Secrets{PerInstance: []string{"password", "token"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: websec-{{.slug}}
  labels:
    podman-api/template: websec
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: web
      image: {{.image}}
`,
		Origin: "seed",
	}
}

// Mirrors TestApplyDomainUniquenessIsHostSerialized, but through
// UpdateInstanceDomains rather than Apply: two different EXISTING instances
// racing to claim the SAME host-wide-unique domain via PATCH .../domains must
// not both succeed, for exactly the reason Apply itself takes a conditional
// host lock. UpdateInstanceDomains's whole purpose is to CHANGE domains
// (unlike UpdateInstanceParameters, which always re-applies the instance's
// own unchanged domains and therefore correctly takes no host lock) -- found
// missing the lock entirely in review of #301's PR.
func TestUpdateInstanceDomainsUniquenessIsHostSerialized(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	mem := seedStore(t, webTemplate())
	svc, _ := newSvcWith(t, fake.New(), hosts, webTemplate())
	svc.SetStore(slowReadStore{Memory: mem, delay: 50 * time.Millisecond})
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()

	const n = 8
	for i := 0; i < n; i++ {
		require.NoError(t, mem.PutSpec(ctx, store.Spec{
			Host: "h1", Template: "web", Slug: fmt.Sprintf("inst%d", i),
			Parameters: map[string]any{"slug": fmt.Sprintf("inst%d", i), "image": "img:1"},
		}))
	}

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Distinct slugs -> distinct instance locks (all run concurrently),
			// but every request claims the same domain app.example.com.
			errs[i] = svc.UpdateInstanceDomains(ctx, "h1", "web", fmt.Sprintf("inst%d", i), []string{"app.example.com"})
		}(i)
	}
	wg.Wait()

	success := 0
	for _, e := range errs {
		if e == nil {
			success++
			continue
		}
		assert.Contains(t, e.Error(), "already claimed")
	}
	assert.Equal(t, 1, success, "exactly one instance may claim a host-wide-unique domain")
}

// UpdateInstanceDomains is issue #301's ask 3: a route to clear an instance's
// domains without restating its full declared secret set. It must replace the
// domains list wholesale while leaving parameters and secrets untouched.
func TestUpdateInstanceDomains_ClearsWithoutSecrets(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, webWithSecretsTemplate())
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "websec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "t"},
		Domains:    []string{"demo.example.com"},
	}, ApplyOptions{Replace: true}))

	// Clear domains with an empty (non-nil) slice, WITHOUT resupplying any
	// secret plaintext — the whole point of the route.
	require.NoError(t, svc.UpdateInstanceDomains(ctx, "h1", "websec", "demo", []string{}))

	got, err := mem.GetSpec(ctx, "h1", "websec", "demo")
	require.NoError(t, err)
	assert.Empty(t, got.Domains)
	assert.Equal(t, "p", got.Secrets["password"], "secrets must survive untouched")
	assert.Equal(t, "t", got.Secrets["token"], "secrets must survive untouched")
	assert.Equal(t, "img:1", got.Parameters["image"], "parameters must survive untouched")
}

// A nil domains argument must also clear (not merely no-op), since the HTTP
// handler passes an explicitly-empty slice for "domains": [] and this method
// must not special-case nil into "leave alone" — that would silently
// resurrect the merge semantics this route deliberately does not have.
func TestUpdateInstanceDomains_NilAlsoClears(t *testing.T) {
	svc, f := newWebSvc(t)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", webApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.UpdateInstanceDomains(ctx, "h1", "web", "demo", nil))

	require.Len(t, f.PlayCalls, 2, "domains change replaces the pod")
	_, err := svc.Get(ctx, "h1", "web", "demo")
	require.NoError(t, err)
}

// Replacing to a brand new (non-empty) domain list is also a replace, not a
// merge — the old domain must be gone, not accumulated alongside the new one.
func TestUpdateInstanceDomains_ReplacesRatherThanMerges(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, webTemplate())
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", webApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.UpdateInstanceDomains(ctx, "h1", "web", "demo", []string{"new.example.com"}))

	got, err := mem.GetSpec(ctx, "h1", "web", "demo")
	require.NoError(t, err)
	assert.Equal(t, []string{"new.example.com"}, got.Domains)
}

func TestUpdateInstanceDomains_NoSpecIsNotFound(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	err := svc.UpdateInstanceDomains(context.Background(), "h1", "postgres", "ghost", nil)
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestUpdateInstanceDomains_CorruptSpecPropagates(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	svc.SetStore(&getSpecErrStore{Memory: mem, err: store.ErrSpecCorrupt})
	err := svc.UpdateInstanceDomains(context.Background(), "h1", "postgres", "demo", nil)
	require.ErrorIs(t, err, store.ErrSpecCorrupt)
}

// An ingress reconcile failure during a domains-only update must be reported
// through the same ErrIngressReconcileFailed sentinel as apply/delete/rename —
// the pod is still fine, only the Caddy push failed (issue #301, ask 1).
func TestUpdateInstanceDomains_IngressFailureIsClassified(t *testing.T) {
	svc, _ := newWebSvc(t)
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", webApply("demo"), ApplyOptions{Replace: true}))

	// Only now does the host's ingress start failing — the initial deploy above
	// must succeed for this to be a meaningful "update fails" test.
	svc.SetIngress(hostErrIngress{failHost: "h1"}, "podman-api-ingress")

	err := svc.UpdateInstanceDomains(ctx, "h1", "web", "demo", nil)
	require.ErrorIs(t, err, ErrIngressReconcileFailed)
	assert.Contains(t, err.Error(), "caddy wedged", "underlying error must be surfaced, not swallowed")
}
