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
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// secretAndPortTemplate declares both a per-host secret and a fixed hostPort, so
// a single destination can surface two distinct preflight issues at once.
func secretAndPortTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "needs-both",
			Parameters: requiredParams("slug", "image"),
			Secrets:    render.Secrets{PerHostReferenced: []string{"shared-token"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: needs-both-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
      ports:
        - hostPort: 9090
          containerPort: 80
`,
		Origin: "seed",
	}
}

func TestPreflightIssues_CollectsAll(t *testing.T) {
	ctx := context.Background()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	f := fake.New()
	svc, _ := newSvcWith(t, f, hosts, secretAndPortTemplate())
	// Occupy port 9090 on the destination so the port check fails; leave the
	// per-host secret "shared-token" absent so the secret check also fails.
	f.AddPod("h2", podman.Pod{Name: "occupier", Status: "Running",
		Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 9090}}}}})

	tmpl := secretAndPortTemplate()
	eff := map[string]any{"slug": "x", "image": "img"}
	errs, _ := svc.preflightIssues(ctx, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "needs-both", Slug: "x"}, tmpl, eff, false)

	require.Len(t, errs, 2, "expected both the missing-secret and port-conflict issues")
	var sawSecret, sawPort bool
	for _, e := range errs {
		if errors.Is(e, ErrHostSecretMissing) {
			sawSecret = true
		}
		if errors.Is(e, ErrPortConflict) {
			sawPort = true
		}
	}
	assert.True(t, sawSecret, "missing per-host secret not reported")
	assert.True(t, sawPort, "port conflict not reported")
}

// newPlanSvc builds a service with the postgres + web (port) + host-secret
// templates and a destination set, returning the service, fake, and store.
func newPlanSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/a"},
		{ID: "h2", Addr: "unix", Socket: "/b"},
		{ID: "h3", Addr: "unix", Socket: "/c"},
		{ID: "draining", Addr: "unix", Socket: "/d", Drain: true},
	}
	f := fake.New()
	svc, mem := newSvcWith(t, f, hosts, pgTemplate(), portTemplate(), templateWithHostSecret())
	return svc, f, mem
}

// seedPGSpec stores a valid postgres spec on h1: full params plus the required
// per-instance "password" secret. The preview runs render.Validate (mirroring
// the executor's Apply), so a postgres spec must carry its secret to be clean —
// exactly as a real stored spec does.
func seedPGSpec(t *testing.T, mem *store.Memory, slug string) {
	t.Helper()
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "postgres", Slug: slug,
		Parameters: map[string]any{"slug": slug, "image": "x", "port": 5432, "db": "d", "user": "u"},
		Secrets:    map[string]string{"password": "p"},
	}))
}

func TestPlanEvacuation_AllClean(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	seedPGSpec(t, mem, "db1")
	seedPGSpec(t, mem, "db2")

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2", "db2": "h3"}})
	require.NoError(t, err)
	assert.Equal(t, "h1", plan.FromHost)
	require.Len(t, plan.Moves, 2)
	assert.Equal(t, "db1", plan.Moves[0].Slug)
	assert.Equal(t, "db2", plan.Moves[1].Slug)
	for _, m := range plan.Moves {
		assert.True(t, m.OK, "move %s should be clean", m.Slug)
		assert.Empty(t, m.Issues)
	}
}

func TestPlanEvacuation_BlockingConditions(t *testing.T) {
	ctx := context.Background()

	t.Run("destination draining", func(t *testing.T) {
		svc, _, mem := newPlanSvc(t)
		seedPGSpec(t, mem, "db1")
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "draining"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves, 1)
		assert.False(t, plan.Moves[0].OK)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "destination_draining", plan.Moves[0].Issues[0].Code)
	})

	t.Run("instance already exists on destination", func(t *testing.T) {
		svc, f, mem := newPlanSvc(t)
		seedPGSpec(t, mem, "db1")
		f.AddPod("h2", podman.Pod{Name: "postgres-db1", Status: "Running"})
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "instance_exists", plan.Moves[0].Issues[0].Code)
	})

	t.Run("missing per-host secret", func(t *testing.T) {
		svc, _, mem := newPlanSvc(t)
		seedSpec(t, mem, "h1", "needs-host-secret", "s1", map[string]any{"slug": "s1", "image": "x"})
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"s1": "h2"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "host_secret_missing", plan.Moves[0].Issues[0].Code)
	})

	t.Run("host port conflict", func(t *testing.T) {
		svc, f, mem := newPlanSvc(t)
		seedSpec(t, mem, "h1", "web", "w1", map[string]any{"slug": "w1", "image": "x"})
		f.AddPod("h2", podman.Pod{Name: "other", Status: "Running",
			Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 8080}}}}})
		plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"w1": "h2"}})
		require.NoError(t, err)
		require.Len(t, plan.Moves[0].Issues, 1)
		assert.Equal(t, "port_conflict", plan.Moves[0].Issues[0].Code)
	})
}

func TestPlanEvacuation_MixedPlan_AllReportedSorted(t *testing.T) {
	svc, f, mem := newPlanSvc(t)
	ctx := context.Background()
	seedPGSpec(t, mem, "alpha")
	seedSpec(t, mem, "h1", "web", "beta", map[string]any{"slug": "beta", "image": "x"})
	f.AddPod("h3", podman.Pod{Name: "other", Status: "Running",
		Containers: []podman.Container{{Name: "c", Ports: []podman.PortMapping{{HostPort: 8080}}}}})

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"alpha": "h2", "beta": "h3"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves, 2)
	assert.Equal(t, "alpha", plan.Moves[0].Slug)
	assert.True(t, plan.Moves[0].OK)
	assert.Equal(t, "beta", plan.Moves[1].Slug)
	assert.False(t, plan.Moves[1].OK, "a problematic move must not stop the others from being reported")
	require.Len(t, plan.Moves[1].Issues, 1)
	assert.Equal(t, "port_conflict", plan.Moves[1].Issues[0].Code)
}

func TestPlanEvacuation_InconclusiveCheck(t *testing.T) {
	svc, f, mem := newPlanSvc(t)
	ctx := context.Background()
	seedPGSpec(t, mem, "db1")
	f.PodInspectErr = errors.New("dial tcp: connection refused")

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves[0].Issues, 1)
	assert.Equal(t, "check_error", plan.Moves[0].Issues[0].Code)
	assert.Contains(t, plan.Moves[0].Issues[0].Message, "connection refused",
		"check_error must surface the underlying error to the operator")
	assert.False(t, plan.Moves[0].OK)
}

func TestPlanEvacuation_InvalidParameters(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	// Contract drift: the stored postgres spec predates a required param/secret
	// (here it omits the "password" secret). Body rendering tolerates this, so the
	// live port check passes — but the executor's Apply-time render.Validate would
	// reject it. The preview must predict that with an invalid_parameters issue.
	require.NoError(t, mem.PutSpec(ctx, store.Spec{
		Host: "h1", Template: "postgres", Slug: "db1",
		Parameters: map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"},
		// Secrets intentionally omitted.
	}))

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves, 1)
	assert.False(t, plan.Moves[0].OK)
	require.Len(t, plan.Moves[0].Issues, 1)
	assert.Equal(t, "invalid_parameters", plan.Moves[0].Issues[0].Code)
	assert.Contains(t, plan.Moves[0].Issues[0].Message, "password")
}

func TestPlanEvacuation_StaticValidationErrors(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"})
	seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{"slug": "db2", "image": "x", "port": 5432, "db": "d", "user": "u"})

	_, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "ghost", Map: map[string]string{}})
	assert.ErrorIs(t, err, ErrUnknownHost)
	_, err = svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
	assert.ErrorIs(t, err, ErrInvalidEvacuation)
	_, err = svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h1", "db2": "h2"}})
	assert.ErrorIs(t, err, ErrSameHost)
}

func TestPlanEvacuation_ProvisionsPersistedHostSecret(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	// Instance referencing a per-host secret; the secret is persisted for source h1
	// but absent on dest h2 -> non-blocking, reported in provisions.
	seedSpec(t, mem, "h1", "needs-host-secret", "s1", map[string]any{"slug": "s1", "image": "x"})
	require.NoError(t, mem.PutHostSecret(ctx, "h1", "shared-pull-token", []byte("v")))

	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"s1": "h2"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves, 1)
	assert.True(t, plan.Moves[0].OK, "provisionable secret keeps the move ok")
	assert.Empty(t, plan.Moves[0].Issues)
	assert.Equal(t, []string{"shared-pull-token"}, plan.Moves[0].Provisions)
}

func TestPlanEvacuation_ProvisionsEmptyByDefault(t *testing.T) {
	svc, _, mem := newPlanSvc(t)
	ctx := context.Background()
	seedPGSpec(t, mem, "db1")
	plan, err := svc.PlanEvacuation(ctx, EvacuateRequest{FromHost: "h1", Map: map[string]string{"db1": "h2"}})
	require.NoError(t, err)
	require.Len(t, plan.Moves, 1)
	assert.NotNil(t, plan.Moves[0].Provisions, "provisions serializes as [], never null")
	assert.Empty(t, plan.Moves[0].Provisions)
}
