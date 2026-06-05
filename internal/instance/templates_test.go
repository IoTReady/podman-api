package instance

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func tmplSvc(t *testing.T, tmpls ...store.Template) (*Service, *store.Memory) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	return newSvcWith(t, fake.New(), hosts, tmpls...)
}

func TestGetTemplate(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t, webTemplate())

	got, err := svc.GetTemplate(ctx, "web")
	require.NoError(t, err)
	assert.Equal(t, "web", got.Meta.ID)

	_, err = svc.GetTemplate(ctx, "nope")
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestCreateTemplate(t *testing.T) {
	ctx := context.Background()
	svc, mem := tmplSvc(t)

	tpl := webTemplate()
	tpl.Origin = "" // CreateTemplate should default to "user"
	require.NoError(t, svc.CreateTemplate(ctx, tpl))

	got, err := mem.GetTemplate(ctx, "web")
	require.NoError(t, err)
	assert.Equal(t, "user", got.Origin)
}

func TestCreateTemplate_Duplicate(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t, webTemplate())

	err := svc.CreateTemplate(ctx, webTemplate())
	require.ErrorIs(t, err, ErrTemplateExists)
}

func TestCreateTemplate_InvalidBody(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := webTemplate()
	// Body references {{.nope}}, which is not a declared parameter, so the
	// dry-run render fails with missingkey=error.
	tpl.Body = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web-{{.nope}}\n"
	err := svc.CreateTemplate(ctx, tpl)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrTemplateExists)
}

func TestCreateTemplate_InvalidName(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := webTemplate()
	tpl.Meta.ID = "Bad ID!"
	err := svc.CreateTemplate(ctx, tpl)
	require.Error(t, err)
}

func TestUpdateTemplate_Absent(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	err := svc.UpdateTemplate(ctx, webTemplate())
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestUpdateTemplate_PreservesOrigin(t *testing.T) {
	ctx := context.Background()
	svc, mem := tmplSvc(t, webTemplate()) // seeded with Origin "seed"

	edit := webTemplate()
	edit.Origin = "user" // an edit must not be able to flip seed->user
	require.NoError(t, svc.UpdateTemplate(ctx, edit))

	got, err := mem.GetTemplate(ctx, "web")
	require.NoError(t, err)
	assert.Equal(t, "seed", got.Origin)
}

func TestCloneTemplate(t *testing.T) {
	ctx := context.Background()
	svc, mem := tmplSvc(t, webTemplate()) // Origin "seed"

	cl, err := svc.CloneTemplate(ctx, "web", "web2")
	require.NoError(t, err)
	assert.Equal(t, "web2", cl.Meta.ID)
	assert.Equal(t, "user", cl.Origin)

	got, err := mem.GetTemplate(ctx, "web2")
	require.NoError(t, err)
	assert.Equal(t, "user", got.Origin)
	assert.Equal(t, webTemplate().Body, got.Body)
}

func TestCloneTemplate_SrcAbsent(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	_, err := svc.CloneTemplate(ctx, "nope", "web2")
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestCloneTemplate_DupTarget(t *testing.T) {
	ctx := context.Background()
	w2 := webTemplate()
	w2.Meta.ID = "web2"
	svc, _ := tmplSvc(t, webTemplate(), w2)

	_, err := svc.CloneTemplate(ctx, "web", "web2")
	require.ErrorIs(t, err, ErrTemplateExists)
}

func TestValidateTemplate_MissingIngressContainer(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := webTemplate()
	// Ingress points at "web" but the body's only container is "app".
	tpl.Body = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web-{{.slug}}\nspec:\n  containers:\n    - name: app\n      image: {{.image}}\n"
	err := svc.CreateTemplate(ctx, tpl)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "web")
}

func TestDeleteTemplate_BlockedWhenInUse(t *testing.T) {
	ctx := context.Background()
	f := fake.New()
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(ctx, store.Template{Meta: render.Meta{ID: "web"}, Body: webTemplate().Body, Origin: "seed"}))
	require.NoError(t, mem.PutSpec(ctx, store.Spec{Host: "h1", Template: "web", Slug: "demo",
		Parameters: map[string]any{"slug": "demo"}}))
	svc := NewService(f, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	svc.SetStore(mem)

	err := svc.DeleteTemplate(ctx, "web", false)
	require.ErrorIs(t, err, ErrTemplateInUse)

	require.NoError(t, svc.DeleteTemplate(ctx, "web", true)) // force
	_, err = mem.GetTemplate(ctx, "web")
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestDeleteTemplate_NotInUse(t *testing.T) {
	ctx := context.Background()
	svc, mem := tmplSvc(t, webTemplate())

	require.NoError(t, svc.DeleteTemplate(ctx, "web", false))
	_, err := mem.GetTemplate(ctx, "web")
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestDeleteTemplate_UnknownIsNotFound(t *testing.T) {
	// store.DeleteTemplate is absent-OK, but the API documents 404 for an
	// unknown id, so DeleteTemplate must surface ErrUnknownTemplate (#61).
	ctx := context.Background()
	svc, _ := tmplSvc(t, webTemplate())
	err := svc.DeleteTemplate(ctx, "nope", false)
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

// TestCreateTemplate_IntParamNormalization verifies that a template whose
// parameter has Type=="" (as received from JSON before normalization) is
// treated as "string" and does not cause a false rejection.
func TestCreateTemplate_IntParamNormalization(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	// Build a template with an int param whose Type is already declared as "int".
	// The body uses a numeric comparison, which fails if type is left as "".
	tpl := store.Template{
		Meta: render.Meta{
			ID: "portapp",
			Parameters: []render.ParamDef{
				{Name: "port", Type: "int"},
			},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: portapp
spec:
  containers:
    - name: app
      image: nginx
      {{- if gt .port 1024}}
      # high port
      {{- end}}
`,
		Origin: "user",
	}

	require.NoError(t, svc.CreateTemplate(ctx, tpl),
		"template with int param should pass validation")
}

// TestCreateTemplate_BlankTypeNormalized verifies that a param with Type==""
// (blank, as from JSON omitempty) is normalized to "string" and not rejected.
func TestCreateTemplate_BlankTypeNormalized(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := store.Template{
		Meta: render.Meta{
			ID: "strapp",
			Parameters: []render.ParamDef{
				{Name: "label", Type: ""}, // blank: should normalize to "string"
			},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: strapp-{{.label}}
spec:
  containers:
    - name: app
      image: nginx
`,
		Origin: "user",
	}

	require.NoError(t, svc.CreateTemplate(ctx, tpl),
		"template with blank Type should normalize to string and pass")
}

// TestCreateTemplate_UnknownTypeRejected verifies that an unknown param type
// is rejected by NormalizeParams before reaching the store.
func TestCreateTemplate_UnknownTypeRejected(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := store.Template{
		Meta: render.Meta{
			ID: "badapp",
			Parameters: []render.ParamDef{
				{Name: "size", Type: "float"},
			},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: badapp
spec:
  containers:
    - name: app
      image: nginx
`,
		Origin: "user",
	}

	err := svc.CreateTemplate(ctx, tpl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "float")
}

// TestCreateTemplate_Concurrent launches N goroutines all creating the SAME
// template id; exactly one must succeed and the rest must get ErrTemplateExists.
// Run under -race to catch the check-then-act race the tmplMu mutex closes (#61).
func TestCreateTemplate_Concurrent(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	const n = 16
	var (
		wg       sync.WaitGroup
		okCount  int32
		dupCount int32
		start    = make(chan struct{})
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // line everyone up so the creates truly race
			err := svc.CreateTemplate(ctx, webTemplate())
			switch {
			case err == nil:
				atomic.AddInt32(&okCount, 1)
			case errors.Is(err, ErrTemplateExists):
				atomic.AddInt32(&dupCount, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), okCount, "exactly one create should succeed")
	assert.Equal(t, int32(n-1), dupCount, "all other creates should get ErrTemplateExists")
}

// TestDeleteTemplate_BlockedAfterApply proves the delete-vs-Apply ordering
// invariant (#61 review-2): once Apply has persisted a spec, the template's
// in-use scan sees it, so a subsequent DeleteTemplate(without force) is blocked
// with ErrTemplateInUse. (The full delete-mid-deploy interleaving can't be
// driven deterministically; this asserts the win condition we can test — Delete
// after Apply observes the spec.)
func TestDeleteTemplate_BlockedAfterApply(t *testing.T) {
	ctx := context.Background()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, noIngressTemplate())

	req := ApplyRequest{
		Template:   "db",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "docker.io/library/postgres:16"},
	}
	require.NoError(t, svc.Apply(ctx, "h1", req, ApplyOptions{Replace: true}))

	// Apply persisted a spec referencing "db" → the in-use scan must see it.
	_, err := mem.GetSpec(ctx, "h1", "db", "demo")
	require.NoError(t, err, "Apply should have persisted the spec")

	err = svc.DeleteTemplate(ctx, "db", false)
	require.ErrorIs(t, err, ErrTemplateInUse)
}

func TestIngressChanged(t *testing.T) {
	web := &render.Ingress{Container: "web", Port: 8080}
	cases := []struct {
		name     string
		old, new *render.Ingress
		want     bool
	}{
		{"no-ingress-to-no-ingress", nil, nil, false},
		{"added-ingress", nil, web, false},
		{"removed-ingress", web, nil, true},
		{"port-changed", web, &render.Ingress{Container: "web", Port: 9090}, true},
		{"container-changed", web, &render.Ingress{Container: "api", Port: 8080}, true},
		{"unchanged", web, &render.Ingress{Container: "web", Port: 8080}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ingressChanged(c.old, c.new); got != c.want {
				t.Errorf("ingressChanged = %v, want %v", got, c.want)
			}
		})
	}
}

// TestUpdateTemplate_IngressRemovalIsNonBlocking confirms that editing a live,
// referenced template to drop its ingress is warned about but NOT blocked
// (#61 review-2): the edit succeeds and the new (ingress-less) body is stored.
func TestUpdateTemplate_IngressRemovalIsNonBlocking(t *testing.T) {
	ctx := context.Background()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, webTemplate())
	svc.SetIngress(&recordingCtl{}, "podman-api-ingress")

	// Deploy a referencing instance with a domain.
	require.NoError(t, svc.Apply(ctx, "h1", webApply("demo"), ApplyOptions{Replace: true}))

	// Edit the template to remove ingress entirely. Must not block.
	edited := webTemplate()
	edited.Meta.Ingress = nil
	require.NoError(t, svc.UpdateTemplate(ctx, edited))

	got, err := mem.GetTemplate(ctx, "web")
	require.NoError(t, err)
	require.Nil(t, got.Meta.Ingress, "ingress removal must be persisted despite live reference")
}

// TestCreateTemplate_IngressPortZero / High / EmptyContainer verify that the
// ingress declaration is validated (port range, non-empty container) on
// API-created templates that bypass render.ParseMeta (#61).
func TestCreateTemplate_IngressPortZero(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := webTemplate()
	tpl.Meta.Ingress.Port = 0
	err := svc.CreateTemplate(ctx, tpl)
	require.ErrorIs(t, err, ErrInvalidTemplate)
}

func TestCreateTemplate_IngressPortTooHigh(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := webTemplate()
	tpl.Meta.Ingress.Port = 70000
	err := svc.CreateTemplate(ctx, tpl)
	require.ErrorIs(t, err, ErrInvalidTemplate)
}

func TestCreateTemplate_IngressEmptyContainer(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := webTemplate()
	tpl.Meta.Ingress.Container = ""
	err := svc.CreateTemplate(ctx, tpl)
	require.ErrorIs(t, err, ErrInvalidTemplate)
}

// TestCloneTemplate_TimestampsNonZero verifies that the returned clone has
// non-zero Created/Updated timestamps (re-fetched from store, not the local
// zero-initialized copy).
func TestCloneTemplate_TimestampsNonZero(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t, webTemplate())

	got, err := svc.CloneTemplate(ctx, "web", "web3")
	require.NoError(t, err)
	assert.Equal(t, "web3", got.Meta.ID)
	require.False(t, got.Created.IsZero(), "cloned template Created should be non-zero")
	require.False(t, got.Updated.IsZero(), "cloned template Updated should be non-zero")
}
