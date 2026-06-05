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
