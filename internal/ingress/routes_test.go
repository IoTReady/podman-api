package ingress

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// webTemplate is an ingress-declaring template used by the route tests.
func webTemplate(id string, port int) store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:      id,
			Ingress: &render.Ingress{Container: id, Port: port},
		},
		Body:   "apiVersion: v1\nkind: Pod\n",
		Origin: "user",
	}
}

// nonIngressTemplate is a template with no ingress: declaration.
func nonIngressTemplate(id string) store.Template {
	return store.Template{
		Meta:   render.Meta{ID: id},
		Body:   "apiVersion: v1\nkind: Pod\n",
		Origin: "user",
	}
}

// newMemStore returns a store.Memory seeded with the given specs and templates.
func newMemStore(t *testing.T, specs []store.Spec, tmpls ...store.Template) *store.Memory {
	t.Helper()
	m := store.NewMemory()
	ctx := context.Background()
	for _, tm := range tmpls {
		require.NoError(t, m.PutTemplate(ctx, tm))
	}
	for _, s := range specs {
		require.NoError(t, m.PutSpec(ctx, s))
	}
	return m
}

func newCtl(t *testing.T, specs []store.Spec, tmpls ...store.Template) *CaddyController {
	t.Helper()
	return NewCaddyController(nil, newMemStore(t, specs, tmpls...), Config{})
}

func TestDeriveRoutesHappyPath(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}, Updated: time.Now()},
		{Host: "h1", Template: "postgres", Slug: "db", Domains: nil}, // no domains -> skipped
	}
	c := newCtl(t, specs, webTemplate("web", 8080))
	routes, err := c.deriveRoutes(context.Background(), "h1")
	require.NoError(t, err)
	require.Equal(t, []Route{{Domain: "blog.example.com", Backend: "web-blog:8080"}}, routes)
}

// TestDeriveRoutesPicksUpTemplateAddedAfterConstruction is the regression test
// for the boot-snapshot bug (#61 review): a template created in the store AFTER
// the controller is built must be resolved at reconcile time, not dropped.
func TestDeriveRoutesPicksUpTemplateAddedAfterConstruction(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	require.NoError(t, st.PutSpec(ctx, store.Spec{
		Host: "h1", Template: "late", Slug: "site", Domains: []string{"site.example.com"},
	}))
	c := NewCaddyController(nil, st, Config{})

	// Template "late" does not exist yet: its spec references a missing template,
	// so the route is skipped (not an error).
	routes, err := c.deriveRoutes(ctx, "h1")
	require.NoError(t, err)
	require.Empty(t, routes)

	// Create the ingress template AFTER the controller was constructed.
	require.NoError(t, st.PutTemplate(ctx, webTemplate("late", 9000)))

	routes, err = c.deriveRoutes(ctx, "h1")
	require.NoError(t, err)
	require.Equal(t, []Route{{Domain: "site.example.com", Backend: "late-site:9000"}}, routes)
}

// A spec whose template declares no ingress: contributes no routes (skipped),
// and a spec whose template was deleted is likewise skipped — neither fails the
// host reconcile.
func TestDeriveRoutesSkipsNonIngressAndMissingTemplates(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "postgres", Slug: "db", Domains: []string{"db.example.com"}},
		{Host: "h1", Template: "gone", Slug: "x", Domains: []string{"gone.example.com"}},
		{Host: "h1", Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}},
	}
	// "postgres" exists but declares no ingress; "gone" is absent; "web" routes.
	c := newCtl(t, specs, nonIngressTemplate("postgres"), webTemplate("web", 8080))
	routes, err := c.deriveRoutes(context.Background(), "h1")
	require.NoError(t, err)
	require.Equal(t, []Route{{Domain: "blog.example.com", Backend: "web-blog:8080"}}, routes)
}

func TestDeriveRoutesRejectsDuplicateDomain(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "web", Slug: "a", Domains: []string{"x.example.com"}},
		{Host: "h1", Template: "web", Slug: "b", Domains: []string{"x.example.com"}},
	}
	c := newCtl(t, specs, webTemplate("web", 8080))
	_, err := c.deriveRoutes(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "claimed by")
}
